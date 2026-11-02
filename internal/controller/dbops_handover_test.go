package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A restart waiting on a shard master that never hands over fails, with a
// message that says where to look, and withdraws its step-down request.
func TestDbOpsHandoverTimesOut(t *testing.T) {
	c := shardedCluster("s", 1, 1)
	zero := int32(0)
	c.Status.Shards = []kividbv1alpha1.ShardStatus{{Index: 0, MasterPod: "s-0-0"}}
	c.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "s-0-0", Shard: &zero, Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
		{Name: "s-0-1", Shard: &zero, Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true},
	}
	master := shardedPod(c, 0, 0, "10.0.0.1")
	replica := shardedPod(c, 0, 1, "10.0.0.2")
	op := &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: c.Namespace},
		Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: c.Name}, Op: kividbv1alpha1.DbOpsRestart},
		Status: kividbv1alpha1.KividbDbOpsStatus{
			Phase:   kividbv1alpha1.DbOpsRunning,
			Restart: &kividbv1alpha1.DbOpsRestartStatus{PendingPods: []string{"s-0-0"}, CompletedPods: []string{"s-0-1"}},
		},
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &KividbDbOpsReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, master, replica, op).
			WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbDbOps{}).Build(),
		Scheme: scheme,
	}
	ctx := context.Background()
	// The status subresource drops what WithObjects was given; set it.
	if err := r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := r.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}
	get := func() *kividbv1alpha1.KividbDbOps {
		var got kividbv1alpha1.KividbDbOps
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return &got
	}
	podAnnotation := func() string {
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKeyFromObject(master), &p); err != nil {
			t.Fatal(err)
		}
		return p.Annotations[StepDownAnnotation]
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := get()
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || got.Status.Restart.HandoverStartTime == nil || podAnnotation() != "true" {
		t.Fatalf("first pass: phase %s, handover start %v, annotation %q", got.Status.Phase, got.Status.Restart.HandoverStartTime, podAnnotation())
	}

	// Still the master well past the timeout.
	past := metav1.NewTime(time.Now().Add(-dbOpsHandoverTimeout - time.Second))
	got.Status.Restart.HandoverStartTime = &past
	if err := r.Status().Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got = get()
	if got.Status.Phase != kividbv1alpha1.DbOpsFailed {
		t.Fatalf("phase %s after the timeout, want Failed", got.Status.Phase)
	}
	for _, want := range []string{"s-0-0 did not hand over the master role within 5m0s", "ShardSwitchover", "Manual failover abandoned"} {
		if !strings.Contains(got.Status.Error, want) {
			t.Errorf("error %q lacks %q", got.Status.Error, want)
		}
	}
	if podAnnotation() != "" {
		t.Error("step-down request left on the master")
	}
}

// newDbOpsReconciler is a KividbDbOpsReconciler over a fake client holding
// objs. The fake drops the status of objects it is built with, so the
// status of each cluster and op in objs is written again.
func newDbOpsReconciler(t *testing.T, objs ...client.Object) *KividbDbOpsReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &KividbDbOpsReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbDbOps{}).Build(),
		Scheme: scheme,
	}
	for _, o := range objs {
		switch o.(type) {
		case *kividbv1alpha1.KividbCluster, *kividbv1alpha1.KividbDbOps:
			if err := r.Status().Update(context.Background(), o); err != nil {
				t.Fatal(err)
			}
		}
	}
	return r
}

// replicatedForRestart is a replicated cluster c1 of master c1-0 and the
// given number of replicas, every pod Ready and in sync, and a restart
// with pending left to do (the rest done).
func replicatedForRestart(replicas int32, pending ...string) (*kividbv1alpha1.KividbCluster, []*corev1.Pod, *kividbv1alpha1.KividbDbOps) {
	c := testCluster("c1")
	c.Spec.Replicas = replicas
	c.Status.MasterPod = "c1-0"
	var pods []*corev1.Pod
	var done []string
	for i := int32(0); i <= replicas; i++ {
		name, role := fmt.Sprintf("c1-%d", i), kividbv1alpha1.RoleReplica
		if i == 0 {
			role = kividbv1alpha1.RoleMaster
		}
		c.Status.Pods = append(c.Status.Pods, kividbv1alpha1.KividbPodStatus{Name: name, Role: role, Ready: true, Synced: true})
		labels := selectorLabels(c)
		labels[kividbv1alpha1.RoleLabel] = string(role)
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels},
			Status: corev1.PodStatus{
				PodIP:      fmt.Sprintf("10.0.0.%d", i+1),
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		})
		if !slices.Contains(pending, name) {
			done = append(done, name)
		}
	}
	op := &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: c.Namespace},
		Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: c.Name}, Op: kividbv1alpha1.DbOpsRestart},
		Status: kividbv1alpha1.KividbDbOpsStatus{
			Phase:   kividbv1alpha1.DbOpsRunning,
			Restart: &kividbv1alpha1.DbOpsRestartStatus{PendingPods: pending, CompletedPods: done},
		},
	}
	return c, pods, op
}

func reconcileOp(t *testing.T, r *KividbDbOpsReconciler, op *kividbv1alpha1.KividbDbOps) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}); err != nil {
		t.Fatal(err)
	}
}

func podExists(t *testing.T, r *KividbDbOpsReconciler, name string) *corev1.Pod {
	t.Helper()
	var p corev1.Pod
	err := r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &p)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return &p
}

// A master whose replica is not in sync at that moment asks it to take
// over and waits (up to dbOpsHandoverTimeout), rather than being deleted
// in place. Only a master with no replica at all is restarted in place.
func TestDbOpsMasterWithReplicaNotInSync(t *testing.T) {
	c, pods, op := replicatedForRestart(1, "c1-0")
	c.Status.Pods[1].Synced = false
	r := newDbOpsReconciler(t, c, pods[0], pods[1], op)
	reconcileOp(t, r, op)
	p := podExists(t, r, "c1-0")
	if p == nil {
		t.Fatal("master c1-0 deleted in place while it had a replica")
	}
	if p.Annotations[StepDownAnnotation] != "true" {
		t.Fatal("no step-down asked of c1-0")
	}

	c, pods, op = replicatedForRestart(0, "c1-0")
	r = newDbOpsReconciler(t, c, pods[0], op)
	reconcileOp(t, r, op)
	if podExists(t, r, "c1-0") != nil {
		t.Fatal("master c1-0 of replicas: 0 not restarted")
	}
}

// A restart takes a pod down only while every other pod is there, Ready
// and in sync, and none is going away -- the gate of reconcileRollout.
func TestDbOpsRestartWaitsForTheRestOfTheCluster(t *testing.T) {
	tests := []struct {
		name    string
		change  func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod
		deleted bool
	}{
		{"every other pod healthy", func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod { return pods }, true},
		{"another pod not Ready", func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod {
			pods[1].Status.Conditions[0].Status = corev1.ConditionFalse
			return pods
		}, false},
		{"another replica not in sync", func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod {
			c.Status.Pods[1].Synced = false
			return pods
		}, false},
		{"another pod being deleted", func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod {
			now := metav1.Now()
			pods[1].DeletionTimestamp, pods[1].Finalizers = &now, []string{"test/hold"}
			return pods
		}, false},
		{"another pod missing", func(c *kividbv1alpha1.KividbCluster, pods []*corev1.Pod) []*corev1.Pod {
			c.Status.Pods = slices.Delete(c.Status.Pods, 1, 2)
			return slices.Delete(pods, 1, 2)
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, pods, op := replicatedForRestart(2, "c1-2", "c1-0")
			objs := []client.Object{c, op}
			for _, p := range tt.change(c, pods) {
				objs = append(objs, p)
			}
			r := newDbOpsReconciler(t, objs...)
			reconcileOp(t, r, op)
			if deleted := podExists(t, r, "c1-2") == nil; deleted != tt.deleted {
				t.Fatalf("c1-2 deleted: %v, want %v", deleted, tt.deleted)
			}
		})
	}
}

// Restart clicked twice in the GUI, a few seconds apart: two ops on one
// cluster with replicas: 2 must not take both replicas down together.
func TestDbOpsTwoRestartsOfOneCluster(t *testing.T) {
	c, pods, a := replicatedForRestart(2, "c1-1", "c1-2", "c1-0")
	b := a.DeepCopy()
	b.Name = "restart-2"
	a.Status.Restart.PendingPods = []string{"c1-2", "c1-0"}
	a.Status.Restart.CompletedPods = []string{"c1-1"}
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], a, b)
	reconcileOp(t, r, a)
	reconcileOp(t, r, b)
	if podExists(t, r, "c1-1") == nil && podExists(t, r, "c1-2") == nil {
		t.Fatal("both replicas deleted at once")
	}
}

// A replica that is not Ready, still to be restarted, is restarted first:
// it is what every other restart waits for (reconcileRollout, too, takes
// an unhealthy pod first). Then the op goes on with the rest.
func TestDbOpsRestartsAnUnhealthyReplicaFirst(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2", "c1-0")
	c.Status.Pods[2].Ready, c.Status.Pods[2].Synced = false, false
	pods[2].Status.Conditions[0].Status = corev1.ConditionFalse
	pods[2].UID = "old"
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	for range 2 {
		reconcileOp(t, r, op)
	}
	if podExists(t, r, "c1-2") != nil || podExists(t, r, "c1-1") == nil {
		t.Fatal("c1-2, not Ready, not restarted first")
	}

	// c1-2 comes back Ready and in sync; c1-1 is next.
	ctx := context.Background()
	fresh := pods[2].DeepCopy()
	fresh.ResourceVersion, fresh.UID = "", "new"
	fresh.Status.Conditions[0].Status = corev1.ConditionTrue
	if err := r.Create(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), c); err != nil {
		t.Fatal(err)
	}
	c.Status.Pods[2].Ready, c.Status.Pods[2].Synced = true, true
	if err := r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reconcileOp(t, r, op)
	}
	if podExists(t, r, "c1-1") != nil {
		t.Fatal("c1-1 not restarted after c1-2")
	}
}

// With two replicas unhealthy, neither is restarted ahead of the other in
// turn, endlessly: the op waits (and so is bounded, below).
func TestDbOpsTwoUnhealthyReplicasWait(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2", "c1-0")
	for _, i := range []int{1, 2} {
		c.Status.Pods[i].Ready = false
		pods[i].Status.Conditions[0].Status = corev1.ConditionFalse
	}
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	for range 3 {
		reconcileOp(t, r, op)
	}
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition) == nil {
		t.Fatalf("not waiting; pending %v", got.Status.Restart.PendingPods)
	}
}

// The wait for the rest of the cluster is bounded like the handover: an
// op that has waited dbOpsHandoverTimeout for another pod fails, naming
// it. A handover start left from an earlier pod plays no part in it.
func TestDbOpsRestartWaitTimesOut(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-2", "c1-0")
	pods[1].Status.Conditions[0].Status = corev1.ConditionFalse
	long := metav1.NewTime(time.Now().Add(-dbOpsHandoverTimeout - time.Second))
	op.Status.Restart.HandoverStartTime = &long
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	reconcileOp(t, r, op)

	ctx := context.Background()
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Restart.HandoverStartTime != nil {
		t.Fatal("handover start of an earlier pod kept while waiting")
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || cond == nil {
		t.Fatalf("phase %s, condition %v: want Running and the wait recorded", got.Status.Phase, cond)
	}
	cond.LastTransitionTime = long
	if err := r.Status().Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsFailed || !strings.Contains(got.Status.Error, "c1-1 is not Ready") {
		t.Fatalf("phase %s, error %q: want Failed naming c1-1", got.Status.Phase, got.Status.Error)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition); cond != nil {
		t.Fatalf("failed op still shows %v", cond)
	}
	if podExists(t, r, "c1-2") == nil {
		t.Fatal("c1-2 deleted")
	}
}

// The wait ends when the cluster is healthy again, and a later wait (for
// another pod) starts its own 5 minutes rather than inheriting the first.
func TestDbOpsWaitEndsWhenTheClusterIsHealthy(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2")
	long := metav1.NewTime(time.Now().Add(-dbOpsHandoverTimeout - time.Second))
	op.Status.Conditions = []metav1.Condition{{Type: DbOpsWaitingCondition, Status: metav1.ConditionTrue, Reason: "PodNotHealthy", LastTransitionTime: long}}
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	reconcileOp(t, r, op)
	if podExists(t, r, "c1-1") != nil {
		t.Fatal("c1-1 not restarted with every other pod healthy")
	}

	// c1-1 comes back; then the master falls out of sync before c1-2.
	ctx := context.Background()
	fresh := pods[1].DeepCopy()
	fresh.ResourceVersion, fresh.UID = "", "new"
	if err := r.Create(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), c); err != nil {
		t.Fatal(err)
	}
	c.Status.Pods[0].Synced = false
	if err := r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || cond == nil || !cond.LastTransitionTime.After(long.Time) {
		t.Fatalf("phase %s, error %q, condition %v: want Running and a new wait", got.Status.Phase, got.Status.Error, cond)
	}
}

// restartOpAt is a fresh restart of cluster, created at created.
func restartOpAt(name, cluster string, created time.Time) *kividbv1alpha1.KividbDbOps {
	return &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", CreationTimestamp: metav1.NewTime(created)},
		Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: cluster}, Op: kividbv1alpha1.DbOpsRestart},
	}
}

// One restart per cluster at a time: an op that has started goes on, then
// the oldest (by creation, then name); the others wait without starting.
func TestDbOpsOneOpPerCluster(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	op := func(name, cluster string, created time.Duration, phase kividbv1alpha1.DbOpsPhase) *kividbv1alpha1.KividbDbOps {
		o := restartOpAt(name, cluster, now.Add(created))
		o.Status.Phase = phase
		return o
	}
	tests := []struct {
		name  string
		other *kividbv1alpha1.KividbDbOps
		waits bool
	}{
		{"an older op running", op("restart-1", "c1", -3*time.Second, kividbv1alpha1.DbOpsRunning), true},
		{"an older op not started yet", op("restart-1", "c1", -3*time.Second, ""), true},
		{"an op of the same second, earlier by name", op("restart-1", "c1", 0, ""), true},
		{"an op of the same second, later by name", op("restart-3", "c1", 0, ""), false},
		{"a younger op that has started", op("restart-3", "c1", 3*time.Second, kividbv1alpha1.DbOpsRunning), true},
		{"an older op completed", op("restart-1", "c1", -3*time.Second, kividbv1alpha1.DbOpsCompleted), false},
		{"an older op failed", op("restart-1", "c1", -3*time.Second, kividbv1alpha1.DbOpsFailed), false},
		{"an older op of another cluster", op("restart-1", "c2", -3*time.Second, kividbv1alpha1.DbOpsRunning), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, pods, _ := replicatedForRestart(2)
			mine := restartOpAt("restart-2", "c1", now)
			r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], tt.other, mine)
			reconcileOp(t, r, mine)
			var got kividbv1alpha1.KividbDbOps
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(mine), &got); err != nil {
				t.Fatal(err)
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsQueuedCondition)
			if !tt.waits {
				if got.Status.Phase != kividbv1alpha1.DbOpsRunning || cond != nil {
					t.Fatalf("phase %s, condition %v: want Running", got.Status.Phase, cond)
				}
				return
			}
			if got.Status.Phase != kividbv1alpha1.DbOpsPending || got.Status.StartTime != nil || got.Status.Restart != nil {
				t.Fatalf("phase %s, start %v, restart %v: want Pending and not started", got.Status.Phase, got.Status.StartTime, got.Status.Restart)
			}
			if cond == nil || cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, tt.other.Name) || !strings.Contains(got.Status.Message, tt.other.Name) {
				t.Fatalf("condition %v, message %q: want both to name %s", cond, got.Status.Message, tt.other.Name)
			}
		})
	}
}

// Two ops already running on one cluster (started by an operator without
// the queue): the younger stops, and its waits start over when it goes on
// rather than failing it at once.
func TestDbOpsQueuedOpKeepsNoTimers(t *testing.T) {
	c, pods, mine := replicatedForRestart(2, "c1-2", "c1-0")
	now := time.Now()
	long := metav1.NewTime(now.Add(-dbOpsHandoverTimeout - time.Second))
	mine.CreationTimestamp = metav1.NewTime(now)
	mine.Status.Restart.HandoverStartTime = &long
	mine.Status.Conditions = []metav1.Condition{{Type: DbOpsWaitingCondition, Status: metav1.ConditionTrue, Reason: "PodNotHealthy", LastTransitionTime: long}}
	older := restartOpAt("restart-0", "c1", now.Add(-time.Minute))
	older.Status.Phase = kividbv1alpha1.DbOpsRunning
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], older, mine)
	reconcileOp(t, r, mine)
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(mine), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || podExists(t, r, "c1-2") == nil {
		t.Fatalf("phase %s, c1-2 deleted %v: want it to wait for restart-0", got.Status.Phase, podExists(t, r, "c1-2") == nil)
	}
	if got.Status.Restart.HandoverStartTime != nil || meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition) != nil {
		t.Fatalf("handover start %v, conditions %v: want both cleared while queued", got.Status.Restart.HandoverStartTime, got.Status.Conditions)
	}
}

// An op whose cluster is gone fails at once, as before, whatever else is
// queued for that cluster.
func TestDbOpsQueuedOpOfAMissingCluster(t *testing.T) {
	now := time.Now()
	a := restartOpAt("restart-1", "gone", now.Add(-3*time.Second))
	a.Status.Phase = kividbv1alpha1.DbOpsRunning
	b := restartOpAt("restart-2", "gone", now)
	r := newDbOpsReconciler(t, a, b)
	for _, op := range []*kividbv1alpha1.KividbDbOps{b, a} {
		reconcileOp(t, r, op)
		var got kividbv1alpha1.KividbDbOps
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(op), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != kividbv1alpha1.DbOpsFailed || !strings.Contains(got.Status.Error, `cluster "gone" not found`) {
			t.Fatalf("%s: phase %s, error %q: want Failed, cluster not found", op.Name, got.Status.Phase, got.Status.Error)
		}
	}
}

// Restart clicked twice in the GUI, 3 s apart: the second op starts when
// the first has completed, and each restarts every pod once, in turn. The
// loop stands in for the StatefulSet (a deleted pod comes back) and the
// cluster controller (a master asked to step down hands over).
func TestDbOpsDoubleClickRestartsOneAfterTheOther(t *testing.T) {
	c, pods, _ := replicatedForRestart(2)
	now := time.Now().Truncate(time.Second)
	a := restartOpAt("c1-restart-1", "c1", now.Add(-3*time.Second))
	b := restartOpAt("c1-restart-2", "c1", now)
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], a, b)
	ctx := context.Background()
	handOver := func(from *corev1.Pod) {
		if err := r.Get(ctx, client.ObjectKeyFromObject(c), c); err != nil {
			t.Fatal(err)
		}
		to := "c1-1"
		if from.Name == to {
			to = "c1-2"
		}
		c.Status.MasterPod = to
		for i := range c.Status.Pods {
			c.Status.Pods[i].Role = kividbv1alpha1.RoleReplica
			if c.Status.Pods[i].Name == to {
				c.Status.Pods[i].Role = kividbv1alpha1.RoleMaster
			}
		}
		if err := r.Status().Update(ctx, c); err != nil {
			t.Fatal(err)
		}
		delete(from.Annotations, StepDownAnnotation)
		from.Labels[kividbv1alpha1.RoleLabel] = string(kividbv1alpha1.RoleReplica)
		if err := r.Update(ctx, from); err != nil {
			t.Fatal(err)
		}
	}
	finished := func(op *kividbv1alpha1.KividbDbOps) *kividbv1alpha1.KividbDbOps {
		var got kividbv1alpha1.KividbDbOps
		if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
			t.Fatal(err)
		}
		return &got
	}
	var restarts []string // "op/pod", in order
	for i := 0; i < 100 && finished(b).Status.CompletionTime == nil; i++ {
		for _, op := range []*kividbv1alpha1.KividbDbOps{a, b} {
			reconcileOp(t, r, op)
			for _, p := range pods {
				switch cur := podExists(t, r, p.Name); {
				case cur == nil:
					restarts = append(restarts, op.Name+"/"+p.Name)
					fresh := p.DeepCopy()
					fresh.ResourceVersion, fresh.UID = "", types.UID(fmt.Sprint(len(restarts)))
					if err := r.Create(ctx, fresh); err != nil {
						t.Fatal(err)
					}
				case cur.Annotations[StepDownAnnotation] == "true":
					handOver(cur)
				}
			}
		}
	}
	for _, op := range []*kividbv1alpha1.KividbDbOps{a, b} {
		if got := finished(op); got.Status.Phase != kividbv1alpha1.DbOpsCompleted {
			t.Fatalf("%s: phase %s, error %q; restarts %v", op.Name, got.Status.Phase, got.Status.Error, restarts)
		}
	}
	if len(restarts) != 6 {
		t.Fatalf("restarts %v: want each of 3 pods once per op", restarts)
	}
	for i, x := range restarts {
		want := a.Name
		if i >= 3 {
			want = b.Name
		}
		if !strings.HasPrefix(x, want+"/") || slices.Contains(restarts[:i], x) {
			t.Fatalf("restarts %v: want %s's 3 pods, then %s's", restarts, a.Name, b.Name)
		}
	}
}

// A restarted pod that does not come back fails the op after
// dbOpsRestartTimeout, naming it, and the next op of the cluster goes on.
func TestDbOpsRestartedPodNotBackTimesOut(t *testing.T) {
	// A pod the kubelet still lets start must not fail the op.
	probe := kividbStartupProbe()
	if ceiling := time.Duration(probe.PeriodSeconds*probe.FailureThreshold) * time.Second; dbOpsRestartTimeout <= ceiling {
		t.Fatalf("dbOpsRestartTimeout %s is not above the startup probe's %s", dbOpsRestartTimeout, ceiling)
	}
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2", "c1-0")
	now := time.Now()
	op.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	pods[1].UID = "deleted"
	op.Status.Restart.CurrentPod, op.Status.Restart.CurrentPodUID = "c1-1", "deleted"
	next := restartOpAt("restart-next", "c1", now)
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op, next)
	reconcileOp(t, r, op)

	ctx := context.Background()
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || cond == nil {
		t.Fatalf("phase %s, condition %v: want Running and the wait recorded", got.Status.Phase, cond)
	}
	cond.LastTransitionTime = metav1.NewTime(now.Add(-dbOpsRestartTimeout - time.Second))
	if err := r.Status().Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsFailed || !strings.Contains(got.Status.Error, "c1-1") || !strings.Contains(got.Status.Error, "to terminate") {
		t.Fatalf("phase %s, error %q: want Failed naming c1-1", got.Status.Phase, got.Status.Error)
	}
	reconcileOp(t, r, next)
	if err := r.Get(ctx, client.ObjectKeyFromObject(next), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning {
		t.Fatalf("next op: phase %s, message %q: want Running", got.Status.Phase, got.Status.Message)
	}
}

// The wait for a restarted pod ends when it is back: a wait of the health
// gate after it starts its own 5 minutes.
func TestDbOpsRestartWaitIsNotTheGateWait(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2")
	ten := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	op.Status.Restart.CurrentPod, op.Status.Restart.CurrentPodUID = "c1-1", "deleted"
	op.Status.Conditions = []metav1.Condition{{Type: DbOpsWaitingCondition, Status: metav1.ConditionTrue, Reason: "PodRestarting", LastTransitionTime: ten}}
	c.Status.Pods[0].Synced = false // the master, so the gate holds c1-2
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	for range 2 {
		reconcileOp(t, r, op)
	}
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || !slices.Contains(got.Status.Restart.CompletedPods, "c1-1") || cond == nil || !cond.LastTransitionTime.After(ten.Time) {
		t.Fatalf("phase %s, error %q, completed %v, condition %v: want c1-1 done and a new wait", got.Status.Phase, got.Status.Error, got.Status.Restart.CompletedPods, cond)
	}
}

// The wait for a restarted pod starts when the op deletes it, not when an
// earlier wait of the gate began.
func TestDbOpsRestartWaitStartsWhenThePodIsDeleted(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1", "c1-2")
	c.Status.Pods[0].Synced = false
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	reconcileOp(t, r, op) // the gate waits for c1-0

	ctx := context.Background()
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if cond == nil || cond.Reason != "PodNotHealthy" {
		t.Fatalf("condition %v: want the gate waiting", cond)
	}
	gateStart := metav1.NewTime(time.Now().Add(-4 * time.Minute))
	cond.LastTransitionTime = gateStart
	if err := r.Status().Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), c); err != nil {
		t.Fatal(err)
	}
	c.Status.Pods[0].Synced = true
	if err := r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	deleted := time.Now().Add(-time.Second) // LastTransitionTime has 1 s resolution
	reconcileOp(t, r, op)                   // the gate passes; c1-1 is deleted
	if podExists(t, r, "c1-1") != nil {
		t.Fatal("c1-1 not restarted once the cluster was healthy")
	}
	reconcileOp(t, r, op) // waits for c1-1 to be recreated
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond = meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if cond == nil || cond.Reason != "PodRestarting" || cond.LastTransitionTime.Time.Before(deleted) {
		t.Fatalf("condition %v: want PodRestarting since the delete, not since the gate wait at %s", cond, gateStart.Format(time.RFC3339))
	}
}

// A gate wait that ends because next became the master (a failover
// meanwhile) does not carry its start into the gate after the handover.
func TestDbOpsGateWaitDoesNotSpanAHandover(t *testing.T) {
	c, pods, op := replicatedForRestart(2, "c1-1")
	old := metav1.NewTime(time.Now().Add(-dbOpsHandoverTimeout - 30*time.Second))
	op.Status.Conditions = []metav1.Condition{{Type: DbOpsWaitingCondition, Status: metav1.ConditionTrue, Reason: "PodNotHealthy", LastTransitionTime: old}}
	c.Status.MasterPod = "c1-1"
	c.Status.Pods[0].Role, c.Status.Pods[1].Role = kividbv1alpha1.RoleReplica, kividbv1alpha1.RoleMaster
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], op)
	reconcileOp(t, r, op) // handover requested
	ctx := context.Background()
	var cl kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &cl); err != nil {
		t.Fatal(err)
	}
	cl.Status.MasterPod = "c1-2"
	cl.Status.Pods[1].Role, cl.Status.Pods[2].Role = kividbv1alpha1.RoleReplica, kividbv1alpha1.RoleMaster
	cl.Status.Pods[0].Synced = false
	if err := r.Status().Update(ctx, &cl); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning {
		t.Fatalf("phase %s, error %q: want Running, a new gate wait", got.Status.Phase, got.Status.Error)
	}
}

// An op that finds no pod to restart waits, but not forever.
func TestDbOpsNoPodsTimesOut(t *testing.T) {
	c := testCluster("c1")
	c.Spec.Replicas = 1
	op := restartOpAt("restart", "c1", time.Now())
	op.Status.Phase = kividbv1alpha1.DbOpsRunning
	r := newDbOpsReconciler(t, c, op)
	reconcileOp(t, r, op)
	ctx := context.Background()
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsWaitingCondition)
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || cond == nil {
		t.Fatalf("phase %s, condition %v: want Running and the wait recorded", got.Status.Phase, cond)
	}
	cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-dbOpsRestartTimeout - time.Second))
	if err := r.Status().Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	reconcileOp(t, r, op)
	if err := r.Get(ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.DbOpsFailed || !strings.Contains(got.Status.Error, "no pods to restart") {
		t.Fatalf("phase %s, error %q: want Failed, no pods", got.Status.Phase, got.Status.Error)
	}
}

// Two ops running at once, created the same second (both started before
// the queue, or in the start race): the later by name, reconciled first,
// waits, and the earlier goes on.
func TestDbOpsTwoRunningOpsOfTheSameSecond(t *testing.T) {
	c, pods, a := replicatedForRestart(2, "c1-1", "c1-2", "c1-0")
	a.Name, a.CreationTimestamp = "restart-1", metav1.NewTime(time.Now().Truncate(time.Second))
	b := a.DeepCopy()
	b.Name = "restart-2"
	r := newDbOpsReconciler(t, c, pods[0], pods[1], pods[2], a, b)
	reconcileOp(t, r, b)
	var got kividbv1alpha1.KividbDbOps
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(b), &got); err != nil {
		t.Fatal(err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, DbOpsQueuedCondition); cond == nil || !strings.Contains(cond.Message, "restart-1") || podExists(t, r, "c1-1") == nil {
		t.Fatalf("restart-2: condition %v, c1-1 deleted %v: want it queued behind restart-1", cond, podExists(t, r, "c1-1") == nil)
	}
	reconcileOp(t, r, a)
	if podExists(t, r, "c1-1") != nil {
		t.Fatal("restart-1 did not go on")
	}
}
