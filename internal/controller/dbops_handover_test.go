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
	if podExists(t, r, "c1-2") == nil {
		t.Fatal("c1-2 deleted")
	}
}
