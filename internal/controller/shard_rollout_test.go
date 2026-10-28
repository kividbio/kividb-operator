package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// formed returns a harness whose cluster has formed, with every shard's
// StatefulSet at revision "new" and every pod still on an older one.
func formedForRollout(t *testing.T, shards, replicas int32) *formingHarness {
	h := newFormingHarness(t, shards, replicas)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	if h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		t.Fatalf("not formed: %s", h.c.Status.Phase)
	}
	for s := int32(0); s < shards; s++ {
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: shardStatefulSetName(h.c, s), Namespace: h.c.Namespace},
			Status:     appsv1.StatefulSetStatus{UpdateRevision: "new"},
		}
		if err := h.r.Create(context.Background(), sts); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

// rolloutPass is one sharded reconcile, as reconcileSharded orders it,
// and then the StatefulSet controller recreating a deleted pod on the new
// revision, at the same IP and with its volume (so its node ID). It
// returns the pods deleted in this pass.
func (h *formingHarness) rolloutPass() []string {
	ctx := context.Background()
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if _, err := h.r.reconcileClusterTopology(ctx, h.c, observed, h.exposed); err != nil {
		h.t.Fatal(err)
	}
	h.r.labelShardedRoles(ctx, observed)
	h.r.reconcileShardStepDown(ctx, h.c, observed)
	h.r.reconcileShardedRollout(ctx, h.c, observed)
	before := append([]kividbv1alpha1.ShardStatus(nil), h.c.Status.Shards...)
	h.c.Status.Phase, _, _ = shardedStatus(h.c, observed)
	h.r.noteShardFailovers(h.c, before, observed)
	h.r.clearShardStepDowns(ctx, observed)

	present := map[string]bool{}
	for _, p := range h.pods() {
		present[p.Name] = true
	}
	var deleted []string
	for s := int32(0); s < shardCount(h.c); s++ {
		for n := int32(0); n <= h.c.Spec.Replicas; n++ {
			name := shardPodName(h.c, s, n)
			if present[name] {
				continue
			}
			deleted = append(deleted, name)
			if len(h.node(name).slots) > 0 && h.c.Spec.Replicas > 0 {
				h.t.Fatalf("%s was deleted while it owned slots", name)
			}
			p := shardedPod(h.c, s, n, fmt.Sprintf("10.0.%d.%d", s, n+1))
			p.Labels[appsv1.StatefulSetRevisionLabel] = "new"
			if err := h.r.Create(ctx, p); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	return deleted
}

func (h *formingHarness) outdated() []string {
	var out []string
	for _, p := range h.pods() {
		if p.Labels[appsv1.StatefulSetRevisionLabel] != "new" {
			out = append(out, p.Name)
		}
	}
	return out
}

func events(r *KividbClusterReconciler) string {
	ch := r.Recorder.(*record.FakeRecorder).Events
	var out []string
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return strings.Join(out, "\n")
		}
	}
}

func TestShardedRolloutHandsEachMasterOverFirst(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	_ = events(h.r)
	var order []string
	for i := 0; i < 20 && len(h.outdated()) > 0; i++ {
		deleted := h.rolloutPass()
		if len(deleted) > 1 {
			t.Fatalf("more than one pod replaced at once: %v", deleted)
		}
		order = append(order, deleted...)
	}
	if left := h.outdated(); len(left) > 0 {
		t.Fatalf("still outdated: %v (replaced %v)", left, order)
	}
	// The replicas went first; each master was replaced only after it had
	// handed over (rolloutPass fails on a pod deleted with slots).
	if fmt.Sprint(order[:2]) != "[s-1-1 s-0-1]" || len(order) != 4 {
		t.Fatalf("order %v", order)
	}
	for s := int32(0); s < 2; s++ {
		if m := h.node(shardPodName(h.c, s, 1)); len(m.slots) != 1 {
			t.Errorf("shard %d: -1 should be its master now, slots %v", s, m.slots)
		}
		if r := h.node(shardPodName(h.c, s, 0)); r.master != nodeID(shardPodName(h.c, s, 1)) {
			t.Errorf("shard %d: -0 follows %q", s, r.master)
		}
	}
	h.rolloutPass()
	ev := events(h.r)
	if !strings.Contains(ev, "ShardSwitchover") || strings.Contains(ev, "ShardFailover") {
		t.Fatalf("events:\n%s", ev)
	}
	if h.c.Status.LastFailoverTime != nil {
		t.Fatal("a planned switchover was recorded as a failover")
	}
	for _, p := range h.pods() {
		if p.Annotations[StepDownAnnotation] != "" {
			t.Fatalf("%s kept its step-down annotation", p.Name)
		}
	}
}

func TestShardedRolloutWithoutReplicasRestartsMastersOneByOne(t *testing.T) {
	h := formedForRollout(t, 3, 0)
	var order []string
	for i := 0; i < 10 && len(h.outdated()) > 0; i++ {
		deleted := h.rolloutPass()
		if len(deleted) > 1 {
			t.Fatalf("more than one pod replaced at once: %v", deleted)
		}
		order = append(order, deleted...)
	}
	if len(order) != 3 || len(h.outdated()) > 0 {
		t.Fatalf("replaced %v, outdated %v", order, h.outdated())
	}
}

func TestShardedRolloutWaitsForReplicasInSync(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	// s-0-1 lags: it is replaced first (the least use as it is), and after
	// that nothing else moves while it is still not in sync.
	h.node("s-0-1").linkDown = true
	if got := h.rolloutPass(); fmt.Sprint(got) != "[s-0-1]" {
		t.Fatalf("first replaced %v", got)
	}
	for i := 0; i < 5; i++ {
		if got := h.rolloutPass(); len(got) > 0 {
			t.Fatalf("replaced %v while s-0-1 is not in sync", got)
		}
	}
	h.node("s-0-1").linkDown = false
	h.node("s-0-0").offset, h.node("s-0-1").offset = 1<<30, 1<<30-(maxSyncLagBytes+1)
	if got := h.rolloutPass(); len(got) > 0 {
		t.Fatalf("replaced %v while s-0-1 lags", got)
	}
	h.node("s-0-1").offset = 1 << 30
	if got := h.rolloutPass(); len(got) != 1 {
		t.Fatalf("nothing replaced once s-0-1 caught up")
	}
}

func TestShardStepDownAnnotation(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	var pod corev1.Pod
	ctx := context.Background()
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod)
	if err := h.r.requestStepDown(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	// Mark every pod current, so only the step-down acts.
	for _, p := range h.pods() {
		p.Labels[appsv1.StatefulSetRevisionLabel] = "new"
		if err := h.r.Update(ctx, &p); err != nil {
			t.Fatal(err)
		}
	}
	h.rolloutPass()
	if len(h.node("s-1-1").slots) != 1 || h.node("s-1-0").master != nodeID("s-1-1") {
		t.Fatal("shard 1 did not fail over to s-1-1")
	}
	h.rolloutPass()
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod)
	if pod.Annotations[StepDownAnnotation] != "" {
		t.Fatal("annotation kept after the step-down")
	}
	if h.c.Status.Shards[1].MasterPod != "s-1-1" || h.c.Status.LastFailoverTime != nil {
		t.Fatalf("shard 1 status %+v", h.c.Status.Shards[1])
	}

	// A master with no replica in sync stays until it has one.
	h.node("s-0-1").linkDown = true
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-0-0"}, &pod)
	_ = h.r.requestStepDown(ctx, &pod)
	h.rolloutPass()
	if len(h.node("s-0-0").slots) != 1 {
		t.Fatal("stepped down to a replica not in sync")
	}
}

func TestDbOpsMasterOfAShard(t *testing.T) {
	c := shardedCluster("s", 2, 1)
	zero, one := int32(0), int32(1)
	c.Status.Shards = []kividbv1alpha1.ShardStatus{{Index: 0, MasterPod: "s-0-0"}, {Index: 1, MasterPod: "s-1-1"}}
	c.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "s-0-0", Shard: &zero, Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
		{Name: "s-0-1", Shard: &zero, Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: false},
		{Name: "s-1-0", Shard: &one, Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true},
		{Name: "s-1-1", Shard: &one, Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
	}
	if !isMasterPod(c, "s-1-1") || isMasterPod(c, "s-1-0") {
		t.Fatal("isMasterPod")
	}
	// s-0-0's only replica is not in sync; s-1-0 in sync is another shard's.
	if hasSyncedReplica(c, "s-0-0") {
		t.Fatal("another shard's replica counted")
	}
	if !hasSyncedReplica(c, "s-1-1") {
		t.Fatal("s-1-0 not counted for its own shard")
	}
	c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{State: "fail"}
	p := shardedPod(c, 0, 1, "10.0.0.2")
	p.UID = "new"
	if got := restartPending(p, "old", c); got != "for the cluster to serve every slot" {
		t.Fatalf("restartPending: %q", got)
	}
}

// A shard master being deleted (kubectl delete, a drain) hands its slots
// to a replica in sync first, as a step-down would; its preStop hook holds
// the engine meanwhile. Not reported as a failover: it was asked for.
func TestDeletedShardMasterHandsOver(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	ctx := context.Background()
	var pod corev1.Pod
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod)
	pod.Finalizers = []string{"test/hold"}
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	if err := h.r.Delete(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if !h.r.reconcileShardStepDown(ctx, h.c, observed) {
		t.Fatal("no CLUSTER FAILOVER asked for")
	}
	if len(h.node("s-1-1").slots) != 1 || h.node("s-1-0").master != nodeID("s-1-1") {
		t.Fatal("shard 1 did not fail over to s-1-1")
	}
	h.pass()
	if h.c.Status.LastFailoverTime != nil {
		t.Fatal("the asked-for handover was counted as a failover")
	}
}
