package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

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
	// A cluster in use: every node holds keys. A master that holds none is
	// not held by the rollout (#108), and the tests of that set it to 0.
	for _, n := range h.engine.nodes {
		n.keys = 100
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
	h.r.reconcileShardedRollout(ctx, h.c, h.c.Status.DeepCopy(), observed, h.kdbConfig)
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
	// s-0-0's only replica is not in sync: s-0-0 still hands over to it.
	if !hasReplica(c, "s-0-0") {
		t.Fatal("s-0-1 not counted for its own shard")
	}
	if !hasReplica(c, "s-1-1") {
		t.Fatal("s-1-0 not counted for its own shard")
	}
	// Without s-0-1, s-0-0 has none: s-1-0 is another shard's replica.
	alone := c.DeepCopy()
	alone.Status.Pods = slices.DeleteFunc(alone.Status.Pods, func(ps kividbv1alpha1.KividbPodStatus) bool { return ps.Name == "s-0-1" })
	if hasReplica(alone, "s-0-0") {
		t.Fatal("another shard's replica counted")
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

// appendOnlyOff is a KividbConfig that turns the append-only file off.
func appendOnlyOff() *kividbv1alpha1.KividbConfig {
	return &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "no-aof", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbConfigSpec{Directives: map[string]string{"appendonly": "no"}},
	}
}

// allowRestartWithoutPersistence stores the cluster with the annotation
// set, as kubectl annotate would, and sets it on h.c.
func (h *formingHarness) allowRestartWithoutPersistence() {
	h.t.Helper()
	if h.c.Annotations == nil {
		h.c.Annotations = map[string]string{}
	}
	h.c.Annotations[kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation] = "true"
	stored := h.c.DeepCopy()
	if err := h.r.Create(context.Background(), stored); err != nil {
		h.t.Fatal(err)
	}
	h.c.ResourceVersion = stored.ResourceVersion
}

// A shard master with no replica to hand over to is restarted in place:
// with the append-only file off, its data does not survive that (#90). The
// rollout holds it, says why on RolloutProgressing, and once in a Warning
// event, not on every pass.
func TestShardedRolloutHoldsAMasterWithNoReplicaAndNoPersistence(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = appendOnlyOff()
	_ = events(h.r)
	for i := 0; i < 3; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want every master held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WouldLoseData" ||
		!strings.Contains(cond.Message, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation) {
		t.Fatalf("condition %+v, want False, WouldLoseData, naming the annotation", cond)
	}
	if ev := events(h.r); strings.Count(ev, "RolloutHeld") != 1 {
		t.Fatalf("events:\n%s\nwant one RolloutHeld", ev)
	}
}

// The annotation lets the rollout restart them, each with a Warning that
// its data is lost, and is removed once no pod is outdated, so that it does
// not let a later rollout through.
func TestShardedRolloutAnnotationRestartsMastersWithoutPersistence(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = appendOnlyOff()
	h.allowRestartWithoutPersistence()
	_ = events(h.r)
	var order []string
	for i := 0; i < 10 && len(h.outdated()) > 0; i++ {
		order = append(order, h.rolloutPass()...)
	}
	if len(order) != 2 || len(h.outdated()) > 0 {
		t.Fatalf("replaced %v, outdated %v", order, h.outdated())
	}
	if ev := events(h.r); strings.Count(ev, "RestartingWithoutPersistence") != 2 {
		t.Fatalf("events:\n%s\nwant a RestartingWithoutPersistence per master", ev)
	}
	h.rolloutPass() // nothing is outdated now
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.c), &stored); err != nil {
		t.Fatal(err)
	}
	if _, ok := stored.Annotations[kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation]; ok {
		t.Fatal("the annotation is still set after the rollout")
	}
}

// What the master's engine says counts too: the KividbConfig may not have
// reached it yet. An agent from before 0.7.0 does not say, and is not taken
// for one with the file off.
func TestShardedRolloutReadsPersistenceFromTheMaster(t *testing.T) {
	for _, tc := range []struct {
		name     string
		set      func(*fakeNode)
		wantHeld bool
	}{
		{"engine reports the file off", func(n *fakeNode) { n.aofOff = true }, true},
		{"agent too old to say", func(n *fakeNode) { n.oldAgent = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 1, 0)
			tc.set(h.node(shardPodName(h.c, 0, 0)))
			deleted := h.rolloutPass()
			if held := len(deleted) == 0; held != tc.wantHeld {
				t.Fatalf("replaced %v, want held=%v", deleted, tc.wantHeld)
			}
		})
	}
}

// With the file on, a master with no replica is restarted in place as
// before, but with a Warning first: its slots are down until it is back.
func TestShardedRolloutWarnsBeforeRestartingAMasterInPlace(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	_ = events(h.r)
	for i := 0; i < 10 && len(h.outdated()) > 0; i++ {
		h.rolloutPass()
	}
	if len(h.outdated()) > 0 {
		t.Fatalf("outdated %v", h.outdated())
	}
	if ev := events(h.r); strings.Count(ev, "RestartingInPlace") != 2 {
		t.Fatalf("events:\n%s\nwant a RestartingInPlace per master", ev)
	}
}

// The file is on or off per master (CONFIG SET appendonly yes reaches one
// engine at a time), so a held master does not keep the others back: the
// rollout restarts those it can, and reports the hold once only held ones
// are left.
func TestShardedRolloutReplacesTheMastersItDoesNotHold(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	// s-1-0 comes first by name: with its file off, it must not hold s-0-0.
	h.node(shardPodName(h.c, 1, 0)).aofOff = true
	_ = events(h.r)
	var order []string
	for i := 0; i < 5; i++ {
		order = append(order, h.rolloutPass()...)
	}
	if fmt.Sprint(order) != "[s-0-0]" || fmt.Sprint(h.outdated()) != "[s-1-0]" {
		t.Fatalf("replaced %v, outdated %v; want s-0-0 replaced and s-1-0 held", order, h.outdated())
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-1-0") || strings.Contains(cond.Message, "s-0-0") {
		t.Fatalf("condition %+v, want WouldLoseData naming s-1-0 only", cond)
	}
	if ev := events(h.r); strings.Count(ev, "RolloutHeld") != 1 {
		t.Fatalf("events:\n%s\nwant one RolloutHeld", ev)
	}
}

// Every master held: the condition names them all, so that one look says
// what is left to do.
func TestShardedRolloutNamesEveryMasterItHolds(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = appendOnlyOff()
	h.rolloutPass()
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0") || !strings.Contains(cond.Message, "s-1-0") {
		t.Fatalf("condition %+v, want WouldLoseData naming s-0-0 and s-1-0", cond)
	}
}

// What the hold says to do lets the rollout go on: the file turned on, or a
// replica added (each master then hands over). The condition goes once
// every pod is on the current template.
func TestShardedRolloutGoesOnOnceTheMasterIsSafe(t *testing.T) {
	for _, tc := range []struct {
		name string
		act  func(h *formingHarness)
	}{
		{"the file turned on", func(h *formingHarness) {
			// appendonly yes in the KividbConfig, and CONFIG SET appendonly
			// yes on each running master.
			h.kdbConfig.Spec.Directives["appendonly"] = "yes"
			for s := int32(0); s < 2; s++ {
				h.node(shardPodName(h.c, s, 0)).aofOff = false
			}
		}},
		{"a replica added", func(h *formingHarness) {
			h.c.Spec.Replicas = 1
			for s := int32(0); s < 2; s++ {
				ip := fmt.Sprintf("10.0.%d.2", s)
				p := shardedPod(h.c, s, 1, ip)
				p.Labels[appsv1.StatefulSetRevisionLabel] = "new"
				if err := h.r.Create(context.Background(), p); err != nil {
					h.t.Fatal(err)
				}
				h.engine.add(nodeID(p.Name), ip)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 2, 0)
			h.kdbConfig = appendOnlyOff()
			for s := int32(0); s < 2; s++ {
				h.node(shardPodName(h.c, s, 0)).aofOff = true
			}
			if deleted := h.rolloutPass(); len(deleted) > 0 {
				t.Fatalf("replaced %v, want every master held", deleted)
			}
			tc.act(h)
			for i := 0; i < 30 && len(h.outdated()) > 0; i++ {
				h.rolloutPass()
			}
			if left := h.outdated(); len(left) > 0 {
				t.Fatalf("still outdated: %v; condition %+v", left, findCondition(h.c.Status.Conditions, RolloutProgressingCondition))
			}
			h.rolloutPass()
			if cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition); cond != nil {
				t.Fatalf("condition %+v left after the rollout", cond)
			}
		})
	}
}

// A master that has been unready on an old template for stuckPodGrace,
// with no kividb container running (here it has none), holds nothing in
// memory, so the hold does not keep it: it is replaced as any stuck pod
// is. One whose container runs stays held (shard_stuck_test.go).
func TestShardedRolloutReplacesAnUnreadyMasterItHolds(t *testing.T) {
	h := formedForRollout(t, 1, 0)
	h.kdbConfig = appendOnlyOff()
	if deleted := h.rolloutPass(); len(deleted) > 0 {
		t.Fatalf("replaced %v, want the master held", deleted)
	}
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 0)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * stuckPodGrace))}}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	_ = events(h.r)
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want the unready master s-0-0", deleted)
	}
	if ev := events(h.r); !strings.Contains(ev, "StuckPodReplaced") {
		t.Fatalf("events:\n%s\nwant StuckPodReplaced", ev)
	}
}

// A master that reports no keys loses nothing to a restart in place, so it
// is not held (#108). Only on a count its agent knows (one from before
// 0.7.0 reports none), and not while its engine loads its data, when it can
// still show 0.
func TestShardedRolloutHoldsOnlyAMasterThatMayHoldKeys(t *testing.T) {
	for _, tc := range []struct {
		name     string
		set      func(*fakeNode)
		wantHeld bool
	}{
		{"no keys", func(n *fakeNode) { n.keys = 0 }, false},
		{"keys", func(n *fakeNode) {}, true},
		{"no keys while loading", func(n *fakeNode) { n.keys, n.loading = 0, true }, true},
		{"agent too old to count", func(n *fakeNode) { n.keys, n.oldAgent = 0, true }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 1, 0)
			h.kdbConfig = appendOnlyOff()
			tc.set(h.node(shardPodName(h.c, 0, 0)))
			deleted := h.rolloutPass()
			if held := len(deleted) == 0; held != tc.wantHeld {
				t.Fatalf("replaced %v, want held=%v", deleted, tc.wantHeld)
			}
		})
	}
}

// It is restarted with the RestartingInPlace Warning, as with the file on
// (its slots are down until it is back), and RolloutProgressing says it
// went ahead because the master reported no keys.
func TestShardedRolloutSaysWhyItRestartsAnEmptyMaster(t *testing.T) {
	h := formedForRollout(t, 1, 0)
	h.kdbConfig = appendOnlyOff()
	h.node(shardPodName(h.c, 0, 0)).keys = 0
	_ = events(h.r)
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want s-0-0", deleted)
	}
	if ev := events(h.r); !strings.Contains(ev, "RestartingInPlace") || strings.Contains(ev, "RestartingWithoutPersistence") {
		t.Fatalf("events:\n%s\nwant RestartingInPlace and no RestartingWithoutPersistence", ev)
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, "s-0-0") || !strings.Contains(cond.Message, "no keys") {
		t.Fatalf("condition %+v, want True, saying s-0-0 reported no keys", cond)
	}
}

// An empty master does not wait behind one that is held: the rollout
// restarts it, then reports the hold for the other alone.
func TestShardedRolloutRestartsAnEmptyMasterBesideAHeldOne(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = appendOnlyOff()
	// s-1-0 comes first by name, and holds keys.
	h.node(shardPodName(h.c, 0, 0)).keys = 0
	_ = events(h.r)
	var order []string
	for i := 0; i < 5; i++ {
		order = append(order, h.rolloutPass()...)
	}
	if fmt.Sprint(order) != "[s-0-0]" || fmt.Sprint(h.outdated()) != "[s-1-0]" {
		t.Fatalf("replaced %v, outdated %v; want s-0-0 replaced and s-1-0 held", order, h.outdated())
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-1-0") || strings.Contains(cond.Message, "s-0-0") {
		t.Fatalf("condition %+v, want WouldLoseData naming s-1-0 only", cond)
	}
	if ev := events(h.r); strings.Count(ev, "RolloutHeld") != 1 {
		t.Fatalf("events:\n%s\nwant one RolloutHeld", ev)
	}
}

// Below 10 s of grace the kubelet may kill the engine before or during its
// save at shutdown, and a write that reached an empty master after its
// agent's report would be lost: the master is held like one with keys.
func TestShardedRolloutHoldsAnEmptyMasterWithAShortGracePeriod(t *testing.T) {
	for _, tc := range []struct {
		grace    int64
		wantHeld bool
	}{
		{0, true},
		{9, true},
		{10, false},
	} {
		t.Run(fmt.Sprintf("%d s", tc.grace), func(t *testing.T) {
			h := formedForRollout(t, 1, 0)
			h.kdbConfig = appendOnlyOff()
			h.c.Spec.TerminationGracePeriodSeconds = &tc.grace
			h.node(shardPodName(h.c, 0, 0)).keys = 0
			deleted := h.rolloutPass()
			if held := len(deleted) == 0; held != tc.wantHeld {
				t.Fatalf("replaced %v, want held=%v", deleted, tc.wantHeld)
			}
		})
	}
}

// Only this pass's report counts: an empty master whose pod is not Ready
// (for less than stuckPodGrace), or whose agent does not answer, is held,
// though buildView does not count it as a master. The not-Ready one's
// engine runs and its probe says 0 keys: a probe does not count either.
// s-1-0, which holds keys, is held too, so that nothing else is replaced.
func TestShardedRolloutHoldsAnEmptyMasterItDidNotHearFrom(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(h *formingHarness)
	}{
		{"pod not Ready", func(h *formingHarness) {
			ctx := context.Background()
			var pod corev1.Pod
			if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 0)}, &pod); err != nil {
				h.t.Fatal(err)
			}
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
				LastTransitionTime: metav1.Now()}}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: kividbContainerName,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			if err := h.r.Status().Update(ctx, &pod); err != nil {
				if err := h.r.Update(ctx, &pod); err != nil {
					h.t.Fatal(err)
				}
			}
		}},
		{"agent does not answer", func(h *formingHarness) { h.node(shardPodName(h.c, 0, 0)).agentDown = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 2, 0)
			h.kdbConfig = appendOnlyOff()
			h.node(shardPodName(h.c, 0, 0)).keys = 0
			tc.set(h)
			if deleted := h.rolloutPass(); len(deleted) > 0 {
				t.Fatalf("replaced %v, want every master held", deleted)
			}
		})
	}
}

// With the annotation set, an empty master is restarted with the
// RestartingInPlace Warning, not RestartingWithoutPersistence: it has
// nothing to lose, whatever the annotation allows.
func TestShardedRolloutAnnotationRestartsAnEmptyMasterInPlace(t *testing.T) {
	h := formedForRollout(t, 1, 0)
	h.kdbConfig = appendOnlyOff()
	h.allowRestartWithoutPersistence()
	h.node(shardPodName(h.c, 0, 0)).keys = 0
	_ = events(h.r)
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want s-0-0", deleted)
	}
	if ev := events(h.r); !strings.Contains(ev, "RestartingInPlace") || strings.Contains(ev, "RestartingWithoutPersistence") {
		t.Fatalf("events:\n%s\nwant RestartingInPlace and no RestartingWithoutPersistence", ev)
	}
}

// With the file on, an empty master is restarted as before: nothing in the
// status about its keys, since they are not why it went ahead.
func TestShardedRolloutRestartsAnEmptyMasterWithTheFileOnAsBefore(t *testing.T) {
	h := formedForRollout(t, 1, 0)
	h.node(shardPodName(h.c, 0, 0)).keys = 0
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want s-0-0", deleted)
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Message != "replacing s-0-0 (0 more to go)" {
		t.Fatalf("condition %+v, want the plain replacing message", cond)
	}
}

// A master whose agent does not answer is held whatever the KividbConfig
// says, its file on here: whether its engine has the file off is not
// known, and the rollout does not restart on a guess. The hold says the
// agent is what to fix. The pass after the agent answers, reporting the
// file on, the master is restarted with the RestartingInPlace Warning.
func TestShardedRolloutHoldsAMasterUntilItsAgentAnswers(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "aof", Namespace: "default"},
		Spec: kividbv1alpha1.KividbConfigSpec{Directives: map[string]string{"appendonly": "yes"}}}
	// Only s-0-0 is left on the old template.
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 1, 0)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Labels[appsv1.StatefulSetRevisionLabel] = "new"
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	h.node(shardPodName(h.c, 0, 0)).agentDown = true
	_ = events(h.r)
	for i := 0; i < 2; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want s-0-0 held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0") ||
		!strings.Contains(cond.Message, "its agent did not answer, so whether its append-only file is on is not known") {
		t.Fatalf("condition %+v, want WouldLoseData saying s-0-0's agent did not answer", cond)
	}
	if ev := events(h.r); strings.Count(ev, "RolloutHeld") != 1 {
		t.Fatalf("events:\n%s\nwant one RolloutHeld", ev)
	}

	h.node(shardPodName(h.c, 0, 0)).agentDown = false
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v once its agent answered, want s-0-0", deleted)
	}
	if ev := events(h.r); !strings.Contains(ev, "RestartingInPlace") {
		t.Fatalf("events:\n%s\nwant RestartingInPlace", ev)
	}
}

// moveDataDir gives every shard's StatefulSet the template of a
// KividbConfig with dir (#112), as a change to it would.
func (h *formingHarness) moveDataDir(dir string) {
	h.t.Helper()
	ctx := context.Background()
	for s := int32(0); s < shardCount(h.c); s++ {
		var sts appsv1.StatefulSet
		if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardStatefulSetName(h.c, s)}, &sts); err != nil {
			h.t.Fatal(err)
		}
		sts.Spec.Template.Spec = dataDirSpec(dir)
		if err := h.r.Update(ctx, &sts); err != nil {
			h.t.Fatal(err)
		}
	}
}

// A KividbConfig dir that moves the engine's files holds a master with no
// replica to resync from, its append-only file on or not: its replacement
// would start empty, and without the nodes.conf it keeps there, as a new
// node, owning no slots (#112). The annotation, which accepts what a
// restart loses of a master whose file is off, does not cover it.
func TestShardedRolloutHoldsAMasterWhoseDirMoves(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.moveDataDir("sub")
	h.allowRestartWithoutPersistence()
	_ = events(h.r)
	for i := 0; i < 3; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want every master held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WouldLoseData" ||
		!strings.Contains(cond.Message, "s-0-0") || !strings.Contains(cond.Message, "s-1-0") || !strings.Contains(cond.Message, "/data/sub") {
		t.Fatalf("condition %+v, want False, WouldLoseData, naming s-0-0, s-1-0 and /data/sub", cond)
	}
	if ev := events(h.r); strings.Count(ev, "RolloutHeld") != 1 {
		t.Fatalf("events:\n%s\nwant one RolloutHeld", ev)
	}
}

// Its files are on the volume, so a master whose dir moves stays held when
// its engine is dead too, unlike one held for its file off (#113).
func TestShardedRolloutHoldsAnUnreadyMasterWhoseDirMoves(t *testing.T) {
	h := formedForRollout(t, 1, 0)
	h.moveDataDir("sub")
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 0)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * stuckPodGrace))}}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	if !engineDead(&pod) {
		t.Fatal("want a pod whose engine is dead")
	}
	if deleted := h.rolloutPass(); len(deleted) > 0 {
		t.Fatalf("replaced %v, want s-0-0 held", deleted)
	}
	if cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition); cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0") {
		t.Fatalf("condition %+v, want WouldLoseData naming s-0-0", cond)
	}
}

// With replicas too: a pod whose dir moves would come back as a new node,
// its nodes.conf left behind, and a dir change on a running sharded
// cluster is not supported yet (#112).
func TestShardedRolloutHoldsADirMoveWithReplicasToo(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	h.moveDataDir("sub")
	for i := 0; i < 3; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want the rollout held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WouldLoseData" ||
		!strings.Contains(cond.Message, "not supported on a running sharded cluster") || !strings.Contains(cond.Message, "Set dir back") ||
		!strings.Contains(cond.Message, "s-0-0, s-0-1, s-1-0, s-1-1 from /data to /data/sub") {
		t.Fatalf("condition %+v, want False, WouldLoseData, naming every pod and saying to set dir back", cond)
	}
	for _, p := range h.pods() {
		if p.Annotations[StepDownAnnotation] != "" {
			t.Fatalf("%s was asked to step down", p.Name)
		}
	}
}

// A stuck master whose dir moves is not replaced as a stuck pod, replicas
// or not: the hold comes first (#112).
func TestShardedRolloutLeavesAStuckMasterWhoseDirMoves(t *testing.T) {
	h := formedForRollout(t, 1, 1)
	h.moveDataDir("sub")
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 0)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * stuckPodGrace))}}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	if deleted := h.rolloutPass(); slices.Contains(deleted, "s-0-0") {
		t.Fatalf("replaced %v, want the stuck master s-0-0 left", deleted)
	}
}
