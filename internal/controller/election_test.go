package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Which replica a failover promotes (#78).

// The replicas' offsets are read one after the other at the start of the
// pass, while the master may still be streaming: one read early looks
// behind one read later. The replica promoted is the one that holds the
// most once the old master is held, read then.
func TestFailover_PromotesTheReplicaThatHoldsTheMostNow(t *testing.T) {
	shortSwitchoverWaits(t)
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(130, "stream-a", "run-0")
	h.agents.status["10.0.0.2"].ReplicationOffset = 100 // c1-1, read early, has 130 by now
	h.agents.status["10.0.0.3"].ReplicationOffset = 120 // c1-2, read later ...
	h.agents.catchUp = map[string]int{"10.0.0.3": -1}   // ... is still at 120
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// No pause was answered, so one may have landed: the old master is
// recorded and demoted as a fenced one (#82). The replica promoted is still
// the one that holds the most when it is decided, not the one that looked
// ahead at the start of the pass.
func TestFailover_PromotesTheReplicaThatHoldsTheMostNowAfterAnUnansweredPause(t *testing.T) {
	defer func(d, p time.Duration) { fenceRetryTimeout, pauseCallTimeout = d, p }(fenceRetryTimeout, pauseCallTimeout)
	fenceRetryTimeout, pauseCallTimeout = 300*time.Millisecond, 600*time.Millisecond
	h := newFenceHarness(t)
	h.agents.pauseUnanswered = map[string]int{"10.0.0.1": 1000}
	h.agents.status["10.0.0.3"].ReplicationOffset = 120     // c1-2 looked ahead at the start of the pass
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 130} // c1-1 holds more by the pick
	h.assertFailoverTo("c1-1")
	if h.storedFence() == nil {
		t.Error("no status.fencedMaster: a pause that may have landed is left to run out")
	}
}

// Replicas that hold as much go by name.
func TestFailover_BreaksATieByName(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.status["10.0.0.3"].ReplicationOffset = 100
	h.assertFailoverTo("c1-1")
}

// The old master is not held -- another engine answers at its address --
// and a replica went on receiving after its offset was read: the replica
// promoted is still the one that holds the most when it is decided.
func TestFailover_PromotesTheReplicaThatHoldsTheMostNowWithoutAFence(t *testing.T) {
	h := newFenceHarness(t)
	h.noRestartWait() // the old master is gone for good here
	h.agents.status["10.0.0.1"] = masterRun(5, "stream-x", "run-other")
	h.agents.status["10.0.0.3"].ReplicationOffset = 120
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 130}
	h.assertFailoverTo("c1-1")
	h.assertNoPause()
}

// replica-priority is a KividbConfig directive, so it is the same on every
// pod and tells no replica from another: the failover does not read it. The
// replica that holds what a restarted master lost is promoted, and no
// replica is asked for its replica-priority.
func TestFailover_PromotesTheReplicaAheadOfARestartedMasterWithoutReadingPriority(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.9", ready: true, agent: masterRun(700, "stream-b", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 990)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: false, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
		{Name: "c1-2", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 990},
	}
	if m, failover := h.reconcile(); m != "c1-1" || !failover {
		t.Fatalf("master=%q failover=%v, want a failover to c1-1, which holds what c1-0 lost", m, failover)
	}
	h.assertNoCall("exec 10.0.0.2 INFO replication")
	h.assertNoCall("exec 10.0.0.3 INFO replication")
}

// shortLiveOffsetBudget makes the replicas' reads end sooner, for a test.
func shortLiveOffsetBudget(t *testing.T, d time.Duration) {
	t.Helper()
	budget := liveOffsetBudget
	liveOffsetBudget = d
	t.Cleanup(func() { liveOffsetBudget = budget })
}

// A replica that cannot say what it holds may hold more than the others:
// with the master gone -- nothing to hold, nothing to catch up with --
// promoting another resyncs away what only it has. c1-1 was read early at
// 1000 and c1-2 at 1200; now c1-1 does not answer (it holds 1500) and
// c1-2 answers 1400. No one is promoted on this pass, a Warning says why,
// and once c1-1 answers it is promoted.
func TestFailover_HoldsWhenAReplicaThatMayBeAheadCannotBeRead(t *testing.T) {
	shortLiveOffsetBudget(t, 200*time.Millisecond)
	h := newFenceHarness(t)
	h.noRestartWait() // the old master is gone for good here
	rec := h.recordEvents()
	delete(h.agents.status, "10.0.0.1")
	h.agents.status["10.0.0.2"].ReplicationOffset = 1000
	h.agents.status["10.0.0.3"].ReplicationOffset = 1200
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 1500, "10.0.0.3": 1400}
	h.agents.roleUnanswered = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
		t.Fatalf("reconcileRoles succeeded, want the failover held (calls %v)", h.agents.calls)
	}
	h.assertNoCall("promote")
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "c1-1") {
		t.Errorf("events %v, want one FailoverBlocked naming c1-1", e)
	}
	h.agents.roleUnanswered = nil
	h.assertFailoverTo("c1-1")
}

// A replica the operator cannot reach while it stays Ready -- its
// readiness goes kubelet -> agent -> engine on its own node, not through
// the operator's network path -- holds the failover for as long as that
// lasts. kividb.io/allow-unread-failover lets it go ahead without that
// replica: the one that holds the most of those that answered is promoted,
// and a Warning says what was given up.
func TestFailover_TheAnnotationLetsAHeldFailoverGoAhead(t *testing.T) {
	shortLiveOffsetBudget(t, 200*time.Millisecond)
	h := newFenceHarness(t)
	h.noRestartWait() // the old master is gone for good here
	rec := h.recordEvents()
	delete(h.agents.status, "10.0.0.1")
	h.agents.status["10.0.0.2"].ReplicationOffset = 1000
	h.agents.status["10.0.0.3"].ReplicationOffset = 1200
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 1500, "10.0.0.3": 1400}
	h.agents.roleUnanswered = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
		t.Fatal("without the annotation the failover went ahead, want it held")
	}
	h.cluster.Annotations = map[string]string{kividbv1alpha1.AllowUnreadFailoverAnnotation: "true"}
	if err := h.r.Update(context.Background(), h.cluster); err != nil {
		t.Fatal(err)
	}
	h.assertFailoverTo("c1-2")
	if e := eventsWith(rec, "FailoverForced"); len(e) != 1 || !strings.Contains(e[0], "c1-1") || !strings.Contains(e[0], kividbv1alpha1.AllowUnreadFailoverAnnotation) || !strings.Contains(e[0], "removed") {
		t.Errorf("events %v, want one FailoverForced naming c1-1, the replica left out, and the annotation, and saying it is removed", e)
	}
	// Gone ahead, the hold is forgotten: a later one counts from its own start.
	if _, ok := h.r.failoverHolds.Load(h.cluster.Namespace + "/" + h.cluster.Name); ok {
		t.Error("the hold is still remembered after the failover went ahead")
	}
	// One failover per annotation: left set, it would skip the hold on every
	// later failover too.
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	if v, ok := stored.Annotations[kividbv1alpha1.AllowUnreadFailoverAnnotation]; ok {
		t.Errorf("%s is still %q after the failover it allowed", kividbv1alpha1.AllowUnreadFailoverAnnotation, v)
	}
}

// A held failover says how long it has been held, from the first pass that
// held it, and what ends it: the network fixed, the replica's pod deleted,
// or the annotation.
func TestFailover_SaysHowLongItHasBeenHeldAndWhatEndsIt(t *testing.T) {
	shortLiveOffsetBudget(t, 200*time.Millisecond)
	h := newFenceHarness(t)
	h.noRestartWait()
	rec := h.recordEvents()
	now := time.Now()
	h.r.clock = func() time.Time { return now }
	delete(h.agents.status, "10.0.0.1")
	h.agents.status["10.0.0.2"].ReplicationOffset = 1000
	h.agents.status["10.0.0.3"].ReplicationOffset = 1200
	h.agents.roleUnanswered = map[string]bool{"10.0.0.2": true}
	start := now
	_, _, _, first := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods())
	if first == nil {
		t.Fatal("pass 1: the failover went ahead, want it held")
	}
	now = now.Add(95 * time.Second)
	// The Ready condition (this error) says since when, in the same words
	// on every pass: the status is written on each, and a write starts the
	// next pass. The event says for how long.
	_, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods())
	if err == nil || err.Error() != first.Error() || !strings.Contains(err.Error(), start.UTC().Format(time.RFC3339)) {
		t.Errorf("pass 2: error %v, want the failover held since %s, in the same words as on pass 1 (%v)", err, start.UTC().Format(time.RFC3339), first)
	}
	e := eventsWith(rec, "FailoverBlocked")
	if len(e) != 2 {
		t.Fatalf("events %v, want a FailoverBlocked on each pass", e)
	}
	for _, want := range []string{"1m35s", "network", "delete", kividbv1alpha1.AllowUnreadFailoverAnnotation} {
		if !strings.Contains(e[1], want) {
			t.Errorf("pass 2 event %q, want it to say %q", e[1], want)
		}
	}
}

// liveViews is the views of h's pods, as a pass starts with.
func liveViews(h *rolesHarness) map[string]*podView {
	views := map[string]*podView{}
	for _, p := range h.pods() {
		v := &podView{pod: &p, ready: isPodReady(&p)}
		v.status = h.agents.status[p.Status.PodIP]
		views[p.Name] = v
	}
	return views
}

// newReplicasHarness: c1-0 is the master of three replicas, each at 100.
func newReplicasHarness(t *testing.T) *rolesHarness {
	t.Helper()
	return newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: master(100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-3", ip: "10.0.0.4", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
	)
}

// The replicas are asked at once: inside the old master's pause, slow ones
// one after another could outlast it, and it would take writes again
// before the promotion.
func TestFailover_AsksTheReplicasForTheirOffsetsAtOnce(t *testing.T) {
	h := newReplicasHarness(t)
	delay := 300 * time.Millisecond
	h.agents.roleDelay = map[string]time.Duration{"10.0.0.2": delay, "10.0.0.3": delay, "10.0.0.4": delay}
	h.agents.liveOffset = map[string]int64{"10.0.0.4": 150}
	start := time.Now()
	name, unread := h.r.electLive(context.Background(), liveViews(h), "c1-0", "")
	// One after another takes 3*delay; at once, delay and some scheduling.
	if took := time.Since(start); took >= 2*delay+delay/2 {
		t.Errorf("reading three replicas that answer in %s took %s: one after another, not at once", delay, took)
	}
	if name != "c1-3" || len(unread) > 0 {
		t.Errorf("picked %q, unread %v; want c1-3, which holds the most, and every replica read", name, unread)
	}
}

// A read that fails at once -- a refused connection, an agent error -- is
// tried again within the budget.
func TestFailover_AsksAReplicaAgainWithinTheBudget(t *testing.T) {
	h := newReplicasHarness(t)
	h.agents.roleFails = map[string]int{"10.0.0.2": 2}
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 150}
	name, unread := h.r.electLive(context.Background(), liveViews(h), "c1-0", "")
	if name != "c1-1" || len(unread) > 0 {
		t.Errorf("picked %q, unread %v; want c1-1, read on a later try", name, unread)
	}
}

// Replicas that never answer cost one budget between them, not one each.
func TestFailover_GivesUpOnTheReplicasAtOneDeadline(t *testing.T) {
	budget := 400 * time.Millisecond
	shortLiveOffsetBudget(t, budget)
	h := newReplicasHarness(t)
	h.agents.roleUnanswered = map[string]bool{"10.0.0.2": true, "10.0.0.3": true}
	start := time.Now()
	name, unread := h.r.electLive(context.Background(), liveViews(h), "c1-0", "")
	// One budget each would be 2*budget.
	if took := time.Since(start); took > budget+300*time.Millisecond {
		t.Errorf("two replicas that never answer took %s, more than the budget (%s)", took, budget)
	}
	slices.Sort(unread)
	if name != "c1-3" || !slices.Equal(unread, []string{"c1-1", "c1-2"}) {
		t.Errorf("picked %q, unread %v; want c1-3, and c1-1 and c1-2 unread", name, unread)
	}
}

// With the old master held, a replica that caught up with it holds
// everything it acknowledged, and no replica can be ahead of that: it is
// promoted though another did not answer.
func TestFailover_PromotesAReplicaThatCaughtUpWithTheHeldMaster(t *testing.T) {
	shortLiveOffsetBudget(t, 200*time.Millisecond)
	shortSwitchoverWaits(t)
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.roleUnanswered = map[string]bool{"10.0.0.3": true}
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverBlocked"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// Held, but the replica picked did not catch up: the one that did not
// answer may hold more. No one is promoted, and the old master's writes are
// let go until the next pass holds them again.
func TestFailover_HoldsWhenThePickDidNotCatchUpAndAReplicaCannotBeRead(t *testing.T) {
	shortLiveOffsetBudget(t, 200*time.Millisecond)
	shortSwitchoverWaits(t)
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(130, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": -1} // c1-1 stays at 100
	h.agents.roleUnanswered = map[string]bool{"10.0.0.3": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
		t.Fatalf("reconcileRoles succeeded, want the failover held (calls %v)", h.agents.calls)
	}
	h.assertNoCall("promote")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "exec 10.0.0.1 CLIENT UNPAUSE")
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "c1-2") {
		t.Errorf("events %v, want one FailoverBlocked naming c1-2", e)
	}
}
