package controller

import (
	"context"
	"strings"
	"testing"
	"time"
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

// Replicas that hold as much are told apart by their replica-priority, the
// lower first, as Sentinel does.
func TestFailover_BreaksATieByReplicaPriority(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.status["10.0.0.3"].ReplicationOffset = 100
	h.agents.priority = map[string]int{"10.0.0.2": 100, "10.0.0.3": 10}
	h.assertFailoverTo("c1-2")
}

// And by name when their priorities are the same too.
func TestFailover_BreaksATieByNameLast(t *testing.T) {
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

// replica-priority 0 means never promoted, as in Sentinel: even a replica
// that holds the most is passed over.
func TestFailover_NeverPromotesAReplicaWithPriorityZero(t *testing.T) {
	shortSwitchoverWaits(t)
	h := newFenceHarness(t)
	h.agents.priority = map[string]int{"10.0.0.2": 0} // c1-1 holds 100 ...
	h.agents.catchUp = map[string]int{"10.0.0.3": -1} // ... c1-2 only 90
	h.assertFailoverTo("c1-2")
}

// Every replica that could take over has replica-priority 0: no failover,
// and the master stays gone, with a Warning saying why. Its writes are not
// held for a promotion that does not come.
func TestFailover_WaitsWhenEveryReplicaHasPriorityZero(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.priority = map[string]int{"10.0.0.2": 0, "10.0.0.3": 0}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		t.Fatal("reconcileRoles succeeded, want the failover blocked")
	}
	h.assertNoCall("promote")
	h.assertNoPause()
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "replica-priority 0") {
		t.Errorf("events %v, want one FailoverBlocked naming replica-priority 0", e)
	}
}
