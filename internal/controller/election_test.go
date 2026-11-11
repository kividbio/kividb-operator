package controller

import (
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
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
// pod and tells no replica from another: the failover does not read it. A
// replica that holds what a restarted master lost is promoted whatever its
// replica-priority; passing it over would resync those writes away.
func TestFailover_PromotesTheReplicaAheadOfARestartedMasterWhateverItsPriority(t *testing.T) {
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
	h.agents.priority = map[string]int{"10.0.0.2": 0}
	if m, failover := h.reconcile(); m != "c1-1" || !failover {
		t.Fatalf("master=%q failover=%v, want a failover to c1-1, which holds what c1-0 lost", m, failover)
	}
	h.assertNoCall("exec 10.0.0.2 INFO replication")
	h.assertNoCall("exec 10.0.0.3 INFO replication")
}
