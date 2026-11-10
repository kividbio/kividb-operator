package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newFenceHarness: the master c1-0 was force-deleted -- its pod object is
// gone, or replaced by the pods passed in -- while its engine still runs,
// and answers, at the address its status entry recorded (#53).
func newFenceHarness(t *testing.T, replacement ...testPod) *rolesHarness {
	t.Helper()
	pods := append(replacement,
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 90)},
	)
	h := newRolesHarness(t, "c1-0", pods...)
	h.agents.status["10.0.0.1"] = masterRun(100, "stream-a", "run-0")
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Synced: true, ReplicationOffset: 100, RunID: "run-0", PodIP: "10.0.0.1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 100},
		{Name: "c1-2", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 90},
	}
	return h
}

func (h *rolesHarness) pods() []corev1.Pod {
	h.t.Helper()
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

func (h *rolesHarness) assertNoPause() {
	h.t.Helper()
	for _, call := range h.agents.calls {
		if strings.Contains(call, "CLIENT PAUSE") {
			h.t.Errorf("unexpected %q", call)
		}
	}
}

func (h *rolesHarness) assertFailoverTo(want string) {
	h.t.Helper()
	if m, failover := h.reconcile(); m != want || !failover {
		h.t.Fatalf("master=%q failover=%v, want a failover to %s", m, failover, want)
	}
}

// Its writes are held, where it was last seen, before a replica is
// promoted: everything it acknowledges after the promotion is lost.
func TestFailover_FencesTheOldMasterAtItsLastAddress(t *testing.T) {
	h := newFenceHarness(t)
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 60000 WRITE", "promote 10.0.0.2")
	if !h.agents.paused["10.0.0.1"] {
		t.Error("the old master's writes are not held")
	}
}

// The StatefulSet recreated the pod under the same name, with a new
// address: the engine to hold is the old one, at the old address.
func TestFailover_FencesTheOldAddressNotTheReplacementPod(t *testing.T) {
	h := newFenceHarness(t, testPod{name: "c1-0", ip: "10.0.0.9", uid: "new", unreadyFor: time.Minute, agent: masterRun(0, "stream-b", "run-new")})
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 60000 WRITE", "promote 10.0.0.2")
	for _, call := range h.agents.calls {
		if strings.Contains(call, "10.0.0.9") {
			t.Errorf("the replacement pod was sent %q", call)
		}
	}
}

// Another engine answers at the recorded address (it was given to another
// pod): it is not the master being replaced, and is left alone.
func TestFailover_DoesNotFenceAnotherEngineAtTheOldAddress(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.status["10.0.0.1"] = masterRun(5, "stream-x", "run-other")
	h.assertFailoverTo("c1-1")
	h.assertNoPause()
}

// Nothing answers at the old address any more: nothing to hold, and no
// reason to wait.
func TestFailover_GoesOnWhenTheOldMasterIsGone(t *testing.T) {
	h := newFenceHarness(t)
	delete(h.agents.status, "10.0.0.1")
	h.assertFailoverTo("c1-1")
	h.assertNoPause()
}

// An address nothing answers on (a dead node) holds the failover up for
// fenceTimeout at most.
func TestFailover_WaitsAtMostFenceTimeoutForAHungOldMaster(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.hang = map[string]bool{"10.0.0.1": true}
	start := time.Now()
	h.assertFailoverTo("c1-1")
	if took := time.Since(start); took < fenceTimeout || took >= time.Second {
		t.Errorf("the failover took %s, want at least fenceTimeout (%s) and well under a second", took, fenceTimeout)
	}
}

// With no address or engine run recorded -- a kividbclusters CRD from
// before podIP drops the field -- the failover goes on unfenced, and says
// so.
func TestFailover_UnfencedWithoutARecordedAddress(t *testing.T) {
	for name, entry := range map[string]kividbv1alpha1.KividbPodStatus{
		"no address":    {Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Synced: true, ReplicationOffset: 100, RunID: "run-0"},
		"no engine run": {Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Synced: true, ReplicationOffset: 100, PodIP: "10.0.0.1"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newFenceHarness(t)
			h.cluster.Status.Pods[0] = entry
			rec := record.NewFakeRecorder(8)
			h.r.Recorder = rec
			h.assertFailoverTo("c1-1")
			h.assertNoPause()
			found := false
			for len(rec.Events) > 0 {
				if e := <-rec.Events; strings.Contains(e, "FailoverUnfenced") && strings.Contains(e, "kividbclusters CRD") {
					found = true
				}
			}
			if !found {
				t.Error("no FailoverUnfenced event naming the CRD")
			}
		})
	}
}

// The promotion failed: the old master is still the master, and its
// writes must not stay held.
func TestFailover_LiftsTheFenceWhenThePromotionFails(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.failPromote = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		t.Fatal("reconcileRoles succeeded, want the promotion's error")
	}
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 60000 WRITE", "promote 10.0.0.2", "exec 10.0.0.1 CLIENT UNPAUSE")
	if h.agents.paused["10.0.0.1"] {
		t.Error("the old master's writes are still held")
	}
}

// An engine before kividb 1.1 has no CLIENT PAUSE ... WRITE: the failover
// goes on without the fence.
func TestFailover_OldEngineWithoutPauseDoesNotBlock(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.noPause = true
	h.assertFailoverTo("c1-1")
	if h.agents.paused["10.0.0.1"] {
		t.Error("an engine without CLIENT PAUSE WRITE recorded as paused")
	}
}

// A master failed away from while hung comes back still paused, and still
// believing it is the master: it follows the new master, and only then are
// its clients disconnected and the pause lifted -- the writes it held fail
// on the dropped connections instead of landing on a replica.
func TestFailover_OldMasterBackWhilePausedFollowsAndIsReleased(t *testing.T) {
	h := newFenceHarness(t, testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: time.Minute, agent: masterRun(100, "stream-a", "run-0")})
	if m, failover := h.reconcileAndPersist(); m != "c1-1" || !failover {
		t.Fatalf("pass 1: master=%q failover=%v, want a failover to c1-1", m, failover)
	}
	if !h.agents.paused["10.0.0.1"] {
		t.Fatal("pass 1: the old master's writes are not held")
	}

	h.setReady("c1-0", true)
	if m, failover := h.reconcileAndPersist(); m != "c1-1" || failover {
		t.Fatalf("pass 2: master=%q failover=%v, want c1-1 and no failover", m, failover)
	}
	h.assertCallOrder("replicaof 10.0.0.1 -> 10.0.0.2", "exec 10.0.0.1 CLIENT KILL TYPE normal", "exec 10.0.0.1 CLIENT UNPAUSE")
	if h.agents.paused["10.0.0.1"] {
		t.Error("the old master's writes are still held")
	}
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
}
