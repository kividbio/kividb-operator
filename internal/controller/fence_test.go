package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	h.withClusterInClient() // the fence is recorded in the status before the promotion
	return h
}

// addPod creates p, as the StatefulSet does when it replaces a pod.
func (h *rolesHarness) addPod(p testPod) {
	h.t.Helper()
	if err := h.r.Create(context.Background(), p.object(h.cluster)); err != nil {
		h.t.Fatal(err)
	}
	if p.agent != nil {
		h.agents.status[p.ip] = p.agent
	}
}

// storedFence is status.fencedMaster as the API server has it.
func (h *rolesHarness) storedFence() *kividbv1alpha1.FencedMaster {
	h.t.Helper()
	var c kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &c); err != nil {
		h.t.Fatal(err)
	}
	return c.Status.FencedMaster
}

// recordEvents makes the reconciler's events readable with eventsWith.
func (h *rolesHarness) recordEvents() *record.FakeRecorder {
	rec := record.NewFakeRecorder(64)
	h.r.Recorder = rec
	return rec
}

// eventsWith drains rec and returns the events that mention s.
func eventsWith(rec *record.FakeRecorder, s string) []string {
	var out []string
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, s) {
			out = append(out, e)
		}
	}
	return out
}

// assertNoCall checks that no agent call starts with prefix.
func (h *rolesHarness) assertNoCall(prefix string) {
	h.t.Helper()
	for _, call := range h.agents.calls {
		if strings.HasPrefix(call, prefix) {
			h.t.Errorf("unexpected %q", call)
		}
	}
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
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}

// Once the replica is promoted, the old master that answered the fence
// follows it at once, as in a switchover: it can take no write when its
// pause ends, its clients are sent to the new master, and a force-deleted
// one's preStop hook lets it stop.
func TestFailover_MakesTheFencedMasterAReplicaAtOnce(t *testing.T) {
	h := newFenceHarness(t)
	h.assertFailoverTo("c1-1")
	h.assertCallOrder(
		"exec 10.0.0.1 CLIENT PAUSE 65000 WRITE",
		"promote 10.0.0.2",
		"replicaof 10.0.0.1 -> 10.0.0.2",
		"exec 10.0.0.1 CLIENT KILL TYPE normal",
		"exec 10.0.0.1 CLIENT UNPAUSE",
	)
	if h.agents.paused["10.0.0.1"] {
		t.Error("the old master's writes are still held")
	}
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
}

// A write the old master acknowledged just before the pause may not have
// reached the replica yet: the promotion waits for it to, within the
// fence's budget.
func TestFailover_WaitsForTheCandidateToCatchUpWithTheFencedMaster(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": 2}
	h.assertFailoverTo("c1-1")
	if n := h.agents.catchUp["10.0.0.2"]; n != 0 {
		t.Errorf("c1-1 was promoted %d ROLE answers before it had caught up with the old master", n)
	}
}

// The wait for the replica is not cut short by the fence's own timeout:
// the old master is paused, so waiting costs availability, not writes.
func TestFailover_KeepsWaitingForTheCatchUpPastFenceTimeout(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": int(2 * fenceTimeout / switchoverPoll)}
	h.assertFailoverTo("c1-1")
	if n := h.agents.catchUp["10.0.0.2"]; n != 0 {
		t.Errorf("c1-1 was promoted %d ROLE answers before it had caught up with the old master", n)
	}
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// A replica that has not caught up when the wait ends is promoted all the
// same, and the operator says so.
func TestFailover_WarnsWhenItPromotesAReplicaThatHasNotCaughtUp(t *testing.T) {
	shortSwitchoverWaits(t)
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": -1}
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) != 1 || !strings.Contains(e[0], "caught up") {
		t.Errorf("events %v, want one FailoverUnfenced saying c1-1 had not caught up", e)
	}
}

// The fenced master stops answering while the replica catches up (its
// container stopped): it takes no more writes, so there is nothing more to
// wait for.
func TestFailover_StopsWaitingForTheCatchUpWhenTheOldMasterIsGone(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": -1}
	h.agents.goneAfterPause = map[string]bool{"10.0.0.1": true}
	start := time.Now()
	h.assertFailoverTo("c1-1")
	if took := time.Since(start); took >= switchoverCatchUpTimeout/2 {
		t.Errorf("the failover took %s, want it to stop waiting once the old master was gone", took)
	}
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) != 1 || !strings.Contains(e[0], "caught up") {
		t.Errorf("events %v, want one FailoverUnfenced saying c1-1 had not caught up", e)
	}
}

// The engine answered with the recorded run, so it is there: a pause whose
// reply does not come back -- it may have landed -- is sent again rather
// than failing over unfenced.
func TestFailover_SendsAnUnansweredPauseAgain(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.pauseUnanswered = map[string]int{"10.0.0.1": 1}
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
	if h.storedFence() == nil {
		t.Error("no status.fencedMaster: the failover went on unfenced")
	}
}

// A pause never answered within fenceRetryTimeout: the failover goes on
// unfenced, and says so.
func TestFailover_UnfencedWhenThePauseIsNeverAnswered(t *testing.T) {
	defer func(d time.Duration) { fenceRetryTimeout = d }(fenceRetryTimeout)
	fenceRetryTimeout = 3 * fenceTimeout
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.pauseUnanswered = map[string]int{"10.0.0.1": 1000}
	start := time.Now()
	h.assertFailoverTo("c1-1")
	if took := time.Since(start); took < fenceRetryTimeout {
		t.Errorf("the failover went on after %s, before fenceRetryTimeout (%s)", took, fenceRetryTimeout)
	}
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) != 1 {
		t.Errorf("events %v, want one FailoverUnfenced", e)
	}
}

// The fence is two engine commands, the run check and the pause: under a
// CPU limit every round trip to a loaded engine can take tens of
// milliseconds, and GET /status makes seven.
func TestFailover_FenceChecksTheRunWithOneCommand(t *testing.T) {
	h := newFenceHarness(t)
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 INFO server", "exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}

// The StatefulSet recreated the pod under the same name, with a new
// address: the engine to hold is the old one, at the old address.
func TestFailover_FencesTheOldAddressNotTheReplacementPod(t *testing.T) {
	h := newFenceHarness(t, testPod{name: "c1-0", ip: "10.0.0.9", uid: "new", unreadyFor: time.Minute, agent: masterRun(0, "stream-b", "run-new")})
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
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

// Nothing listens at the old address any more (connection refused):
// nothing to hold, and no reason to ask again.
func TestFailover_GoesOnWhenTheOldMasterIsGone(t *testing.T) {
	h := newFenceHarness(t)
	delete(h.agents.status, "10.0.0.1")
	start := time.Now()
	h.assertFailoverTo("c1-1")
	h.assertNoPause()
	if took := time.Since(start); took >= fenceTimeout {
		t.Errorf("the failover took %s: a refused connection is not asked again", took)
	}
}

// A slow engine, alive: its run check times out once, and is asked again.
func TestFailover_AsksASlowOldMasterAgain(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.infoUnanswered = map[string]int{"10.0.0.1": 1}
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 INFO server", "exec 10.0.0.1 INFO server", "exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// The promotion went through, but a step after it failed (the new
// master's label): the fenced master is made its replica all the same,
// rather than wait for a pass that may come after its pause.
func TestFailover_DemotesTheFencedMasterWhenAStepAfterThePromotionFails(t *testing.T) {
	h := newFenceHarness(t)
	h.r.Client = interceptor.NewClient(h.r.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetName() == "c1-1" && obj.GetLabels()[kividbv1alpha1.RoleLabel] == string(kividbv1alpha1.RoleMaster) {
				return errors.New("the API server is unavailable")
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		t.Fatal("reconcileRoles succeeded, want the label's error")
	}
	h.assertCallOrder("promote 10.0.0.2", "replicaof 10.0.0.1 -> 10.0.0.2", "exec 10.0.0.1 CLIENT KILL TYPE normal", "exec 10.0.0.1 CLIENT UNPAUSE")
	if h.cluster.Status.FencedMaster != nil {
		t.Errorf("status.fencedMaster = %+v, want it cleared", h.cluster.Status.FencedMaster)
	}
}

// The fenced master is only ever pointed at a master that is ready and
// says it is one.
func TestFailover_DemotesTheFencedMasterOnlyTowardAReadyMaster(t *testing.T) {
	h := newRolesHarness(t, "c1-1",
		testPod{name: "c1-1", ip: "10.0.0.2", label: "master", unreadyFor: 5 * time.Second, agent: masterRun(100, "stream-b", "run-1")},
	)
	h.cluster.Status.FencedMaster = &kividbv1alpha1.FencedMaster{Name: "c1-0", PodIP: "10.0.0.1", RunID: "run-0"}
	h.agents.status["10.0.0.1"] = masterRun(100, "stream-a", "run-0")
	h.reconcile()
	h.assertNoCall("replicaof 10.0.0.1")
	if h.cluster.Status.FencedMaster == nil {
		t.Error("status.fencedMaster cleared, want it kept until there is a ready master")
	}
}

// An address nothing answers on (a dead node) is asked again for
// fenceRunCheckTimeout -- a slow engine is there -- and then the failover
// goes on, and says it went on unfenced.
func TestFailover_WaitsAboutASecondForAHungOldMaster(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.hang = map[string]bool{"10.0.0.1": true}
	start := time.Now()
	h.assertFailoverTo("c1-1")
	if took := time.Since(start); took < fenceRunCheckTimeout || took >= fenceRunCheckTimeout+2*fenceTimeout+250*time.Millisecond {
		t.Errorf("the failover took %s, want about fenceRunCheckTimeout (%s)", took, fenceRunCheckTimeout)
	}
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) != 1 {
		t.Errorf("events %v, want one FailoverUnfenced", e)
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

// The promotion failed, or seemed to: it may have gone through all the
// same. The old master's writes stay held -- lifting the pause would let it
// take writes the replica, master after all, never sees -- and the record
// of it, written before the promotion, is there for the next pass.
func TestFailover_KeepsTheFenceWhenThePromotionFails(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.failPromote = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		t.Fatal("reconcileRoles succeeded, want the promotion's error")
	}
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
	h.assertNoCall("exec 10.0.0.1 CLIENT UNPAUSE")
	if !h.agents.paused["10.0.0.1"] {
		t.Error("the old master's writes are no longer held")
	}
	want := kividbv1alpha1.FencedMaster{Name: "c1-0", PodIP: "10.0.0.1", RunID: "run-0"}
	if got := h.storedFence(); got == nil || *got != want {
		t.Errorf("stored status.fencedMaster = %+v, want %+v", got, want)
	}
}

// The promotion went through but its reply was lost. The next pass
// finishes the failover with the old master's writes held all along.
func TestFailover_PromoteThatErrorsAfterTakingEffect(t *testing.T) {
	h := newFenceHarness(t)
	h.agents.failPromoteAfter = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		t.Fatal("pass 1: reconcileRoles succeeded, want the promotion's error")
	}
	h.agents.failPromoteAfter = nil
	rec := h.recordEvents()
	if m, _ := h.reconcileAndPersist(); m != "c1-1" {
		t.Fatalf("pass 2: master=%q, want c1-1", m)
	}
	h.assertCallOrder("promote 10.0.0.2", "replicaof 10.0.0.1 -> 10.0.0.2", "exec 10.0.0.1 CLIENT KILL TYPE normal", "exec 10.0.0.1 CLIENT UNPAUSE")
	for _, call := range h.agents.calls {
		if call == "replicaof 10.0.0.1 -> 10.0.0.2" {
			break
		}
		if call == "exec 10.0.0.1 CLIENT UNPAUSE" {
			t.Fatalf("the old master's pause was lifted before it was made a replica (calls %v)", h.agents.calls)
		}
	}
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	if h.cluster.Status.FencedMaster != nil {
		t.Errorf("status.fencedMaster = %+v, want it cleared", h.cluster.Status.FencedMaster)
	}
	// c1-1 caught up before pass 1 promoted it, and is a master since.
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// The force-deleted master could not be made a replica right after the
// promotion. Its pod object is gone and a replacement under the same name
// has another address, so only the record of the fence leads back to it:
// each pass holds its writes again and tries, until it is done.
func TestFailover_RetriesDemotingAForceDeletedMaster(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.failReplicaOf = map[string]bool{"10.0.0.1": true}
	h.assertFailoverTo("c1-1")
	if h.cluster.Status.FencedMaster == nil {
		t.Fatal("pass 1: no status.fencedMaster")
	}

	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "replica", ready: true, uid: "new", agent: masterRun(0, "stream-b", "run-new")})
	h.agents.calls = nil
	h.reconcileAndPersist()
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "replicaof 10.0.0.1 -> 10.0.0.2")
	if !h.agents.paused["10.0.0.1"] || h.cluster.Status.FencedMaster == nil {
		t.Fatalf("pass 2: paused=%v fencedMaster=%+v, want the writes still held and the record kept", h.agents.paused["10.0.0.1"], h.cluster.Status.FencedMaster)
	}
	if got := h.podStatus("c1-0").PodIP; got != "10.0.0.9" {
		t.Errorf("c1-0's entry records %q, want the replacement's 10.0.0.9", got)
	}
	if e := eventsWith(rec, "FencedMasterNotDemoted"); len(e) != 1 {
		t.Errorf("events %v, want one FencedMasterNotDemoted over the two failed tries", e)
	}

	h.agents.failReplicaOf = nil
	h.agents.calls = nil
	h.reconcileAndPersist()
	h.assertCallOrder("replicaof 10.0.0.1 -> 10.0.0.2", "exec 10.0.0.1 CLIENT KILL TYPE normal", "exec 10.0.0.1 CLIENT UNPAUSE")
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	if h.agents.paused["10.0.0.1"] || h.cluster.Status.FencedMaster != nil {
		t.Errorf("pass 3: paused=%v fencedMaster=%+v, want it released and the record cleared", h.agents.paused["10.0.0.1"], h.cluster.Status.FencedMaster)
	}
	h.assertNoCall("exec 10.0.0.9 CLIENT")
}

// Once another engine answers at the fenced master's address, or nothing
// does on two passes in a row, there is nothing left to hold: the record
// goes. One unanswered check is not enough.
func TestFailover_ForgetsTheFencedMasterOnceItIsGone(t *testing.T) {
	for name, tc := range map[string]struct {
		at     *agentapi.StatusResponse
		passes int
	}{
		"another engine": {masterRun(5, "stream-x", "run-other"), 1},
		"nothing":        {nil, 2},
	} {
		t.Run(name, func(t *testing.T) {
			h := newRolesHarness(t, "c1-1",
				testPod{name: "c1-1", ip: "10.0.0.2", label: "master", ready: true, agent: masterRun(100, "stream-b", "run-1")},
			)
			h.cluster.Status.FencedMaster = &kividbv1alpha1.FencedMaster{Name: "c1-0", PodIP: "10.0.0.1", RunID: "run-0"}
			if tc.at != nil {
				h.agents.status["10.0.0.1"] = tc.at
			}
			for pass := 1; pass < tc.passes; pass++ {
				h.reconcile()
				if h.cluster.Status.FencedMaster == nil {
					t.Fatalf("pass %d: status.fencedMaster cleared after one unanswered check", pass)
				}
			}
			h.reconcile()
			if h.cluster.Status.FencedMaster != nil {
				t.Errorf("status.fencedMaster = %+v, want it cleared", h.cluster.Status.FencedMaster)
			}
			h.assertNoCall("replicaof 10.0.0.1")
			h.assertNoPause()
		})
	}
}

// terminationGracePeriodSeconds: 0 would make the pause CLIENT PAUSE 0,
// which the engine takes as no pause at all. And the kubelet gives a
// preStop hook 2 s past the grace period: the pause outlasts both.
func TestFailover_PausesForAtLeastTenSecondsAndAMargin(t *testing.T) {
	h := newFenceHarness(t)
	zero := int64(0)
	h.cluster.Spec.TerminationGracePeriodSeconds = &zero
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 15000 WRITE", "promote 10.0.0.2")
}

// An engine before kividb 1.1 has no CLIENT PAUSE ... WRITE: the failover
// goes on without the fence.
func TestFailover_OldEngineWithoutPauseDoesNotBlock(t *testing.T) {
	h := newFenceHarness(t)
	rec := h.recordEvents()
	h.agents.noPause = true
	start := time.Now()
	h.assertFailoverTo("c1-1")
	if h.agents.paused["10.0.0.1"] {
		t.Error("an engine without CLIENT PAUSE WRITE recorded as paused")
	}
	if took := time.Since(start); took >= fenceTimeout {
		t.Errorf("the failover took %s: an engine that refuses the pause is not asked again", took)
	}
	if e := eventsWith(rec, "FailoverUnfenced"); len(e) != 1 {
		t.Errorf("events %v, want one FailoverUnfenced", e)
	}
}

// A master failed away from while hung comes back still paused, and still
// believing it is the master: it follows the new master, and only then are
// its clients disconnected and the pause lifted -- the writes it held fail
// on the dropped connections instead of landing on a replica.
func TestFailover_OldMasterBackWhilePausedFollowsAndIsReleased(t *testing.T) {
	h := newFenceHarness(t, testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: time.Minute, agent: masterRun(100, "stream-a", "run-0")})
	h.agents.failReplicaOf = map[string]bool{"10.0.0.1": true} // pointing it at the new master at once fails
	if m, failover := h.reconcileAndPersist(); m != "c1-1" || !failover {
		t.Fatalf("pass 1: master=%q failover=%v, want a failover to c1-1", m, failover)
	}
	if !h.agents.paused["10.0.0.1"] {
		t.Fatal("pass 1: the old master's writes are not held")
	}

	h.agents.failReplicaOf = nil
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

// A replica whose pod restarted reports role master (an engine starts as
// one) until it is pointed at the master. It is a new engine run, not a
// demoted master: its clients are left alone.
func TestReconcileRoles_RestartedReplicaKeepsItsClients(t *testing.T) {
	h := newRolesHarness(t, "c1-1",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "replica", ready: true, agent: masterRun(0, "stream-new", "run-new")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "master", ready: true, agent: masterRun(100, "stream-a", "run-1")},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleReplica, Synced: true, ReplicationOffset: 100, RunID: "run-old", PodIP: "10.0.0.1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleMaster, Synced: true, ReplicationOffset: 100, RunID: "run-1", PodIP: "10.0.0.2"},
	}
	h.reconcile()
	h.assertCallOrder("replicaof 10.0.0.1 -> 10.0.0.2")
	for _, call := range h.agents.calls {
		if strings.HasPrefix(call, "exec 10.0.0.1 CLIENT") {
			t.Errorf("the restarted replica was sent %q", call)
		}
	}
}

// While the fenced master is not yet a replica, the operator comes back
// well within its pause, which each try renews.
func TestReconcile_ComesBackSoonWhileTheFencedMasterIsHeld(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Status.FencedMaster = &kividbv1alpha1.FencedMaster{Name: "c1-9", PodIP: "10.0.0.9", RunID: "run-9"}
	}, nil)
	f.agents.status["10.0.0.9"] = masterRun(50, "stream-a", "run-9")
	f.agents.failReplicaOf = map[string]bool{"10.0.0.9": true}
	got := f.reconcile(t, fencedMasterRetry)
	if got.Status.FencedMaster == nil {
		t.Error("status.fencedMaster cleared while the fenced master is not a replica yet")
	}
	if fencedMasterRetry >= minFencePause {
		t.Errorf("fencedMasterRetry %s is not within the pause it renews (%s)", fencedMasterRetry, minFencePause)
	}
}

// The pass read the cluster before an overlapping pass's status write --
// the cache lags behind the API server during a failover. The fence is
// recorded all the same, and the failover goes on; the field is all it
// writes over the newer status.
func TestFailover_RecordsTheFenceFromAStaleRead(t *testing.T) {
	h := newFenceHarness(t)
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	stored.Status.Phase = kividbv1alpha1.PhaseDegraded
	if err := h.r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}

	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2", "replicaof 10.0.0.1 -> 10.0.0.2")
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	want := kividbv1alpha1.FencedMaster{Name: "c1-0", PodIP: "10.0.0.1", RunID: "run-0"}
	if got := stored.Status.FencedMaster; got == nil || *got != want {
		t.Errorf("stored status.fencedMaster = %+v, want %+v", got, want)
	}
	if stored.Status.Phase != kividbv1alpha1.PhaseDegraded {
		t.Errorf("stored status.phase = %q: the newer status was written over", stored.Status.Phase)
	}
}
