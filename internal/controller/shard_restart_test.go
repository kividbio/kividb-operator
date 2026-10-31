package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func int64p(n int64) *int64 { return &n }

// restartHarness is a formed sharded cluster whose every node holds keys
// and has been up for an hour, with a clock the test moves.
func restartHarness(t *testing.T, shards, replicas int32) (*formingHarness, *time.Time) {
	h := newFormingHarness(t, shards, replicas)
	now := time.Date(2026, 10, 31, 6, 28, 0, 0, time.UTC)
	h.r.clock = func() time.Time { return now }
	for _, n := range h.engine.nodes {
		n.keys, n.uptime, n.runID = 25939, int64p(3600), "run-"+n.id[:6]
	}
	for i := 0; i < 4; i++ {
		h.restartPass()
	}
	if h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		t.Fatalf("not formed: %s", h.c.Status.Phase)
	}
	_ = events(h.r)
	h.engine.calls = nil
	return h, &now
}

// restartPass is one sharded reconcile as reconcileSharded orders it, up to
// the status (the restarted-master check first).
func (h *formingHarness) restartPass() {
	ctx := context.Background()
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	h.r.reconcileRestartedMasters(ctx, h.c, observed)
	if _, err := h.r.reconcileClusterTopology(ctx, h.c, observed, h.exposed); err != nil {
		h.t.Fatal(err)
	}
	observed = h.r.observeShardedPods(ctx, h.c, h.pods())
	h.r.labelShardedRoles(ctx, observed)
	before := append([]kividbv1alpha1.ShardStatus(nil), h.c.Status.Shards...)
	h.c.Status.Phase, _, _ = shardedStatus(h.c, observed)
	h.r.noteShardFailovers(h.c, before, observed)
	if cond := h.r.restartedMasterCondition(h.c); cond != nil {
		setCondition(&h.c.Status.Conditions, *cond)
	} else {
		removeCondition(&h.c.Status.Conditions, MasterRestartedEmptyCondition)
	}
}

// notReadyYet makes pod a pod whose engine runs but which is not Ready: a
// restarted master before its readiness probe passes.
func (h *formingHarness) notReadyYet(name string) {
	var pod corev1.Pod
	ctx := context.Background()
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: name}, &pod); err != nil {
		h.t.Fatal(err)
	}
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: kividbContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			h.t.Fatal(err)
		}
	}
}

// restart is the AKS run's B2: the master's engine killed and back at once,
// with nothing on its volume, keeping its node ID and its slots.
func (h *formingHarness) restart(pod string, keys int64) {
	n := h.node(pod)
	n.keys, n.uptime, n.runID = keys, int64p(1), "run-new"
}

func hasCall(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func TestShardMasterRestartedEmptyFailsOverToItsReplica(t *testing.T) {
	h, _ := restartHarness(t, 3, 1)
	h.restart("s-2-0", 0)
	h.notReadyYet("s-2-0") // seen before it is Ready: before clients reach it by name

	h.restartPass()
	master, replica := h.node("s-2-0"), h.node("s-2-1")
	if !hasCall(h.engine.calls, "/exec "+master.id[:6]+"  CLIENT PAUSE") {
		t.Fatalf("the emptied master's writes were not held: %v", h.engine.calls)
	}
	if !hasCall(h.engine.calls, "/cluster/failover "+replica.id[:6]+"  force") {
		t.Fatalf("no forced failover on the replica: %v", h.engine.calls)
	}
	if len(replica.slots) == 0 || master.master != replica.id {
		t.Fatalf("the replica did not take over: replica slots %v, old master follows %q", replica.slots, master.master)
	}
	ev := events(h.r)
	if !strings.Contains(ev, "Warning MasterRestartedEmpty shard 2: master s-2-0 restarted with 0 keys, and its replica s-2-1 holds 25939") {
		t.Fatalf("events:\n%s", ev)
	}
	cond := findCondition(h.c.Status.Conditions, MasterRestartedEmptyCondition)
	if cond == nil || cond.Reason != "FailingOver" {
		t.Fatalf("condition %+v", cond)
	}

	// Next pass: over. The pause is lifted, and the condition says how it
	// ended for a while.
	h.restartPass()
	if master.paused {
		t.Fatal("the old master's writes are still held")
	}
	ev = events(h.r)
	if !strings.Contains(ev, "Normal MasterRestartedEmptyResolved shard 2: master s-2-0 restarted with 0 keys; s-2-1, which held 25939, took over") {
		t.Fatalf("events:\n%s", ev)
	}
	cond = findCondition(h.c.Status.Conditions, MasterRestartedEmptyCondition)
	if cond == nil || cond.Reason != "FailedOver" || !strings.Contains(cond.Message, "took over") {
		t.Fatalf("condition %+v", cond)
	}
	if h.c.Status.Shards[2].MasterPod != "s-2-1" {
		t.Fatalf("shard 2 master %s", h.c.Status.Shards[2].MasterPod)
	}

	// Nothing more is asked for.
	h.engine.calls = nil
	for i := 0; i < 3; i++ {
		h.restartPass()
	}
	if hasCall(h.engine.calls, "/cluster/failover") {
		t.Fatalf("asked again: %v", h.engine.calls)
	}
}

func TestShardMasterRestartedWithItsDataIsLeftAlone(t *testing.T) {
	h, _ := restartHarness(t, 3, 1)
	// The append-only file on: back with everything (or a few writes more
	// than the replica, which lags).
	h.restart("s-1-0", 25940)
	for i := 0; i < 3; i++ {
		h.restartPass()
	}
	if hasCall(h.engine.calls, "/cluster/failover") || hasCall(h.engine.calls, "/exec") {
		t.Fatalf("acted on a master that restarted with its data: %v", h.engine.calls)
	}
	if ev := events(h.r); strings.Contains(ev, "MasterRestarted") || strings.Contains(ev, "ShardDataLost") {
		t.Fatalf("events:\n%s", ev)
	}
	if findCondition(h.c.Status.Conditions, MasterRestartedEmptyCondition) != nil {
		t.Fatal("condition set")
	}
	// A master that has been up for long, holding less than its replica
	// (a FLUSHALL its replica has not applied yet): not a restart.
	h.node("s-0-0").keys = 0
	h.restartPass()
	if hasCall(h.engine.calls, "/cluster/failover") {
		t.Fatalf("acted on a master that did not restart: %v", h.engine.calls)
	}
	// Nor one that is still loading its data.
	h.restart("s-0-0", 10)
	h.node("s-0-0").loading = true
	h.restartPass()
	if hasCall(h.engine.calls, "/cluster/failover") {
		t.Fatalf("acted on a master still loading: %v", h.engine.calls)
	}
}

func TestRestartedMasterTakeoverFallsBackToTakeover(t *testing.T) {
	h, now := restartHarness(t, 3, 1)
	h.engine.refuseForced = true // the election is not won
	h.restart("s-0-0", 0)
	h.restartPass()
	h.restartPass() // too soon to ask again
	if n := strings.Count(strings.Join(h.engine.calls, "\n"), "/cluster/failover"); n != 1 {
		t.Fatalf("%d requests: %v", n, h.engine.calls)
	}
	*now = now.Add(restartedMasterRetry)
	h.restartPass()
	*now = now.Add(restartedMasterRetry)
	h.restartPass()
	calls := strings.Join(h.engine.calls, "\n")
	if strings.Count(calls, "force") != 2 || !strings.Contains(calls, "takeover") {
		t.Fatalf("want two FORCE then TAKEOVER: %v", h.engine.calls)
	}
	if len(h.node("s-0-1").slots) == 0 {
		t.Fatal("the replica did not take over")
	}
}

// With one shard, the restarted master is the only master that could vote
// in an election: TAKEOVER at once.
func TestRestartedMasterOfTheOnlyShardIsTakenOver(t *testing.T) {
	h, _ := restartHarness(t, 1, 1)
	h.restart("s-0-0", 0)
	h.restartPass()
	if !hasCall(h.engine.calls, "/cluster/failover "+h.node("s-0-1").id[:6]+"  takeover") {
		t.Fatalf("calls %v", h.engine.calls)
	}
}

// Too late: the replica already resynced from the emptied master (or the
// shard has none). Nothing to fail over to, but it is said.
func TestShardDataLostIsReported(t *testing.T) {
	h, _ := restartHarness(t, 2, 1)
	h.restart("s-1-0", 987)
	h.node("s-1-1").keys = 987 // dropped its 25939 and loaded the master's 987
	h.restartPass()
	if hasCall(h.engine.calls, "/cluster/failover") {
		t.Fatalf("failed over to a replica holding no more: %v", h.engine.calls)
	}
	ev := events(h.r)
	if !strings.Contains(ev, "Warning ShardDataLost shard 1: master s-1-0 restarted with 987 keys; it last reported 25939") {
		t.Fatalf("events:\n%s", ev)
	}
	if cond := findCondition(h.c.Status.Conditions, MasterRestartedEmptyCondition); cond == nil || cond.Reason != "DataLost" {
		t.Fatalf("condition %+v", cond)
	}
	// Once: the next passes compare with what the restarted engine reported.
	for i := 0; i < 3; i++ {
		h.restartPass()
	}
	if ev := events(h.r); strings.Contains(ev, "ShardDataLost") {
		t.Fatalf("reported again:\n%s", ev)
	}
}

// The replica resynced from the emptied master before the takeover: the
// case ends as lost data, not as a failover.
func TestRestartedMasterRaceLostIsReported(t *testing.T) {
	h, _ := restartHarness(t, 2, 1)
	h.engine.refuseForced = true
	h.restart("s-1-0", 0)
	h.restartPass()
	h.node("s-1-1").keys = 3 // the guard let a resync through
	h.node("s-1-0").keys = 3
	h.restartPass()
	ev := events(h.r)
	if !strings.Contains(ev, "Warning MasterRestartedEmpty") || !strings.Contains(ev, "Warning ShardDataLost shard 1: master s-1-0 restarted with 0 keys; s-1-1, which held 25939, no longer holds more") {
		t.Fatalf("events:\n%s", ev)
	}
	if h.node("s-1-0").paused {
		t.Fatal("the pause was not lifted")
	}
	if cond := findCondition(h.c.Status.Conditions, MasterRestartedEmptyCondition); cond == nil || cond.Reason != "DataLost" {
		t.Fatalf("condition %+v", cond)
	}
}

// While slots move in or out of a shard, its master's key count runs ahead
// of its replicas': no comparison then.
func TestRestartedMasterCheckWaitsForSlotMoves(t *testing.T) {
	h, _ := restartHarness(t, 2, 1)
	h.c.Status.Cluster.Rebalance = &kividbv1alpha1.RebalanceStatus{From: 2, To: 2, Current: &kividbv1alpha1.SlotMove{FromShard: 0, ToShard: 1}}
	h.restart("s-0-0", 0)
	h.restartPass()
	if hasCall(h.engine.calls, "/cluster/failover") {
		t.Fatalf("acted during a slot move: %v", h.engine.calls)
	}
}
