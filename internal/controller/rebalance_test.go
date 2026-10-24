package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlanMoves(t *testing.T) {
	owners := func(shards int) []int32 {
		out := make([]int32, agentapi.TotalSlots)
		for i, rg := range evenSlotRanges(shards) {
			for s := rg[0]; s <= rg[1]; s++ {
				out[s] = int32(i)
			}
		}
		return out
	}
	apply := func(o []int32, moves []kividbv1alpha1.SlotMove) map[int32]int {
		for _, m := range moves {
			rgs, err := moveRanges(&m)
			if err != nil {
				t.Fatal(err)
			}
			for _, rg := range rgs {
				for s := rg[0]; s <= rg[1]; s++ {
					if o[s] != m.FromShard {
						t.Fatalf("move %+v takes slot %d from shard %d", m, s, o[s])
					}
					o[s] = m.ToShard
				}
			}
		}
		counts := map[int32]int{}
		for _, s := range o {
			counts[s]++
		}
		return counts
	}
	for _, tc := range []struct {
		from, to int
		moved    int
	}{
		{3, 4, 4096}, // 3 shards give the new one exactly its share
		{4, 3, 4096}, // shard 3 gives everything away
		{1, 2, 8192},
		{2, 8, 16384 - 2*2048},
		{8, 1, 16384 - 2048},
		{3, 3, 0},
	} {
		o := owners(tc.from)
		moves := planMoves(o, int32(max(tc.from, tc.to)), int32(tc.to))
		total := 0
		for _, m := range moves {
			total += int(m.SlotCount)
		}
		if total != tc.moved {
			t.Errorf("%d -> %d shards: %d slots moved, want %d", tc.from, tc.to, total, tc.moved)
		}
		counts := apply(o, moves)
		for s := int32(0); s < int32(max(tc.from, tc.to)); s++ {
			if counts[s] != shareOf(s, int32(tc.to)) {
				t.Errorf("%d -> %d shards: shard %d has %d slots, want %d", tc.from, tc.to, s, counts[s], shareOf(s, int32(tc.to)))
			}
		}
		if again := planMoves(o, int32(max(tc.from, tc.to)), int32(tc.to)); len(again) != 0 {
			t.Errorf("%d -> %d shards: more moves after the plan: %v", tc.from, tc.to, again)
		}
	}
	if fmt.Sprint(slotRanges([]int{1, 2, 3, 7, 9, 10})) != "[1-3 7 9-10]" {
		t.Fatal(slotRanges([]int{1, 2, 3, 7, 9, 10}))
	}
}

// reshardPass is one sharded reconcile with a shard count that changes:
// the StatefulSet controller's part is to add the pods of new shards and
// take away those of removed ones.
func (h *formingHarness) reshardPass() {
	ctx := context.Background()
	h.r.startRebalance(h.c)
	present := map[string]bool{}
	for _, p := range h.pods() {
		present[p.Name] = true
		var s int32
		fmt.Sscanf(p.Labels[ShardLabel], "%d", &s)
		if s >= shardCount(h.c) {
			pod := p
			_ = h.r.Delete(ctx, &pod)
			id := h.engine.byIP[p.Status.PodIP]
			delete(h.engine.byIP, p.Status.PodIP)
			delete(h.engine.nodes, id)
		}
	}
	for s := int32(0); s < shardCount(h.c); s++ {
		for n := int32(0); n <= h.c.Spec.Replicas; n++ {
			if name := shardPodName(h.c, s, n); !present[name] {
				ip := fmt.Sprintf("10.0.%d.%d", s, n+1)
				if err := h.r.Create(ctx, shardedPod(h.c, s, n, ip)); err != nil {
					h.t.Fatal(err)
				}
				h.engine.add(nodeID(name), ip)
			}
		}
	}
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if _, err := h.r.reconcileClusterTopology(ctx, h.c, observed, h.exposed); err != nil {
		h.t.Log(err) // forgetting a removed node on its own replica, say
	}
	h.r.labelShardedRoles(ctx, observed)
	h.r.reconcileRebalance(ctx, h.c, observed)
	before := append([]kividbv1alpha1.ShardStatus(nil), h.c.Status.Shards...)
	h.c.Status.Phase, _, _ = shardedStatus(h.c, observed)
	h.r.noteShardFailovers(h.c, before, observed)
}

func (h *formingHarness) reshard(to int32, passes int) {
	h.c.Spec.Shards = int32p(to)
	for i := 0; i < passes && (i == 0 || rebalanceOf(h.c) != nil); i++ {
		h.reshardPass()
	}
	for i := 0; i < 4; i++ {
		h.reshardPass() // forgetting, statuses
	}
}

func (h *formingHarness) checkSpread(to int32) {
	h.t.Helper()
	if rb := rebalanceOf(h.c); rb != nil {
		h.t.Fatalf("still rebalancing: %+v", rb)
	}
	if h.c.Status.Cluster.Shards != to || h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		h.t.Fatalf("shards %d, phase %s", h.c.Status.Cluster.Shards, h.c.Status.Phase)
	}
	for s := int32(0); s < to; s++ {
		m := h.c.Status.Shards[s].MasterPod
		if got := slotCount(h.node(m).slots); got != shareOf(s, to) {
			h.t.Errorf("shard %d (%s) owns %d slots, want %d", s, m, got, shareOf(s, to))
		}
		for n := int32(0); n <= h.c.Spec.Replicas; n++ {
			if p := shardPodName(h.c, s, n); p != m && h.node(p).master != nodeID(m) {
				h.t.Errorf("%s follows %q, not its shard's master %s", p, h.node(p).master, m)
			}
		}
	}
	if len(h.c.Status.Shards) != int(to) {
		h.t.Errorf("%d shards in the status", len(h.c.Status.Shards))
	}
}

func TestAddAShard(t *testing.T) {
	h := newFormingHarness(t, 3, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	if h.c.Status.Cluster.Shards != 3 {
		t.Fatalf("formed shards %d", h.c.Status.Cluster.Shards)
	}
	h.reshard(4, 20)
	h.checkSpread(4)
	if h.engine.moved != 4096 {
		t.Fatalf("%d slots moved, want 4096", h.engine.moved)
	}
}

func TestRemoveAShard(t *testing.T) {
	h := newFormingHarness(t, 3, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	removed := []string{nodeID("s-2-0"), nodeID("s-2-1")}
	h.reshard(2, 20)
	h.checkSpread(2)
	if h.engine.moved != 5461 {
		t.Fatalf("%d slots moved, want 5461", h.engine.moved)
	}
	for _, p := range h.pods() {
		if p.Labels[ShardLabel] == "2" {
			t.Fatalf("pod %s of the removed shard is still there", p.Name)
		}
	}
	// Every remaining node has forgotten the removed ones.
	for _, n := range h.engine.nodes {
		for _, id := range removed {
			if _, knows := n.seen[id]; knows {
				t.Errorf("%s still knows removed node %s", n.id[:6], id[:6])
			}
		}
	}
	if len(h.c.Status.Cluster.StaleNodeIDs) != 0 {
		t.Errorf("stale IDs left: %v", h.c.Status.Cluster.StaleNodeIDs)
	}
}

func TestFailedMoveIsRetried(t *testing.T) {
	h := newFormingHarness(t, 2, 0)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	h.engine.failImport = true
	h.c.Spec.Shards = int32p(3)
	for i := 0; i < 6; i++ {
		h.reshardPass()
	}
	rb := rebalanceOf(h.c)
	if rb == nil || rb.LastError == "" || rb.Current != nil || h.c.Status.Phase != kividbv1alpha1.PhaseRebalancing {
		t.Fatalf("after a failed move: %+v, phase %s", rb, h.c.Status.Phase)
	}
	if h.engine.moved != 0 {
		t.Fatal("a move was retried before rebalanceRetry")
	}
	past := metav1.NewTime(time.Now().Add(-rebalanceRetry - time.Second))
	rb.LastErrorTime = &past
	h.reshard(3, 20)
	h.checkSpread(3)
}

func TestRetargetHalfWay(t *testing.T) {
	c := shardedCluster("s", 4, 0)
	c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{Shards: 2}
	r := shardedReconciler(t)
	r.startRebalance(c)
	if rb := rebalanceOf(c); rb == nil || rb.From != 2 || rb.To != 4 || shardCount(c) != 4 {
		t.Fatalf("%+v", rb)
	}
	c.Spec.Shards = int32p(3)
	r.startRebalance(c)
	// Shard 3 may own slots by now: it stays until it has given them away.
	if rb := rebalanceOf(c); rb.From != 4 || rb.To != 3 || shardCount(c) != 4 || targetShards(c) != 3 {
		t.Fatalf("%+v, %d shards", rb, shardCount(c))
	}
}

// A new shard's -1 pod can come up before its -0: it is not taken for the
// shard's master in the meantime (and then replaced, which looked like a
// failover).
func TestNewShardMasterIsItsFirstPod(t *testing.T) {
	h := newFormingHarness(t, 2, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	h.c.Spec.Shards = int32p(3)
	h.r.startRebalance(h.c)
	ctx := context.Background()
	if err := h.r.Create(ctx, shardedPod(h.c, 2, 1, "10.0.2.2")); err != nil {
		t.Fatal(err)
	}
	h.engine.add(nodeID("s-2-1"), "10.0.2.2")
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if m := buildView(h.c, observed).masters[2]; m != nil {
		t.Fatalf("shard 2's master is %s before its -0 is up", m.pod.Name)
	}
	if err := h.r.Create(ctx, shardedPod(h.c, 2, 0, "10.0.2.1")); err != nil {
		t.Fatal(err)
	}
	h.engine.add(nodeID("s-2-0"), "10.0.2.1")
	observed = h.r.observeShardedPods(ctx, h.c, h.pods())
	if m := buildView(h.c, observed).masters[2]; m == nil || m.pod.Name != "s-2-0" {
		t.Fatalf("shard 2's master: %v", m)
	}
	h.reshard(3, 20)
	h.checkSpread(3)
	if h.c.Status.LastFailoverTime != nil || strings.Contains(events(h.r), "ShardFailover") {
		t.Fatal("adding a shard was recorded as a failover")
	}
}
