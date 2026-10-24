package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Changing a sharded cluster's shard count. The shards' StatefulSets
// exist for the larger of the old and new counts while the change runs.
// Slots move one move at a time, each a CLUSTER MIGRATION IMPORT on the
// destination master: atomic, clients see no ASK, and the source keeps
// serving the slots until the instant they change hands. Each shard over
// its share gives its highest slots to the shards under theirs, so only
// what has to move moves. A shard being removed gives all of its slots
// away; then its StatefulSet is deleted and its nodes forgotten.

// rebalanceRetry is how long a failed move waits before it is tried again.
const rebalanceRetry = 30 * time.Second

// maxPendingShown caps status.cluster.rebalance.pending.
const maxPendingShown = 16

// targetShards is spec.shards: the count the slots are to be spread over.
func targetShards(c *kividbv1alpha1.KividbCluster) int32 {
	if c.Spec.Shards == nil {
		return 0
	}
	return *c.Spec.Shards
}

func rebalanceOf(c *kividbv1alpha1.KividbCluster) *kividbv1alpha1.RebalanceStatus {
	if c.Status.Cluster == nil {
		return nil
	}
	return c.Status.Cluster.Rebalance
}

// startRebalance notes a change of spec.shards, from the count the slots
// are spread over, before the workloads are reconciled: the new shards'
// StatefulSets are made, and the removed ones' kept, from here on.
func (r *KividbClusterReconciler) startRebalance(c *kividbv1alpha1.KividbCluster) {
	cl := c.Status.Cluster
	to := targetShards(c)
	if cl == nil || cl.Shards == 0 {
		return // not formed yet: formed straight at spec.shards
	}
	rb := cl.Rebalance
	switch {
	case rb == nil && cl.Shards != to:
		now := metav1.Now()
		cl.Rebalance = &kividbv1alpha1.RebalanceStatus{From: cl.Shards, To: to, Phase: kividbv1alpha1.RebalanceAddingShards, StartTime: &now}
		r.event(c, corev1.EventTypeNormal, "RebalanceStarted", "changing from %d to %d shards", cl.Shards, to)
	case rb != nil && rb.To != to:
		// Changed again half way: every shard of either count may hold
		// slots now.
		rb.From = max(rb.From, rb.To)
		rb.To = to
		r.event(c, corev1.EventTypeNormal, "RebalanceRetargeted", "now changing to %d shards", to)
	}
}

// slotOwners maps each slot to the shard whose master owns it; -1 for a
// slot no known pod owns. Each master's own word on its slots comes first:
// the anchor learns of a finished move by gossip, a moment later, and a
// plan made from its view alone would move slots the destination already
// has. The anchor's view fills in for a master that did not answer.
func slotOwners(v clusterView) []int32 {
	owners := make([]int32, agentapi.TotalSlots)
	for i := range owners {
		owners[i] = -1
	}
	assign := func(shard int32, ranges []string) {
		for _, rg := range ranges {
			lo, hi, ok := agentapi.ParseSlotRange(rg)
			if !ok {
				continue
			}
			for s := lo; s <= hi; s++ {
				owners[s] = shard
			}
		}
	}
	for _, n := range v.anchor.status.Nodes {
		if p, ok := v.byID[n.ID]; ok && n.IsMaster() {
			assign(p.shard, n.Slots)
		}
	}
	for _, p := range v.byID {
		if me, ok := p.myself(); ok && me.IsMaster() {
			assign(p.shard, me.Slots)
		}
	}
	return owners
}

// shareOf is how many slots shard s owns once spread over `to` shards.
func shareOf(s, to int32) int {
	if s >= to {
		return 0
	}
	share := agentapi.TotalSlots / int(to)
	if int(s) < agentapi.TotalSlots%int(to) {
		share++
	}
	return share
}

// planMoves is the moves that spread the slots over `to` shards: each
// shard over its share gives its highest slots, in turn, to the shards
// under theirs, lowest shard first. One move per pair of shards.
func planMoves(owners []int32, shards, to int32) []kividbv1alpha1.SlotMove {
	bySlot := make([][]int, shards)
	for slot, s := range owners {
		if s >= 0 && s < shards {
			bySlot[s] = append(bySlot[s], slot)
		}
	}
	var give []struct {
		from int32
		slot int
	}
	need := map[int32]int{}
	for s := int32(0); s < shards; s++ {
		have, share := len(bySlot[s]), shareOf(s, to)
		for i := have - 1; i >= share; i-- {
			give = append(give, struct {
				from int32
				slot int
			}{s, bySlot[s][i]})
		}
		if have < share {
			need[s] = share - have
		}
	}
	// Slots given, in order, to the shards in need, lowest first.
	sort.Slice(give, func(i, j int) bool { return give[i].slot < give[j].slot })
	type pair struct{ from, to int32 }
	moved := map[pair][]int{}
	next := 0
	for s := int32(0); s < to; s++ {
		for ; need[s] > 0 && next < len(give); need[s]-- {
			g := give[next]
			moved[pair{g.from, s}] = append(moved[pair{g.from, s}], g.slot)
			next++
		}
	}
	var out []kividbv1alpha1.SlotMove
	for p, slots := range moved {
		sort.Ints(slots)
		out = append(out, kividbv1alpha1.SlotMove{FromShard: p.from, ToShard: p.to, Slots: slotRanges(slots), SlotCount: int32(len(slots))})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ToShard != out[j].ToShard {
			return out[i].ToShard < out[j].ToShard
		}
		return out[i].FromShard < out[j].FromShard
	})
	return out
}

// slotRanges writes sorted slots as ranges: "0-1364", "42".
func slotRanges(slots []int) []string {
	var out []string
	for i := 0; i < len(slots); {
		j := i
		for j+1 < len(slots) && slots[j+1] == slots[j]+1 {
			j++
		}
		if i == j {
			out = append(out, strconv.Itoa(slots[i]))
		} else {
			out = append(out, fmt.Sprintf("%d-%d", slots[i], slots[j]))
		}
		i = j + 1
	}
	return out
}

func moveRanges(m *kividbv1alpha1.SlotMove) ([][2]int, error) {
	out := make([][2]int, 0, len(m.Slots))
	for _, rg := range m.Slots {
		lo, hi, ok := agentapi.ParseSlotRange(rg)
		if !ok {
			return nil, fmt.Errorf("bad slot range %q", rg)
		}
		out = append(out, [2]int{lo, hi})
	}
	return out, nil
}

// reconcileRebalance takes the next step of a change of the shard count.
// It reports whether one is under way, so the caller looks again soon.
func (r *KividbClusterReconciler) reconcileRebalance(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	rb := rebalanceOf(c)
	if rb == nil {
		return false
	}
	log := logf.FromContext(ctx)
	v := buildView(c, pods)

	// A move under way: wait for it, whatever else is going on.
	if m := rb.Current; m != nil {
		dest := podByName(pods, m.DestPod)
		if dest == nil || dest.status == nil {
			return true // its pod is down: the task may resume, or be lost
		}
		task, found, err := r.Agent.ClusterMigrationStatus(ctx, dest.ip(), m.TaskID)
		switch {
		case err != nil:
			log.Error(err, "rebalance: reading the migration task", "pod", m.DestPod)
			return true
		case found && task.State == agentapi.MigrationInProgress:
			return true
		case found && task.State == agentapi.MigrationCompleted:
			r.event(c, corev1.EventTypeNormal, "SlotsMoved", "moved %d slots from shard %d to shard %d", m.SlotCount, m.FromShard, m.ToShard)
			rb.MovesDone++
			rb.LastError, rb.LastErrorTime = "", nil
		default:
			// Failed, canceled, or gone (the destination restarted; see
			// kividbio/kividb#108): the slots stayed with the source.
			why := "the task is gone from " + m.DestPod + " (it restarted)"
			if found {
				why = task.State + ": " + task.LastError
			}
			now := metav1.Now()
			rb.LastError, rb.LastErrorTime = fmt.Sprintf("moving %d slots from shard %d to %d: %s", m.SlotCount, m.FromShard, m.ToShard, why), &now
			r.event(c, corev1.EventTypeWarning, "SlotMoveFailed", "%s; retried in %s", rb.LastError, rebalanceRetry)
		}
		rb.Current = nil
	}

	// Everyone up, known, and the cluster serving.
	if !v.formed || v.anchor == nil {
		rb.Phase = kividbv1alpha1.RebalanceAddingShards
		return true
	}
	want := int(shardCount(c) * (c.Spec.Replicas + 1))
	up := 0
	for _, p := range pods {
		if p.ready && p.status != nil && p.pod.DeletionTimestamp == nil {
			if n, ok := v.anchor.status.Node(p.status.MyID); ok && !n.Has("handshake") {
				up++
			}
		}
	}
	if up < want || v.anchor.status.State != "ok" {
		rb.Phase = kividbv1alpha1.RebalanceAddingShards
		return true
	}
	// Every shard that is to own slots has a master to import them. (A
	// shard being removed may already have given all of its away.)
	for s := int32(0); s < rb.To; s++ {
		if v.masters[s] == nil {
			rb.Phase = kividbv1alpha1.RebalanceAddingShards
			return true // a new shard's pods are not sorted into master and replicas yet
		}
	}
	rb.Phase = kividbv1alpha1.RebalanceMovingSlots

	owners := slotOwners(v)
	for slot, s := range owners {
		if s < 0 {
			rb.LastError = fmt.Sprintf("slot %d has no master among the cluster's pods", slot)
			return true
		}
	}
	plan := planMoves(owners, shardCount(c), rb.To)
	rb.Pending = plan[:min(len(plan), maxPendingShown)]
	if len(plan) == 0 {
		return r.finishRebalance(ctx, c, pods)
	}
	if rb.LastErrorTime != nil && time.Since(rb.LastErrorTime.Time) < rebalanceRetry {
		return true
	}

	m := plan[0]
	dest := v.masters[m.ToShard]
	ranges, err := moveRanges(&m)
	if err != nil {
		log.Error(err, "rebalance: planned ranges")
		return true
	}
	id, err := r.Agent.ClusterMigrationImport(ctx, dest.ip(), ranges)
	if err != nil && strings.Contains(err.Error(), "already in progress") {
		// This move is under way already: started by a pass that read the
		// cluster before this one's status had landed. It ends with the
		// slots moved, and the next plan goes on from there.
		log.V(1).Info("rebalance: the move is already under way", "from", m.FromShard, "to", m.ToShard)
		return true
	}
	if err != nil {
		now := metav1.Now()
		rb.LastError, rb.LastErrorTime = fmt.Sprintf("starting to move %d slots from shard %d to %d: %v", m.SlotCount, m.FromShard, m.ToShard, err), &now
		r.event(c, corev1.EventTypeWarning, "SlotMoveFailed", "%s; retried in %s", rb.LastError, rebalanceRetry)
		return true
	}
	now := metav1.Now()
	m.DestPod, m.TaskID, m.StartTime = dest.pod.Name, id, &now
	rb.Current = &m
	rb.Pending = plan[1:min(len(plan), maxPendingShown+1)]
	r.event(c, corev1.EventTypeNormal, "MovingSlots", "moving %d slots (%s) from shard %d to shard %d", m.SlotCount, strings.Join(m.Slots, " "), m.FromShard, m.ToShard)
	return true
}

// finishRebalance ends the change once every slot is where it belongs:
// the shards past the new count, which own nothing now, go, and their
// nodes are forgotten (as stale IDs, until no node knows them).
func (r *KividbClusterReconciler) finishRebalance(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	rb := rebalanceOf(c)
	cl := c.Status.Cluster
	for s := rb.To; s < rb.From; s++ {
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: shardStatefulSetName(c, s), Namespace: c.Namespace}}
		if err := r.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "rebalance: deleting a removed shard's StatefulSet", "shard", s)
			return true
		}
		pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: shardStatefulSetName(c, s), Namespace: c.Namespace}}
		if err := r.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "rebalance: deleting a removed shard's disruption budget", "shard", s)
			return true
		}
		// Its volumes hold nothing of the cluster's now (every slot has
		// moved out), but they do hold the node's identity and its view of
		// the cluster: a shard added later would come back on them, as a
		// node the others forgot, knowing nodes that are gone, at addresses
		// that are gone. Kubernetes removes them once their pods are gone.
		if err := r.deleteShardVolumes(ctx, c, s); err != nil {
			logf.FromContext(ctx).Error(err, "rebalance: deleting a removed shard's volumes", "shard", s)
			return true
		}
	}
	stale := map[string]bool{}
	for _, id := range cl.StaleNodeIDs {
		stale[id] = true
	}
	for _, p := range pods {
		if p.shard >= rb.To && p.status != nil && !stale[p.status.MyID] {
			cl.StaleNodeIDs = append(cl.StaleNodeIDs, p.status.MyID)
			stale[p.status.MyID] = true
		}
	}
	sort.Strings(cl.StaleNodeIDs)
	if rb.From > rb.To {
		r.event(c, corev1.EventTypeNormal, "ShardsRemoved", "removed shards %d to %d, and their volumes", rb.To, rb.From-1)
	}
	r.event(c, corev1.EventTypeNormal, "RebalanceCompleted", "%d shards, %d slot moves", rb.To, rb.MovesDone)
	cl.Shards = rb.To
	cl.Rebalance = nil
	return true
}

// deleteShardVolumes deletes the PersistentVolumeClaims of shard s's pods
// (data-<cluster>-<shard>-<n>).
func (r *KividbClusterReconciler) deleteShardVolumes(ctx context.Context, c *kividbv1alpha1.KividbCluster, s int32) error {
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(c.Namespace)); err != nil {
		return err
	}
	prefix := "data-" + shardStatefulSetName(c, s) + "-"
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		n := strings.TrimPrefix(pvc.Name, prefix)
		if n == pvc.Name || n == "" || strings.Trim(n, "0123456789") != "" {
			continue
		}
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func podByName(pods []*shardPod, name string) *shardPod {
	for _, p := range pods {
		if p.pod.Name == name {
			return p
		}
	}
	return nil
}
