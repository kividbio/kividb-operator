package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Forming and looking after a sharded cluster. The engine does the
// failing over; the operator introduces nodes to each other, hands out the
// slots once, puts each shard's replicas behind its master, forgets node
// IDs a pod has replaced, and reports what it sees.

// shardPod is one of a sharded cluster's pods, with what its agent says.
type shardPod struct {
	pod    *corev1.Pod
	shard  int32
	index  int32
	ready  bool
	status *agentapi.ClusterStatusResponse // nil: not ready, or the agent did not answer
}

func (p *shardPod) ip() string { return p.pod.Status.PodIP }

func (p *shardPod) myself() (agentapi.ClusterNode, bool) {
	if p.status == nil {
		return agentapi.ClusterNode{}, false
	}
	return p.status.Myself()
}

// observeShardedPods asks every ready pod's agent for its view of the
// cluster, in parallel.
func (r *KividbClusterReconciler) observeShardedPods(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod) []*shardPod {
	out := make([]*shardPod, 0, len(pods))
	for i := range pods {
		p := &pods[i]
		sp := &shardPod{pod: p, ready: isPodReady(p)}
		if s, err := strconv.Atoi(p.Labels[ShardLabel]); err == nil {
			sp.shard = int32(s)
		}
		if n, err := strconv.Atoi(p.Name[len(shardStatefulSetName(c, sp.shard))+1:]); err == nil {
			sp.index = int32(n)
		}
		out = append(out, sp)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, sp := range out {
		if !sp.ready || sp.ip() == "" {
			continue
		}
		wg.Add(1)
		go func(sp *shardPod) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if st, err := r.Agent.ClusterStatus(ctx, sp.ip()); err == nil && st.Enabled && st.MyID != "" {
				sp.status = st
			}
		}(sp)
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].pod.Name < out[j].pod.Name })
	return out
}

// evenSlotRanges splits the slots into one contiguous range per shard, the
// remainder going one each to the first shards.
func evenSlotRanges(shards int) [][2]int {
	out := make([][2]int, shards)
	base, extra := agentapi.TotalSlots/shards, agentapi.TotalSlots%shards
	next := 0
	for i := range out {
		n := base
		if i < extra {
			n++
		}
		out[i] = [2]int{next, next + n - 1}
		next += n
	}
	return out
}

// announceName is the name a node announces to clients: its pod's DNS
// name under its shard's headless Service.
func announceName(c *kividbv1alpha1.KividbCluster, p *shardPod) string {
	return fmt.Sprintf("%s.%s.%s.svc", p.pod.Name, shardHeadlessServiceName(c, p.shard), c.Namespace)
}

// clusterView is what the operator works from: the cluster as one node
// (the anchor) sees it, and which pod is which node.
type clusterView struct {
	anchor  *shardPod
	byID    map[string]*shardPod // node ID -> pod, from each pod's own MYID
	formed  bool                 // some node owns slots
	masters map[int32]*shardPod  // shard -> the pod that owns its slots
}

// buildView picks the anchor: a reachable pod that is a master owning
// slots (it knows the cluster best), else <cluster>-0-0, else any.
func buildView(c *kividbv1alpha1.KividbCluster, pods []*shardPod) clusterView {
	v := clusterView{byID: map[string]*shardPod{}, masters: map[int32]*shardPod{}}
	for _, p := range pods {
		if p.status == nil {
			continue
		}
		v.byID[p.status.MyID] = p
		if me, ok := p.myself(); ok && me.IsMaster() && me.SlotCount() > 0 {
			v.formed = true
			if cur, taken := v.masters[p.shard]; !taken || p.pod.Name < cur.pod.Name {
				v.masters[p.shard] = p
			}
			if v.anchor == nil {
				v.anchor = p
			}
		}
	}
	if v.anchor == nil {
		for _, p := range pods {
			if p.status != nil && p.shard == 0 && p.index == 0 {
				v.anchor = p
			}
		}
	}
	if v.anchor == nil {
		for _, p := range pods {
			if p.status != nil {
				v.anchor = p
				break
			}
		}
	}
	return v
}

// reconcileClusterTopology takes the next steps towards a formed, healthy
// cluster. It reports whether it did anything (so the caller looks again
// soon). Errors from single steps are logged and the rest carry on.
func (r *KividbClusterReconciler) reconcileClusterTopology(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod, exposed map[string]exposure) (acted bool, err error) {
	log := logf.FromContext(ctx)
	var errs []error
	do := func(what string, f func() error) {
		acted = true
		if e := f(); e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", what, e))
		}
	}
	port := int(getPort(c))

	// 1. Every node announces its stable DNS name to clients, or the
	// address of the Service a user exposed it through.
	for _, p := range pods {
		if p.status == nil {
			continue
		}
		if want := announceTarget(c, p, exposed); p.status.Hostname != want {
			do("announce "+p.pod.Name, func() error { return r.Agent.ClusterAnnounce(ctx, p.ip(), want) })
		}
	}

	v := buildView(c, pods)
	if v.anchor == nil {
		return acted, errors.Join(errs...)
	}

	// 2. Introduce every node the anchor does not know, or knows at an old
	// address (a pod that came back with a new IP; after a cold restart,
	// all of them).
	for _, p := range pods {
		if p.status == nil || p == v.anchor {
			continue
		}
		seen, known := v.anchor.status.Node(p.status.MyID)
		if known && seen.IP == p.ip() && !seen.Has("noaddr") && !seen.Failed() {
			continue
		}
		if known && seen.Has("handshake") {
			continue
		}
		do("meet "+p.pod.Name, func() error { return r.Agent.ClusterMeet(ctx, v.anchor.ip(), p.ip(), port) })
	}

	// 3. Hand out the slots, once: when no node owns any, every pod is up,
	// and the anchor knows them all.
	want := int(shardCount(c) * (c.Spec.Replicas + 1))
	if !v.formed {
		reachable := 0
		allKnown := true
		for _, p := range pods {
			if p.status == nil {
				continue
			}
			reachable++
			if n, ok := v.anchor.status.Node(p.status.MyID); !ok || n.Has("handshake") {
				allKnown = false
			}
		}
		if reachable == want && allKnown {
			ranges := evenSlotRanges(int(shardCount(c)))
			for _, p := range pods {
				if p.status == nil || p.index != 0 || int(p.shard) >= len(ranges) {
					continue
				}
				rg := ranges[p.shard]
				do(fmt.Sprintf("slots %d-%d to %s", rg[0], rg[1], p.pod.Name), func() error {
					return r.Agent.ClusterAddSlots(ctx, p.ip(), [][2]int{rg})
				})
			}
			r.event(c, corev1.EventTypeNormal, "ClusterForming", "assigned the %d slots across %d shards", agentapi.TotalSlots, shardCount(c))
		}
		return acted, errors.Join(errs...)
	}

	// 4. Each shard's other pods replicate its master: a pod that is a
	// master owning nothing (new, or its volume was replaced), or that
	// follows a node outside its shard.
	for _, p := range pods {
		master, ok := v.masters[p.shard]
		if p.status == nil || !ok || master == p {
			continue
		}
		me, _ := p.myself()
		masterID := master.status.MyID
		if me.MasterID == masterID {
			continue
		}
		if me.IsMaster() && me.SlotCount() > 0 {
			continue // a master in its own right: the engine's doing, not ours to undo
		}
		if me.MasterID != "" {
			if follows, ok := v.byID[me.MasterID]; ok && follows.shard == p.shard {
				continue // follows another node of its own shard
			}
		}
		if _, knows := p.status.Node(masterID); !knows {
			continue // not introduced yet: step 2 sees to it first
		}
		do("replicate "+p.pod.Name, func() error { return r.Agent.ClusterReplicate(ctx, p.ip(), masterID) })
	}

	// 5. A pod that came back with a new node ID (its volume was lost)
	// leaves its old ID behind, failed, for good. Note it, and forget it on
	// every node that knows it, pass after pass until none does: the nodes
	// gossip it to each other, and a node that learns it again would bring
	// it back. Never a node that owns slots.
	if c.Status.Cluster == nil {
		c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{}
	}
	stale := map[string]bool{}
	for _, id := range c.Status.Cluster.StaleNodeIDs {
		stale[id] = true
	}
	for _, ps := range c.Status.Pods {
		for _, p := range pods {
			if p.pod.Name != ps.Name || p.status == nil || ps.NodeID == "" || ps.NodeID == p.status.MyID {
				continue
			}
			if old, known := v.anchor.status.Node(ps.NodeID); known && old.SlotCount() > 0 {
				r.event(c, corev1.EventTypeWarning, "StaleNodeOwnsSlots",
					"pod %s rejoined as node %s; its old node %s still owns slots, so it is not forgotten", p.pod.Name, p.status.MyID, ps.NodeID)
				continue
			}
			if !stale[ps.NodeID] {
				log.Info("pod rejoined with a new node ID; forgetting the old one", "pod", p.pod.Name, "old", ps.NodeID, "new", p.status.MyID)
				stale[ps.NodeID] = true
			}
		}
	}
	// A pod that did not answer may still know an ID, and would gossip it
	// back: an ID leaves the list only when every pod answered and none
	// knows it.
	everyoneAnswered := true
	for _, q := range pods {
		if q.status == nil {
			everyoneAnswered = false
		}
	}
	var still []string
	for id := range stale {
		known := !everyoneAnswered
		for _, q := range pods {
			if q.status == nil || q.status.MyID == id {
				continue
			}
			if _, has := q.status.Node(id); !has {
				continue
			}
			known = true
			do("forget "+id+" on "+q.pod.Name, func() error { return r.Agent.ClusterForget(ctx, q.ip(), id) })
		}
		if known {
			still = append(still, id)
		}
	}
	sort.Strings(still)
	c.Status.Cluster.StaleNodeIDs = still

	return acted, errors.Join(errs...)
}

// roleOf is the replication role a pod's node reports.
func roleOf(p *shardPod) kividbv1alpha1.NodeRole {
	me, ok := p.myself()
	switch {
	case !ok:
		return kividbv1alpha1.RoleUnknown
	case me.IsMaster():
		return kividbv1alpha1.RoleMaster
	default:
		return kividbv1alpha1.RoleReplica
	}
}

// labelShardedRoles keeps kividb.io/role in step with the engine, so the
// replicas Service follows failovers.
func (r *KividbClusterReconciler) labelShardedRoles(ctx context.Context, pods []*shardPod) {
	for _, p := range pods {
		if role := roleOf(p); role != kividbv1alpha1.RoleUnknown {
			if err := r.setRoleLabel(ctx, p.pod, role); err != nil {
				logf.FromContext(ctx).Error(err, "labelling pod", "pod", p.pod.Name)
			}
		}
	}
}

// shardedStatus fills the status from the pods' views, and returns the
// phase and the Ready condition's reason and message.
func shardedStatus(c *kividbv1alpha1.KividbCluster, pods []*shardPod) (kividbv1alpha1.ClusterPhase, string, string) {
	v := buildView(c, pods)
	shards := make([]kividbv1alpha1.ShardStatus, shardCount(c))
	for i := range shards {
		shards[i].Index = int32(i)
	}

	c.Status.Pods = c.Status.Pods[:0]
	ready := 0
	for _, p := range pods {
		shard := p.shard
		st := kividbv1alpha1.KividbPodStatus{Name: p.pod.Name, Role: roleOf(p), Ready: p.ready, Shard: &shard}
		if p.status != nil {
			st.NodeID = p.status.MyID
			st.ReplicationOffset = p.status.ReplicationOffset
		}
		if p.ready {
			ready++
		}
		c.Status.Pods = append(c.Status.Pods, st)
		if int(shard) < len(shards) {
			shards[shard].Pods = append(shards[shard].Pods, p.pod.Name)
			if p.ready {
				shards[shard].ReadyPods++
			}
		}
	}
	for s, m := range v.masters {
		if int(s) >= len(shards) {
			continue
		}
		me, _ := m.myself()
		shards[s].MasterPod = m.pod.Name
		shards[s].MasterNodeID = me.ID
		shards[s].Slots = me.Slots
	}
	c.Status.Shards = shards
	c.Status.MasterPod = ""

	if v.anchor != nil {
		a := v.anchor.status
		var stale []string
		if c.Status.Cluster != nil {
			stale = c.Status.Cluster.StaleNodeIDs
		}
		c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{
			State: a.State, SlotsAssigned: int32(a.SlotsAssigned), SlotsFail: int32(a.SlotsFail),
			KnownNodes: int32(a.KnownNodes), CurrentEpoch: a.CurrentEpoch, StaleNodeIDs: stale,
		}
	}

	want := int(shardCount(c) * (c.Spec.Replicas + 1))
	switch {
	case !v.formed || c.Status.Cluster == nil || c.Status.Cluster.SlotsAssigned < agentapi.TotalSlots:
		if ready < want {
			return kividbv1alpha1.PhaseProvisioning, "PodsNotReady", fmt.Sprintf("%d of %d pods ready", ready, want)
		}
		return kividbv1alpha1.PhaseProvisioning, "ClusterForming", "forming the cluster: introducing nodes and assigning slots"
	case c.Status.Cluster.State != "ok":
		return kividbv1alpha1.PhaseDegraded, "ClusterStateFail", fmt.Sprintf("cluster_state is %s (%d slots failed)", c.Status.Cluster.State, c.Status.Cluster.SlotsFail)
	case ready < want:
		return kividbv1alpha1.PhaseDegraded, "PodsNotReady", fmt.Sprintf("serving all slots; %d of %d pods ready", ready, want)
	}
	for _, sh := range shards {
		if sh.MasterPod == "" {
			return kividbv1alpha1.PhaseDegraded, "ShardWithoutMaster", fmt.Sprintf("shard %d has no master owning slots", sh.Index)
		}
	}
	return kividbv1alpha1.PhaseRunning, "ClusterOK", fmt.Sprintf("%d shards, all %d slots served, %d pods ready", len(shards), agentapi.TotalSlots, ready)
}

// noteShardFailovers records a shard whose master moved to another pod.
func (r *KividbClusterReconciler) noteShardFailovers(c *kividbv1alpha1.KividbCluster, before []kividbv1alpha1.ShardStatus) {
	prev := map[int32]string{}
	for _, s := range before {
		prev[s.Index] = s.MasterPod
	}
	for _, s := range c.Status.Shards {
		if old := prev[s.Index]; old != "" && s.MasterPod != "" && old != s.MasterPod {
			now := metav1.Now()
			c.Status.LastFailoverTime = &now
			r.event(c, corev1.EventTypeWarning, "ShardFailover", "shard %d: master moved from %s to %s", s.Index, old, s.MasterPod)
		}
	}
}
