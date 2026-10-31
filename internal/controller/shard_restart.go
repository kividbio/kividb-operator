package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// A shard master that restarted with less than its replicas hold.
//
// The engine fails over in cluster mode, but only from a master that has
// been unreachable for the node timeout (5 s by default). A master killed
// without its shutdown save -- out of memory, a forced delete, a lost
// node's pod rescheduled -- is back in about a second, with whatever its
// volume held: with the append-only file off, often nothing. It keeps its
// node ID and its slots, so no failover happens, and its replicas are
// re-pointed at it. A replica refuses to full-sync from a master with no
// keys at all, and asks again every 10 s; once the emptied master has taken
// a single write, the next attempt goes through and the replica drops what
// it held -- the shard's data, gone on every copy, with the cluster Ready
// (the AKS run's B2: 25,939 acknowledged writes).
//
// Persistence (configmap.go) is what prevents this. This is the safety net
// for clusters that run without it, or whose files were lost: when a
// shard's master restarted recently and a replica of the shard holds more
// than twice as many keys, the operator
//   - holds the master's writes (CLIENT PAUSE WRITE): everything it takes
//     now is lost when it resyncs from the replica, and a master that stays
//     at no keys is one the replica's full-sync guard keeps refusing;
//   - has that replica take over with CLUSTER FAILOVER FORCE (no handshake
//     with the master, an election among the others), TAKEOVER after two
//     tries or when the shard's master is the only one that could vote;
//   - says so: a Warning Event and the MasterRestartedEmpty condition.
//
// It is a race the operator does not always win. It sees the master as
// soon as its agent answers -- before the pod is Ready, so usually before a
// client that goes through DNS can reach it -- and the takeover itself is
// an election, ~100 ms. But a client that kept the pod's address (the
// container restarted in place) writes to it at once, and the replica's
// first full-sync attempt, about a second after the restart, is then let
// through: its data is gone before any controller could look. That case is
// reported (ShardDataLost), not prevented.

const (
	// restartedMasterWindow is how long after its engine started a shard
	// master is checked against its replicas.
	restartedMasterWindow = 2 * time.Minute
	// restartedMasterRetry: a takeover not done by then is asked for again.
	restartedMasterRetry = 5 * time.Second
	// restartedMasterAttempts: how many times the operator asks.
	restartedMasterAttempts = 6
	// emptyMasterPause is how long each request holds the emptied
	// master's writes.
	emptyMasterPause = 15 * time.Second
	// restartedMasterShownFor is how long the condition reports a case after
	// it is over.
	restartedMasterShownFor = 10 * time.Minute

	// MasterRestartedEmptyCondition reports shard masters that restarted
	// with less than their replicas held: the takeover under way, or how it
	// ended.
	MasterRestartedEmptyCondition = "MasterRestartedEmpty"
)

// restartedMaster is one shard's case, in KividbClusterReconciler.restartedMasters.
type restartedMaster struct {
	shard                   int32
	master, replica         string
	masterIP                string // whose writes are held
	masterKeys, replicaKeys int64
	detected, last          time.Time
	attempts                int
	lastError               string
	// resolved is when it ended (zero while under way), outcome how.
	resolved time.Time
	outcome  string
	lost     bool // the replica no longer held more when it ended
}

// recentlyRestarted reports whether the engine st describes started less
// than restartedMasterWindow ago: by its uptime, or, from an agent that
// does not report one, by a run ID or a pod incarnation other than the one
// last recorded.
func recentlyRestarted(last kividbv1alpha1.KividbPodStatus, pod *corev1.Pod, st *agentapi.ClusterStatusResponse) bool {
	if st.UptimeSeconds != nil {
		return *st.UptimeSeconds < int64(restartedMasterWindow/time.Second)
	}
	if last.RunID != "" && st.RunID != "" {
		return last.RunID != st.RunID
	}
	now := incarnation(pod)
	return last.Incarnation != "" && now != "" && last.Incarnation != now
}

// shardMaster is the pod of shard that owns slots, as its own agent says
// (a pod not Ready yet included).
func shardMaster(pods []*shardPod, shard int32) *shardPod {
	var m *shardPod
	for _, p := range pods {
		st := p.seen()
		if p.shard != shard || st == nil {
			continue
		}
		if me, ok := st.Myself(); ok && me.IsMaster() && me.SlotCount() > 0 && (m == nil || p.pod.Name < m.pod.Name) {
			m = p
		}
	}
	return m
}

// restartedBehind looks at shard: if its master restarted recently and a
// replica of the shard holds more than twice its keys, it returns the two
// (the replica holding the most). lost is set instead when the master
// restarted with less than half of what it last reported and no replica
// holds more: the writes are gone.
func restartedBehind(c *kividbv1alpha1.KividbCluster, pods []*shardPod, shard int32) (m, best *shardPod, lost bool) {
	m = shardMaster(pods, shard)
	if m == nil {
		return nil, nil, false
	}
	// Slots moving out of or into the shard change its master's key count
	// faster than its replicas follow: no comparison while they do.
	if rb := rebalanceOf(c); rb != nil && rb.Current != nil && (rb.Current.FromShard == shard || rb.Current.ToShard == shard) {
		return nil, nil, false
	}
	st := m.seen()
	var last kividbv1alpha1.KividbPodStatus
	for _, ps := range c.Status.Pods {
		if ps.Name == m.pod.Name {
			last = ps
		}
	}
	if !st.KeyCountKnown || st.Loading || !recentlyRestarted(last, m.pod, st) {
		return nil, nil, false
	}
	for _, q := range pods {
		qs := q.seen()
		if q == m || q.shard != shard || qs == nil || !qs.KeyCountKnown || qs.Loading {
			continue
		}
		if me, ok := qs.Myself(); !ok || me.IsMaster() {
			continue
		}
		if qs.KeyCount > 0 && qs.KeyCount > 2*st.KeyCount && (best == nil || qs.KeyCount > best.seen().KeyCount) {
			best = q
		}
	}
	if best != nil {
		return m, best, false
	}
	// Nothing to take over from. Compared with what it last reported only
	// on the first look at the new engine (a new run ID): from then on, the
	// count recorded is the restarted engine's own, and a drop is writes.
	if last.RunID != "" && st.RunID != "" && last.RunID != st.RunID && last.Keys > 0 && st.KeyCount*2 < last.Keys {
		return m, nil, true
	}
	return nil, nil, false
}

// reconcileRestartedMasters acts on the shards whose master restarted with
// less than a replica holds (see above). It reports whether a takeover is
// under way, so the caller looks again soon.
func (r *KividbClusterReconciler) reconcileRestartedMasters(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	log := logf.FromContext(ctx)
	now := r.now()
	acted := false
	for s := int32(0); s < shardCount(c); s++ {
		key := handoverKey(c, s)
		var rm *restartedMaster
		if v, ok := r.restartedMasters.Load(key); ok {
			rm = v.(*restartedMaster)
			if !rm.resolved.IsZero() && now.Sub(rm.resolved) > restartedMasterShownFor {
				r.restartedMasters.Delete(key)
				rm = nil
			}
		}
		m, q, lost := restartedBehind(c, pods, s)
		if lost && (rm == nil || !rm.resolved.IsZero()) {
			mk := m.seen().KeyCount
			had := int64(0)
			for _, ps := range c.Status.Pods {
				if ps.Name == m.pod.Name {
					had = ps.Keys
				}
			}
			log.Info("a shard master restarted with less than it had, and no replica holds more", "shard", s, "pod", m.pod.Name, "keys", mk, "had", had)
			r.event(c, corev1.EventTypeWarning, "ShardDataLost",
				"shard %d: master %s restarted with %d keys; it last reported %d, and no replica of the shard holds more. The writes it lost are gone unless restored from a backup. %s",
				s, m.pod.Name, mk, had, persistenceHint)
			r.restartedMasters.Store(key, &restartedMaster{shard: s, master: m.pod.Name, masterKeys: mk, detected: now, resolved: now, lost: true,
				outcome: fmt.Sprintf("master %s restarted with %d keys, having reported %d; no replica held more: the writes are lost", m.pod.Name, mk, had)})
			continue
		}
		if q == nil {
			if rm != nil && rm.resolved.IsZero() {
				r.endRestartedMaster(ctx, c, pods, rm)
			}
			continue
		}
		mk, qk := m.seen().KeyCount, q.seen().KeyCount
		if rm == nil || !rm.resolved.IsZero() || rm.master != m.pod.Name {
			rm = &restartedMaster{shard: s, master: m.pod.Name, replica: q.pod.Name, masterKeys: mk, replicaKeys: qk, detected: now}
			r.restartedMasters.Store(key, rm)
			log.Info("a shard master restarted with less than its replica holds; failing over to the replica", "shard", s, "pod", m.pod.Name, "keys", mk, "replica", q.pod.Name, "replicaKeys", qk)
			r.event(c, corev1.EventTypeWarning, "MasterRestartedEmpty",
				"shard %d: master %s restarted with %d keys, and its replica %s holds %d; holding the master's writes and having %s take over (CLUSTER FAILOVER FORCE), rather than letting it resync from the emptied master and drop them. %s",
				s, m.pod.Name, mk, q.pod.Name, qk, q.pod.Name, persistenceHint)
		}
		acted = true
		if rm.attempts >= restartedMasterAttempts || (rm.attempts > 0 && now.Sub(rm.last) < restartedMasterRetry) {
			continue
		}
		if ip := r.pauseWrites(ctx, m.pod, emptyMasterPause); ip != "" {
			rm.masterIP = ip
		}
		mode := agentapi.FailoverForce
		if rm.attempts >= 2 || onlyVoter(pods, m) {
			mode = agentapi.FailoverTakeover
		}
		rm.attempts++
		rm.last, rm.replica, rm.replicaKeys, rm.masterKeys = now, q.pod.Name, qk, mk
		if err := r.Agent.ClusterFailoverMode(ctx, q.ip(), mode); err != nil {
			rm.lastError = err.Error()
			log.Error(err, "asking the replica to take over from the restarted master", "shard", s, "replica", q.pod.Name, "mode", mode)
			continue
		}
		rm.lastError = ""
		log.Info("asked the replica to take over from the restarted master", "shard", s, "replica", q.pod.Name, "mode", mode, "attempt", rm.attempts)
	}
	return acted
}

// persistenceHint ends the Events above.
const persistenceHint = "With the append-only file on (the default; see the Persistence condition), a restarted master reloads its data instead."

// onlyVoter reports whether m is the only master that owns slots: then an
// election has no one but m itself to vote in it.
func onlyVoter(pods []*shardPod, m *shardPod) bool {
	for _, p := range pods {
		st := p.seen()
		if p == m || st == nil {
			continue
		}
		if me, ok := st.Myself(); ok && me.IsMaster() && me.SlotCount() > 0 {
			return false
		}
	}
	return true
}

// endRestartedMaster closes a case whose master no longer stands out: the
// replica has taken over, or no longer holds more than the master (it
// resynced from it after all, and its data is gone).
func (r *KividbClusterReconciler) endRestartedMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod, rm *restartedMaster) {
	r.unpauseWrites(ctx, rm.masterIP)
	rm.resolved = r.now()
	took := r.now().Sub(rm.detected).Round(100 * time.Millisecond)
	if m := shardMaster(pods, rm.shard); m != nil && m.pod.Name == rm.replica {
		rm.outcome = fmt.Sprintf("master %s restarted with %d keys; %s, which held %d, took over after %s", rm.master, rm.masterKeys, rm.replica, rm.replicaKeys, took)
		r.event(c, corev1.EventTypeNormal, "MasterRestartedEmptyResolved", "shard %d: %s", rm.shard, rm.outcome)
		return
	}
	if m := shardMaster(pods, rm.shard); m == nil || m.pod.Name != rm.master {
		// Another pod, or none that answered: not settled by the takeover.
		rm.outcome = fmt.Sprintf("master %s restarted with %d keys; the shard's master is no longer it, but not %s either", rm.master, rm.masterKeys, rm.replica)
		return
	}
	rm.lost = true
	rm.outcome = fmt.Sprintf("master %s restarted with %d keys; %s, which held %d, no longer holds more after %s (it resynced from the restarted master): those writes are lost", rm.master, rm.masterKeys, rm.replica, rm.replicaKeys, took)
	r.event(c, corev1.EventTypeWarning, "ShardDataLost", "shard %d: %s. %s", rm.shard, rm.outcome, persistenceHint)
}

// restartedMasterCondition reports the cases of the last
// restartedMasterShownFor, nil when there are none.
func (r *KividbClusterReconciler) restartedMasterCondition(c *kividbv1alpha1.KividbCluster) *metav1.Condition {
	var cases []*restartedMaster
	prefix := handoverKey(c, 0)
	prefix = prefix[:strings.LastIndexByte(prefix, '/')+1]
	r.restartedMasters.Range(func(k, v any) bool {
		if key := k.(string); strings.HasPrefix(key, prefix) {
			cases = append(cases, v.(*restartedMaster))
		}
		return true
	})
	if len(cases) == 0 {
		return nil
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].shard < cases[j].shard })
	reason := "FailedOver"
	var lines []string
	for _, rm := range cases {
		switch {
		case rm.resolved.IsZero() && rm.attempts >= restartedMasterAttempts:
			reason = "Stalled"
			lines = append(lines, fmt.Sprintf("shard %d: master %s restarted with %d keys; %s, which holds %d, has not taken over after %d requests (last error: %s)", rm.shard, rm.master, rm.masterKeys, rm.replica, rm.replicaKeys, rm.attempts, orNone(rm.lastError)))
		case rm.resolved.IsZero():
			if reason != "Stalled" {
				reason = "FailingOver"
			}
			lines = append(lines, fmt.Sprintf("shard %d: master %s restarted with %d keys; %s, which holds %d, is taking over (request %d)", rm.shard, rm.master, rm.masterKeys, rm.replica, rm.replicaKeys, rm.attempts))
		default:
			if rm.lost && reason == "FailedOver" {
				reason = "DataLost"
			}
			lines = append(lines, fmt.Sprintf("shard %d, %s: %s", rm.shard, rm.resolved.UTC().Format(time.RFC3339), rm.outcome))
		}
	}
	return &metav1.Condition{
		Type:               MasterRestartedEmptyCondition,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            strings.Join(lines, "; "),
		ObservedGeneration: c.Generation,
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
