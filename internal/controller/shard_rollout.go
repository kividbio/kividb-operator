package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Restarting a sharded cluster's pods: a template change, a new password,
// a KividbDbOps restart. As with one replicated set (see reconcileRollout),
// one pod at a time, replicas first, and only while everything else is
// healthy. A shard's master is never restarted while it is the master: a
// step-down asks its most caught-up replica for a CLUSTER FAILOVER, which
// the engine runs without losing a write (the master pauses its clients
// until the replica has caught up), and the old master is restarted as a
// replica afterwards. A shard with no replicas has no one to hand over
// to, and its master is restarted in place: its slots are down until it
// is back (a RestartingInPlace Warning says so first). With its
// append-only file off, it keeps only what its save at shutdown writes in
// time, so the rollout holds it instead (#90), until the cluster gets a
// replica, the file is turned on, or
// kividb.io/allow-restart-without-persistence says to go ahead. The other
// masters go on meanwhile. A master that holds no keys has nothing to lose,
// and is not held (#108). One that is not Ready is replaced as a stuck pod
// only once its engine is dead (#113).

// A handover can fail: from engine 1.2 the master pauses its writes for up
// to 10 s, and the replica gives the failover up if it has not caught up
// and won its election within 5 s ("Manual failover abandoned" in its
// log); the master then goes on as before. The operator asks again, a few
// times, and says so.
const (
	// failoverRetry is how long a CLUSTER FAILOVER the operator asked for
	// is given before it is asked for again.
	failoverRetry = 20 * time.Second
	// failoverAbandonedAfter is how long after a request a master that is
	// still the master is reported (a Warning event): well past the
	// engine's own 5 s.
	failoverAbandonedAfter = 15 * time.Second
	// maxFailoverAttempts is how many times in a row a shard's master is
	// asked to hand over before the operator stops asking...
	maxFailoverAttempts = 5
	// ...for this long; then it starts again.
	failoverGiveUpCooldown = 10 * time.Minute
)

// shardHandover is the state of one shard's handover, in
// KividbClusterReconciler.failovers.
type shardHandover struct {
	master   string    // the pod asked to step down
	attempts int       // CLUSTER FAILOVERs asked for in this round
	last     time.Time // when the last one was asked for
	warned   bool      // the last one has been reported as abandoned
	target   string    // the replica last asked to take over
}

// handoverStep says what to do about a pending handover at now: ask for a
// CLUSTER FAILOVER (again), and whether to report the last one as
// abandoned. It starts a new round of attempts once a spent one has
// cooled down.
func (h *shardHandover) step(now time.Time) (ask, warn bool) {
	since := now.Sub(h.last)
	warn = h.attempts > 0 && !h.warned && since >= failoverAbandonedAfter
	switch {
	case h.attempts >= maxFailoverAttempts:
		if since < failoverGiveUpCooldown {
			return false, warn
		}
		h.attempts = 0
		return true, warn
	case h.attempts > 0 && since < failoverRetry:
		return false, warn
	}
	return true, warn
}

// exhausted reports whether the operator has stopped asking for now.
func (h *shardHandover) exhausted() bool { return h.attempts >= maxFailoverAttempts }

func handoverKey(c *kividbv1alpha1.KividbCluster, shard int32) string {
	return fmt.Sprintf("%s/%s/%d", c.Namespace, c.Name, shard)
}

func (r *KividbClusterReconciler) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

// shardSynced reports whether replica p follows its shard's master m and
// has caught up with it (the same rule as replicaSynced).
func shardSynced(p, m *shardPod) bool {
	if p == nil || m == nil || p == m || p.status == nil || m.status == nil {
		return false
	}
	me, ok := p.myself()
	mm, mok := m.myself()
	if !ok || !mok || me.IsMaster() || me.MasterID != mm.ID || !p.status.MasterLinkUp {
		return false
	}
	if m.status.ReplicationOffset > 0 {
		return p.status.ReplicationOffset > 0 && m.status.ReplicationOffset-p.status.ReplicationOffset <= maxSyncLagBytes
	}
	return true
}

// shardHealthy is a pod that serves its part: a master owning its
// shard's slots, or a replica in sync with it.
func shardHealthy(v clusterView, p *shardPod) bool {
	if !p.ready || p.status == nil {
		return false
	}
	m := v.masters[p.shard]
	return m == p || shardSynced(p, m)
}

// reconcileShardStepDown runs the CLUSTER FAILOVER a StepDownAnnotation on
// a shard master's pod asks for, on the shard's most caught-up replica in
// sync. The annotation is removed by clearShardStepDowns once the pod is
// a replica. It reports whether it asked for a failover.
func (r *KividbClusterReconciler) reconcileShardStepDown(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	log := logf.FromContext(ctx)
	v := buildView(c, pods)
	acted := false
	pending := map[int32]*shardPod{}
	for _, p := range pods {
		// A master being deleted hands over too: its preStop hook holds
		// the engine meanwhile (see the agent's /prestop).
		if (p.pod.Annotations[StepDownAnnotation] != "true" && p.pod.DeletionTimestamp == nil) || v.masters[p.shard] != p {
			continue
		}
		pending[p.shard] = p
	}
	// A handover is over once its shard's master is no longer asked to
	// step down (it has, or the request was withdrawn).
	for s := int32(0); s < shardCount(c); s++ {
		key := handoverKey(c, s)
		if at, ok := r.failovers.Load(key); ok && (pending[s] == nil || at.(*shardHandover).master != pending[s].pod.Name) {
			r.failovers.Delete(key)
		}
	}
	for _, p := range pods {
		if pending[p.shard] != p {
			continue
		}
		key := handoverKey(c, p.shard)
		at, _ := r.failovers.LoadOrStore(key, &shardHandover{master: p.pod.Name})
		h := at.(*shardHandover)
		now := r.now()
		ask, warn := h.step(now)
		if warn {
			h.warned = true
			next := "asking again"
			if h.exhausted() {
				next = fmt.Sprintf("gave up after %d attempts; asking again in %s", maxFailoverAttempts, failoverGiveUpCooldown)
			}
			r.event(c, corev1.EventTypeWarning, "ShardSwitchoverAbandoned",
				"shard %d: %s is still the master %s after %s was asked to take over (attempt %d of %d); the engine gives a manual failover up when the replica has not caught up and won its election within 5 s (\"Manual failover abandoned\" in its log); %s",
				p.shard, p.pod.Name, now.Sub(h.last).Round(time.Second), h.target, h.attempts, maxFailoverAttempts, next)
		}
		if !ask {
			continue // the engine is still on it, or the operator has stopped asking for now
		}
		var best *shardPod
		for _, q := range pods {
			if q.shard == p.shard && q.ready && shardSynced(q, p) && (best == nil || q.status.ReplicationOffset > best.status.ReplicationOffset) {
				best = q
			}
		}
		if best == nil {
			log.V(1).Info("step-down requested, but no replica of the shard is in sync yet", "pod", p.pod.Name)
			continue
		}
		if err := r.Agent.ClusterFailover(ctx, best.ip()); err != nil {
			log.Error(err, "asking for CLUSTER FAILOVER", "pod", best.pod.Name)
			continue
		}
		first := h.attempts == 0 && h.last.IsZero()
		h.attempts++
		h.last, h.warned, h.target = now, false, best.pod.Name
		r.handedOver.Store(key, plannedHandover{from: p.pod.Name, at: now})
		if first {
			r.event(c, corev1.EventTypeNormal, "ShardSwitchover", "shard %d: asked %s to take over from %s (CLUSTER FAILOVER)", p.shard, best.pod.Name, p.pod.Name)
		} else {
			log.Info("asking for CLUSTER FAILOVER again", "shard", p.shard, "master", p.pod.Name, "replica", best.pod.Name, "attempt", h.attempts)
		}
		acted = true
	}
	return acted
}

// switchoverCondition reports the shards whose master has been asked to
// hand over and has not yet: the ShardSwitchover condition, nil when
// there are none.
func (r *KividbClusterReconciler) switchoverCondition(c *kividbv1alpha1.KividbCluster) *metav1.Condition {
	var lines []string
	reason := "InProgress"
	for s := int32(0); s < shardCount(c); s++ {
		at, ok := r.failovers.Load(handoverKey(c, s))
		if !ok {
			continue
		}
		h := at.(*shardHandover)
		switch {
		case h.attempts == 0 && h.last.IsZero():
			lines = append(lines, fmt.Sprintf("shard %d: %s waits for a replica in sync to hand over to", s, h.master))
		case h.exhausted():
			reason = "Stalled"
			lines = append(lines, fmt.Sprintf("shard %d: %s did not hand over in %d attempts (CLUSTER FAILOVER to %s); asking again %s after the last", s, h.master, h.attempts, h.target, failoverGiveUpCooldown))
		default:
			if h.attempts > 1 && reason == "InProgress" {
				reason = "Retrying"
			}
			lines = append(lines, fmt.Sprintf("shard %d: %s handing over to %s (attempt %d of %d)", s, h.master, h.target, h.attempts, maxFailoverAttempts))
		}
	}
	if len(lines) == 0 {
		return nil
	}
	return &metav1.Condition{
		Type:               "ShardSwitchover",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            strings.Join(lines, "; "),
		ObservedGeneration: c.Generation,
	}
}

// clearShardStepDowns removes the step-down request from a pod that is no
// longer a master: the request is fulfilled.
func (r *KividbClusterReconciler) clearShardStepDowns(ctx context.Context, pods []*shardPod) {
	for _, p := range pods {
		if p.pod.Annotations[StepDownAnnotation] != "true" || roleOf(p) != kividbv1alpha1.RoleReplica {
			continue
		}
		patch := client.MergeFrom(p.pod.DeepCopy())
		delete(p.pod.Annotations, StepDownAnnotation)
		if err := r.Patch(ctx, p.pod, patch); err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "removing step-down annotation", "pod", p.pod.Name)
		}
	}
}

// requestStepDown sets the StepDownAnnotation on pod.
func (r *KividbClusterReconciler) requestStepDown(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations[StepDownAnnotation] == "true" {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[StepDownAnnotation] = "true"
	return r.Patch(ctx, pod, patch)
}

// reconcileShardedRollout moves the pods onto their shard StatefulSet's
// current template (OnDelete), one at a time across the whole cluster.
// It reports whether it replaced a pod or asked for a step-down. read is
// the status this pass started from, and kdbConfig the cluster's
// KividbConfig, if any.
func (r *KividbClusterReconciler) reconcileShardedRollout(ctx context.Context, c *kividbv1alpha1.KividbCluster, read *kividbv1alpha1.KividbClusterStatus, pods []*shardPod, kdbConfig *kividbv1alpha1.KividbConfig) bool {
	log := logf.FromContext(ctx)

	targets := map[int32]string{}
	templates := map[int32]*corev1.PodSpec{}
	for s := int32(0); s < shardCount(c); s++ {
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: shardStatefulSetName(c, s)}, &sts); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error(err, "rollout: reading StatefulSet", "shard", s)
			}
			return false
		}
		if sts.Status.UpdateRevision == "" || sts.Status.ObservedGeneration < sts.Generation {
			return false // the StatefulSet controller has not seen the latest template yet
		}
		targets[s] = sts.Status.UpdateRevision
		templates[s] = &sts.Spec.Template.Spec
	}

	var outdated []*shardPod
	for _, p := range pods {
		if p.pod.DeletionTimestamp != nil {
			return false // one replacement at a time
		}
		if p.pod.Labels[appsv1.StatefulSetRevisionLabel] != targets[p.shard] {
			outdated = append(outdated, p)
		}
	}
	if len(outdated) == 0 {
		r.rolloutDone(c)
		if c.Annotations[kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation] != "" {
			r.clearRestartWithoutPersistence(ctx, c, read)
		}
		return false
	}

	// A KividbConfig `dir` that moves the engine's files holds the rollout,
	// replicas or not, the append-only file on or not, and stuck pod or not
	// (#112). Each node keeps its nodes.conf there too, so a pod whose dir
	// moves would come back as a new node: with no replica, one that owns
	// no slots and holds no data; with replicas, one left to the rule for a
	// lost volume, which a dir change has not been tried with. A dir change
	// on a running sharded cluster is not supported yet. The annotation,
	// which accepts what a restart loses of a master whose file is off,
	// does not cover it.
	var moves []dataDirMove
	for _, p := range outdated {
		if m, ok := dataDirMoved(p.pod, templates[p.shard]); ok {
			moves = append(moves, m)
		}
	}
	if len(moves) > 0 {
		r.rolloutHeld(c, shardedDataDirHoldMessage(moves))
		return false
	}

	// A pod serving nothing has nothing to protect. A master the hold below
	// keeps (no replica to hand over to, no annotation: one not Ready is not
	// heard from, so held whatever the KividbConfig says) is that only once
	// its engine is dead (engineDead): one whose kividb container runs,
	// loading a large snapshot or too busy to answer its probe, holds data
	// in memory, and stays held, the hold saying so (#113).
	allowLoss := c.Annotations[kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation] == "true"
	replaced := false
	for _, p := range outdated {
		if p.ready || time.Since(unreadySince(p.pod)) < stuckPodGrace {
			continue
		}
		if c.Spec.Replicas == 0 && !allowLoss && !engineDead(p.pod) {
			continue
		}
		log.Info("rollout: replacing unready pod on an outdated template", "pod", p.pod.Name)
		if err := r.Delete(ctx, p.pod); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "rollout: deleting pod", "pod", p.pod.Name)
			continue
		}
		r.event(c, corev1.EventTypeWarning, "StuckPodReplaced", "%s was not ready and on an outdated pod template; replaced it", p.pod.Name)
		r.rolloutProgressed(c, fmt.Sprintf("replaced %s, which was not ready, on an outdated pod template", p.pod.Name))
		replaced = true
	}
	if replaced {
		return true
	}
	if rebalanceOf(c) != nil {
		return false // slots are moving: restarts wait for the new shard count
	}

	// The rest only while the cluster is whole and serving every slot. A
	// held master that is alive but not Ready (kept from the stuck-pod pass
	// above) is reported before that: one loading or hung has its slots seen
	// as failed by the others after the node timeout, so the cluster is not
	// ok, and the hold below would never be reached (#113).
	v := buildView(c, pods)
	if int32(len(pods)) != shardCount(c)*(c.Spec.Replicas+1) || !v.formed || v.anchor == nil || v.anchor.status.State != "ok" {
		if c.Spec.Replicas == 0 && !allowLoss {
			var alive []*shardPod
			for _, p := range outdated {
				if !p.ready && !engineDead(p.pod) {
					alive = append(alive, p)
				}
			}
			if len(alive) > 0 {
				r.rolloutHeld(c, heldMastersMessage(alive, kdbConfig))
			}
		}
		return false
	}
	healthy := map[*shardPod]bool{}
	unhealthy := 0
	for _, p := range pods {
		healthy[p] = shardHealthy(v, p)
		if !healthy[p] {
			unhealthy++
		}
	}
	isMaster := func(p *shardPod) bool { return v.masters[p.shard] == p }
	// Replicas first, masters last; among them, one that is not healthy
	// first (it is the least use as it stands), then by name, highest
	// first.
	sort.Slice(outdated, func(i, j int) bool {
		a, b := outdated[i], outdated[j]
		if isMaster(a) != isMaster(b) {
			return isMaster(b)
		}
		if healthy[a] != healthy[b] {
			return !healthy[a]
		}
		return a.pod.Name > b.pod.Name
	})
	// A master with no replica to hand over to and its append-only file off
	// is held (#90), unless it holds no keys (#108) or the annotation says
	// to restart it anyway. It does not hold the others back (the file is
	// on or off per master): the next is the first that is not held, and
	// the hold is reported only once every pod left is held.
	//
	// With no replicas, every pod is its shard's only one (counted above),
	// and so its master: also one this pass did not hear from (not Ready,
	// or its agent did not answer), which buildView does not count as a
	// master. Such a master is held whatever the KividbConfig says: without
	// its report, whether its engine has the file off (CONFIG SET appendonly
	// no) is not known, and the rollout does not restart on a guess. One not
	// Ready is replaced once it has been so for stuckPodGrace (above); one
	// whose agent does not answer is held for as long as it does not, and
	// the hold says so.
	//
	// An empty master has nothing to lose, and a write that reaches it after
	// its agent's report is not lost either: with the file off, either way
	// withoutPersistence finds it so, the engine still writes a snapshot as
	// it stops, keys and index definitions alike. SIGTERM always saves
	// (`save ""` and shutdown-on-sigterm change nothing, and the operator
	// never sends SHUTDOWN NOSAVE), the agent's /prestop returns at once for
	// a master with no replicas, and the snapshot of a near-empty dataset is
	// written in milliseconds, inside a grace period of 10 s or more (below
	// that, the master counts as holding keys: see holdsNoKeys). The save
	// can still fail (a full or unwritable volume ends the process without
	// a snapshot), and a script running past its time limit can use up the
	// grace period; neither changes the decision, since both are about a
	// write landing in a window of seconds on an empty master. The snapshot
	// goes to the engine's working directory, /data, the pod's volume,
	// unless a KividbConfig `dir` moves it off, which loses data with the
	// file on too (#109).
	var next *shardPod
	var held []*shardPod
	for _, p := range outdated {
		if c.Spec.Replicas == 0 && !allowLoss && (p.status == nil || withoutPersistence(p, kdbConfig) != "") && !holdsNoKeys(c, p) {
			held = append(held, p)
			continue
		}
		next = p
		break
	}
	if next == nil {
		r.rolloutHeld(c, heldMastersMessage(held, kdbConfig))
		return false
	}
	// Every pod but the one replaced must be healthy (see reconcileRollout).
	if unhealthy > 1 || (unhealthy == 1 && healthy[next]) {
		var blockers []rolloutBlocker
		for _, p := range pods {
			switch {
			case healthy[p] || p == next:
			case !p.ready:
				blockers = append(blockers, rolloutBlocker{pod: p.pod.Name, why: "not Ready"})
			case p.status == nil:
				blockers = append(blockers, rolloutBlocker{pod: p.pod.Name, why: "its agent did not answer"})
			default:
				blockers = append(blockers, rolloutBlocker{pod: p.pod.Name, syncing: true,
					why: fmt.Sprintf("replica of shard %d not in sync: %s", p.shard, whyShardNotSynced(p, v.masters[p.shard]))})
			}
		}
		r.rolloutWaiting(ctx, c, next.pod.Name, blockers)
		return false
	}

	if isMaster(next) && c.Spec.Replicas > 0 {
		if err := r.requestStepDown(ctx, next.pod); err != nil {
			log.Error(err, "rollout: asking the shard master to step down", "pod", next.pod.Name)
			return false
		}
		r.rolloutProgressed(c, fmt.Sprintf("asked shard %d's master %s to hand over, to replace it next", next.shard, next.pod.Name))
		return r.reconcileShardStepDown(ctx, c, pods)
	}
	progress := fmt.Sprintf("replacing %s (%d more to go)", next.pod.Name, len(outdated)-1)
	if c.Spec.Replicas == 0 {
		// Its shard's master, with no replica to hand over to: restarted in
		// place. Without the file, only because it holds no keys, which the
		// status says so that it reads apart from a restart with the file
		// on, or because the annotation says so (see above).
		why := withoutPersistence(next, kdbConfig)
		empty := why != "" && holdsNoKeys(c, next)
		if empty {
			progress += fmt.Sprintf(": %s, but it reported no keys, so a restart in place has nothing to lose", why)
		}
		if why == "" || empty {
			r.event(c, corev1.EventTypeWarning, "RestartingInPlace",
				"restarting shard %d's master %s in place to apply the updated pod template: it has no replica to hand over to, so its slots are unavailable until it is back", next.shard, next.pod.Name)
		} else {
			r.event(c, corev1.EventTypeWarning, "RestartingWithoutPersistence",
				"restarting shard %d's master %s in place to apply the updated pod template, with %s (%s is set): it keeps only what its save at shutdown writes in time, and its slots are unavailable until it is back",
				next.shard, next.pod.Name, why, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
		}
	}
	log.Info("rollout: replacing pod to apply the updated pod template", "pod", next.pod.Name, "remaining", len(outdated)-1)
	if err := r.Delete(ctx, next.pod); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "rollout: deleting pod", "pod", next.pod.Name)
		return false
	}
	r.event(c, corev1.EventTypeNormal, "RollingUpdate", "replacing %s to apply the updated pod template (%d more to go)", next.pod.Name, len(outdated)-1)
	r.rolloutProgressed(c, progress)
	return true
}

// emptyRestartMinGrace is the shortest grace period
// (spec.terminationGracePeriodSeconds), in seconds, with which a shard
// master that holds no keys is restarted in place with its append-only
// file off: below it, the kubelet may kill the engine before or during its
// save at shutdown, and a write that reached it after its agent's report
// would be lost.
const emptyRestartMinGrace = 10

// holdsNoKeys reports whether shard pod p of c holds no keys, for the hold
// (#108): as its agent reports it, on a count the agent knows (one from
// before 0.7.0 reports none), not while the engine loads its data, when it
// can still show 0, and only with a grace period of emptyRestartMinGrace or
// more. Otherwise it counts as holding keys. An unready pod's probe does
// not count.
func holdsNoKeys(c *kividbv1alpha1.KividbCluster, p *shardPod) bool {
	st := p.status
	return st != nil && st.KeyCountKnown && !st.Loading && st.KeyCount == 0 &&
		*gracePeriod(c) >= emptyRestartMinGrace
}

// withoutPersistence says why shard master p would keep only what its save
// at shutdown writes in time if restarted, or "" if its data survives: the
// KividbConfig turns the append-only file off (the replacement runs
// without it), or its engine reports the file off now. An agent from
// before 0.7.0 reports neither that nor the key count (KeyCountKnown), so
// what it does not say is not taken for a file that is off.
func withoutPersistence(p *shardPod, kdbConfig *kividbv1alpha1.KividbConfig) string {
	if appendOnlyDisabled(kdbConfig) {
		return fmt.Sprintf("KividbConfig %s sets appendonly %q", kdbConfig.Name, appendOnlySetting(kdbConfig))
	}
	if st := p.status; st != nil && st.KeyCountKnown && !st.AofEnabled {
		return "its engine reports its append-only file off"
	}
	return ""
}

// heldMastersMessage says which shard masters the rollout holds (held, in
// any order, at least one), why, and how to go on: those with the file off,
// then those the pass did not hear from.
func heldMastersMessage(held []*shardPod, kdbConfig *kividbv1alpha1.KividbConfig) string {
	var off, unheard []*shardPod
	for _, p := range held {
		if withoutPersistence(p, kdbConfig) != "" {
			off = append(off, p)
		} else {
			unheard = append(unheard, p)
		}
	}
	var parts []string
	if len(off) > 0 {
		parts = append(parts, fileOffMessage(off, kdbConfig))
	}
	if len(unheard) > 0 {
		parts = append(parts, unheardMessage(unheard))
	}
	if alive := aliveNotReady(held); alive != "" {
		parts = append(parts, alive)
	}
	return strings.Join(parts, "; ")
}

// aliveNotReady says which of held have been not Ready for stuckPodGrace
// though their engine is alive, and so are not replaced as stuck pods
// (#113), and how to restart one alone, or "". Before stuckPodGrace a pod
// is not eligible either way, so nothing is said of it.
func aliveNotReady(held []*shardPod) string {
	var names []string
	for _, p := range held {
		if !p.ready && !engineDead(p.pod) && time.Since(unreadySince(p.pod)) >= stuckPodGrace {
			names = append(names, p.pod.Name)
		}
	}
	sort.Strings(names)
	switch len(names) {
	case 0:
		return ""
	case 1:
		return fmt.Sprintf("%s is alive but not Ready (its kividb container runs), so it is not replaced as a stuck pod while it does; to restart it alone, delete pod %s by hand, though a hung engine may not finish its save at shutdown", names[0], names[0])
	}
	return fmt.Sprintf("%s are alive but not Ready (their kividb containers run), so they are not replaced as stuck pods while they do; to restart one alone, delete its pod by hand, though a hung engine may not finish its save at shutdown", strings.Join(names, ", "))
}

// unheardMessage says that the rollout holds the shard masters in held
// (at least one), which the pass did not hear from (not Ready, or their
// agent did not answer), and how to go on.
func unheardMessage(held []*shardPod) string {
	if len(held) == 1 {
		p := held[0]
		why, fix := "its agent did not answer", "Get its agent answering (its log says why it does not)"
		if !p.ready {
			why, fix = "it is not Ready", "Get it Ready (its log says why it is not)"
		}
		return fmt.Sprintf("the rollout holds shard %d's master %s: it has no replica to hand over to, and %s, so whether its append-only file is on is not known, nor whether a restart in place to apply the updated pod template would lose data. %s, give the cluster a replica (spec.replicas), or annotate the cluster %s=true to restart it anyway",
			p.shard, p.pod.Name, why, fix, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
	}
	why, fix := "their agents did not answer", "Get their agents answering (their logs say why they do not)"
	for _, p := range held {
		if !p.ready {
			why, fix = "the operator did not hear from them (not Ready, or their agent not answering)", "Get them Ready and their agents answering (their logs say why not)"
			break
		}
	}
	shards, names := heldNames(held)
	return fmt.Sprintf("the rollout holds the masters of shards %s (%s): none has a replica to hand over to, and %s, so whether their append-only files are on is not known, nor whether a restart in place to apply the updated pod template would lose data. %s, give the cluster a replica (spec.replicas), or annotate the cluster %s=true to restart them anyway",
		shards, names, why, fix, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
}

// heldNames lists held (at least two) by shard: "0, 1 and 2" and their
// pods' names, "s-0-0, s-1-0, s-2-0".
func heldNames(held []*shardPod) (shards, names string) {
	held = slices.Clone(held)
	slices.SortFunc(held, func(a, b *shardPod) int { return int(a.shard) - int(b.shard) })
	s := make([]string, len(held))
	n := make([]string, len(held))
	for i, p := range held {
		s[i], n[i] = fmt.Sprint(p.shard), p.pod.Name
	}
	return strings.Join(s[:len(s)-1], ", ") + " and " + s[len(s)-1], strings.Join(n, ", ")
}

// fileOffMessage says that the rollout holds the shard masters in held (at
// least one), whose append-only file is off, and how to go on. They are
// held for the same reason: the KividbConfig, which is the cluster's, or
// else each one's engine.
func fileOffMessage(held []*shardPod, kdbConfig *kividbv1alpha1.KividbConfig) string {
	if len(held) == 1 {
		p := held[0]
		return fmt.Sprintf("the rollout holds shard %d's master %s: it has no replica to hand over to, and %s, so restarted in place to apply the updated pod template it keeps only what its save at shutdown writes in time. Give the cluster a replica (spec.replicas), turn the file on (appendonly yes in the KividbConfig, and CONFIG SET appendonly yes on the running master), or annotate the cluster %s=true to restart it anyway",
			p.shard, p.pod.Name, withoutPersistence(p, kdbConfig), kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
	}
	shards, names := heldNames(held)
	why := withoutPersistence(held[0], kdbConfig)
	if !appendOnlyDisabled(kdbConfig) {
		why = "the engine of each reports its append-only file off"
	}
	return fmt.Sprintf("the rollout holds the masters of shards %s (%s): none has a replica to hand over to, and %s, so restarted in place to apply the updated pod template each keeps only what its save at shutdown writes in time. Give the cluster a replica (spec.replicas), turn the file on (appendonly yes in the KividbConfig, and CONFIG SET appendonly yes on each running master), or annotate the cluster %s=true to restart them anyway",
		shards, names, why, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
}

// clearRestartWithoutPersistence removes
// AllowRestartWithoutPersistenceAnnotation from c once no pod is left on an
// outdated template, so that it does not let a later rollout through. As
// clearUnreadFailoverOverride does: the patch goes through a copy, and c
// takes the new resourceVersion only if the stored status is still read.
func (r *KividbClusterReconciler) clearRestartWithoutPersistence(ctx context.Context, c *kividbv1alpha1.KividbCluster, read *kividbv1alpha1.KividbClusterStatus) {
	stored := c.DeepCopy()
	patch := client.MergeFrom(stored.DeepCopy())
	delete(stored.Annotations, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
	if err := r.Patch(ctx, stored, patch); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "removing the annotation", "annotation", kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
		}
		return
	}
	delete(c.Annotations, kividbv1alpha1.AllowRestartWithoutPersistenceAnnotation)
	if equality.Semantic.DeepEqual(stored.Status, *read) {
		c.ResourceVersion = stored.ResourceVersion
	}
}
