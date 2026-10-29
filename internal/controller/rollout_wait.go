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

// A rollout (a template change, a new password, a KividbDbOps restart)
// replaces one pod at a time, and only while every other pod is Ready and
// every replica in sync. That wait is right, but it used to be silent: a
// replica that never caught up (a frozen engine, a link that kept
// failing) stopped the rollout with the cluster still Running and Ready.
// It is now reported on the RolloutProgressing condition:
//
//   - True, reason ReplacingPod: a pod was just replaced (or the master
//     asked to hand over first);
//   - True, reason WaitingForReplicaSync or WaitingForPodReady: the
//     rollout waits, as it does after every replacement while the new pod
//     syncs, for less than rolloutStallAfter so far;
//   - False, same reasons: it has waited on the same pods for longer. A
//     Warning Event (RolloutWaiting) and a log line name each pod and why,
//     again every rolloutWaitReportEvery while it lasts.
//
// The condition is removed once no pod is on an outdated template.

const (
	// RolloutProgressingCondition is the condition type.
	RolloutProgressingCondition = "RolloutProgressing"

	// rolloutStallAfter is how long a rollout may wait on the same pods
	// before it is reported as stalled. A replaced replica's full sync
	// takes seconds for most datasets.
	rolloutStallAfter = 2 * time.Minute
	// rolloutWaitReportEvery is how often a stall that goes on is
	// reported again (Event and log line).
	rolloutWaitReportEvery = 10 * time.Minute
)

// rolloutBlocker is a pod a rollout waits for, and why.
type rolloutBlocker struct {
	pod string
	// syncing: a Ready replica that is not in sync (else: not Ready).
	syncing bool
	why     string
}

// rolloutWait is the state of one cluster's waiting rollout, in
// KividbClusterReconciler.rolloutWaits.
type rolloutWait struct {
	pods     string    // the pods waited for, sorted and joined
	since    time.Time // since when it has waited for those pods
	reported time.Time // when the stall was last reported; zero: not yet
}

func rolloutWaitKey(c *kividbv1alpha1.KividbCluster) string { return c.Namespace + "/" + c.Name }

// rolloutWaiting records that the rollout of c cannot replace next because
// of blockers, and sets the RolloutProgressing condition, reporting a
// stall as described above.
func (r *KividbClusterReconciler) rolloutWaiting(ctx context.Context, c *kividbv1alpha1.KividbCluster, next string, blockers []rolloutBlocker) {
	sort.Slice(blockers, func(i, j int) bool { return blockers[i].pod < blockers[j].pod })
	names := make([]string, 0, len(blockers))
	lines := make([]string, 0, len(blockers))
	reason := "WaitingForPodReady"
	for _, b := range blockers {
		names = append(names, b.pod)
		lines = append(lines, b.pod+": "+b.why)
		if b.syncing {
			reason = "WaitingForReplicaSync"
		}
	}
	pods := strings.Join(names, ",")
	now := r.now()
	key := rolloutWaitKey(c)
	w := &rolloutWait{pods: pods, since: now}
	if at, ok := r.rolloutWaits.Load(key); ok && at.(*rolloutWait).pods == pods {
		w = at.(*rolloutWait)
	} else {
		r.rolloutWaits.Store(key, w)
	}

	waited := now.Sub(w.since)
	stalled := waited >= rolloutStallAfter
	message := fmt.Sprintf("the rollout waits to replace %s until every other pod is Ready and in sync: %s", next, strings.Join(lines, "; "))
	status := metav1.ConditionTrue
	if stalled {
		status = metav1.ConditionFalse
		message = fmt.Sprintf("the rollout has waited %s to replace %s, for: %s", waited.Round(time.Second), next, strings.Join(lines, "; "))
	}
	setCondition(&c.Status.Conditions, metav1.Condition{
		Type:               RolloutProgressingCondition,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: c.Generation,
	})

	log := logf.FromContext(ctx)
	if !stalled || (!w.reported.IsZero() && now.Sub(w.reported) < rolloutWaitReportEvery) {
		log.V(1).Info("rollout: waiting", "next", next, "waitingFor", lines)
		return
	}
	w.reported = now
	log.Info("rollout: stalled, waiting for pods that are not Ready or not in sync", "next", next, "waited", waited.Round(time.Second).String(), "waitingFor", lines)
	r.event(c, corev1.EventTypeWarning, "RolloutWaiting", "%s", message)
}

// rolloutProgressed records that the rollout of c replaced a pod (or asked
// the master to hand over first).
func (r *KividbClusterReconciler) rolloutProgressed(c *kividbv1alpha1.KividbCluster, message string) {
	r.rolloutWaits.Delete(rolloutWaitKey(c))
	setCondition(&c.Status.Conditions, metav1.Condition{
		Type:               RolloutProgressingCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "ReplacingPod",
		Message:            message,
		ObservedGeneration: c.Generation,
	})
}

// rolloutDone records that no pod of c is on an outdated template.
func (r *KividbClusterReconciler) rolloutDone(c *kividbv1alpha1.KividbCluster) {
	r.rolloutWaits.Delete(rolloutWaitKey(c))
	removeCondition(&c.Status.Conditions, RolloutProgressingCondition)
}

// whyNotSynced says, for a replica replicaSynced holds not to be in sync
// with the master at masterIP:port, why: its link, how far behind it is
// and when it last heard from the master.
func whyNotSynced(master, replica *agentapi.StatusResponse, masterIP string, port int32) string {
	switch {
	case replica == nil:
		return "its agent did not answer"
	case replica.Role != agentapi.RoleReplica:
		return fmt.Sprintf("it reports role %s", replica.Role)
	case replica.MasterHost != "" && !followsMaster(replica, masterIP, port):
		return fmt.Sprintf("it replicates from %s:%d, not from the master at %s:%d", replica.MasterHost, replica.MasterPort, masterIP, port)
	case master == nil:
		return "the master could not be asked"
	}
	var parts []string
	if replica.MasterLinkUp != nil {
		parts = append(parts, map[bool]string{true: "link up", false: "link down"}[*replica.MasterLinkUp])
	}
	parts = append(parts, lagText(master.ReplicationOffset, replica.ReplicationOffset, master.KeyCount, replica.KeyCount))
	if s := replica.MasterLastIOSeconds; s != nil {
		parts = append(parts, lastIOText(*s))
	}
	return strings.Join(parts, ", ")
}

// whyShardNotSynced is whyNotSynced for a replica p of a shard whose
// master is m (see shardSynced).
func whyShardNotSynced(p, m *shardPod) string {
	switch {
	case p.status == nil:
		return "its agent did not answer"
	case m == nil || m.status == nil:
		return "its shard's master could not be asked"
	}
	me, ok := p.myself()
	mm, mok := m.myself()
	switch {
	case !ok || !mok:
		return "its cluster node could not be read"
	case me.IsMaster():
		return fmt.Sprintf("it is a master, not a replica of %s", m.pod.Name)
	case me.MasterID != mm.ID:
		return fmt.Sprintf("it replicates from node %s, not from its shard's master %s", me.MasterID, m.pod.Name)
	}
	parts := []string{map[bool]string{true: "link up", false: "link down"}[p.status.MasterLinkUp]}
	parts = append(parts, lagText(m.status.ReplicationOffset, p.status.ReplicationOffset, 0, 0))
	if s := p.status.MasterLastIOSeconds; s != nil {
		parts = append(parts, lastIOText(*s))
	}
	return strings.Join(parts, ", ")
}

func lagText(masterOffset, replicaOffset, masterKeys, replicaKeys int64) string {
	switch {
	case masterOffset <= 0:
		return fmt.Sprintf("%d keys against the master's %d", replicaKeys, masterKeys)
	case replicaOffset <= 0:
		return "first full sync not finished (offset 0)"
	}
	return fmt.Sprintf("%d bytes behind the master (offset %d of %d)", max(masterOffset-replicaOffset, 0), replicaOffset, masterOffset)
}

func lastIOText(seconds int64) string {
	if seconds < 0 {
		return "no I/O from the master (link down)"
	}
	return fmt.Sprintf("last I/O from the master %d s ago", seconds)
}
