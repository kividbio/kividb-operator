package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Waiting for a master that may come back on its volume (#78).
//
// A master whose pod is force-deleted used to be failed away from at once:
// its pod object is gone, or its replacement (a new UID) not Ready. If its
// engine still answers, the fence holds its writes and its replica catches
// up (fence.go). But if its engine is already gone, the replacement pod may
// be coming back on the same volume, holding writes it acknowledged that no
// replica has; promoting a replica first would have them resynced away. So
// the failover waits for it, until its new pod is Ready and has reported,
// and then compares it with its replicas as for a master that restarted in
// place (regressedMaster, #42) before promoting anyone -- for
// failover.restartWaitSeconds at most, counted from when it was found gone
// (or its replacement made, whichever is earlier: an operator that
// restarted does not wait it out again), whether its pod came back or not.
// A replacement that cannot be scheduled is not waited for: its volume is
// not coming back soon. The operator does not read Nodes to tell a node
// that is not Ready, so that its RBAC stays as small as it is: the
// scheduler keeps new pods off such a node, so a replacement pinned there
// by its volume shows as unschedulable. The cap covers the rest, a volume
// that cannot attach yet in particular: on AKS or EBS a zonal disk lets the
// replacement schedule on another node in the zone, where it sits in
// ContainerCreating (Multi-Attach) while the disk is still attached to the
// old node -- never unschedulable. The cap is checked on each pass, so it
// can be overshot by up to the 5 s a waiting pass requeues after. Each
// wait, and how it ended, is an event (FailoverWait).

// defaultRestartWait is failover.restartWaitSeconds when not set.
const defaultRestartWait = 15 * time.Second

// restartWait is how long a failover waits for a master to come back on its
// volume: failover.restartWaitSeconds, 0 for not at all.
func restartWait(c *kividbv1alpha1.KividbCluster) time.Duration {
	if s := c.Spec.Failover.RestartWaitSeconds; s != nil {
		return time.Duration(*s) * time.Second
	}
	return defaultRestartWait
}

// restartWaiting is a wait for a master to come back: which master, which
// pod of it (the incarnation last recorded for it), and since when. over:
// the wait ran out, or was skipped (its replacement cannot be scheduled).
// unsynced: no replica was in sync with it, so it is waited for until it
// comes back (waitForUnsynced). It is kept until the master changes, so
// that a failover that does not go through after it is tried again on the
// next pass at once, without a new wait or new events about one.
type restartWaiting struct {
	master, incarnation string
	since               time.Time
	over, unsynced      bool
}

// waitForUnsynced holds the failover away from master name -- its pod gone,
// or replaced and not ready yet (mv its replacement, or nil), or the same
// pod restarting in place past unhealthyThresholdSeconds (mv nil); last its
// entry in the last persisted status -- for as long as it takes it to come
// back, when no ready replica was in sync with it the last time that could
// be told (#75): the one replica of a replicas: 1 cluster in the middle of
// a full resync, say, which holds the snapshot it was sent but not the
// writes queued after it. Those writes are on the master's volume.
// Promoting the replica, and resyncing the master from it when it comes
// back, would lose them. So neither failover.restartWaitSeconds nor a
// replacement that cannot be scheduled ends this wait, nor
// unhealthyThresholdSeconds for one that restarts in place: only the
// master coming back, or AllowUnsyncedFailoverAnnotation, which lets the
// failover go on. A master whose engine still answers is not waited for:
// the fence holds its writes, and the replica promoted catches up with it
// first. Nor one with no entry in the status: nothing tells which replicas
// were in sync with it. Nor when the replicas say the append-only file is
// off: the master then comes back with only its last save, and nothing on
// its volume is worth the wait. views are the replicas a failover may
// promote (failoverPool). It returns the error that holds the failover, or
// nil.
func (r *KividbClusterReconciler) waitForUnsynced(ctx context.Context, c *kividbv1alpha1.KividbCluster, name string, mv *podView, last kividbv1alpha1.KividbPodStatus, views map[string]*podView, synced map[string]bool) error {
	if c.Annotations[kividbv1alpha1.AllowUnsyncedFailoverAnnotation] == "true" || last.Name == "" ||
		electReplica(views, name) == "" || electReplica(onlySynced(views, synced), name) != "" || aofOff(views, name) {
		return nil
	}
	if last.PodIP != "" && last.RunID != "" {
		if runID, err := r.runOf(ctx, last.PodIP); err == nil && runID == last.RunID {
			return nil
		}
	}
	key, now := c.Namespace+"/"+c.Name, r.now()
	var w *restartWaiting
	if v, ok := r.restartWaits.Load(key); ok {
		if got := v.(*restartWaiting); got.master == name && got.incarnation == last.Incarnation {
			w = got
		}
	}
	if w == nil {
		w = &restartWaiting{master: name, incarnation: last.Incarnation, since: now}
		r.restartWaits.Store(key, w)
	}
	if mv != nil && !mv.pod.CreationTimestamp.IsZero() && mv.pod.CreationTimestamp.Time.Before(w.since) {
		w.since = mv.pod.CreationTimestamp.Time // made before this operator saw it gone
	}
	if !w.unsynced {
		w.unsynced, w.over = true, false
		var replicas []string
		for n, v := range views {
			if n != name && v.ready {
				replicas = append(replicas, n)
			}
		}
		sort.Strings(replicas)
		r.event(c, corev1.EventTypeWarning, "FailoverBlocked",
			"%s does not answer, and no replica was in sync with it (%s): its volume may hold acknowledged writes that none of them has. Waiting for it to come back, however long that takes, rather than promote a replica without them. To fail over anyway, losing the writes it acknowledged since that replica was last in sync, annotate the cluster %s=true",
			name, strings.Join(replicas, ", "), kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	}
	// The same words on every pass: the status is written on each, and a
	// write starts the next pass.
	return heldBack("WaitingForMaster", fmt.Errorf("waiting for master %s to come back: no replica was in sync with it. To fail over anyway, losing the writes it acknowledged since that replica was last in sync, annotate the cluster %s=true",
		name, kividbv1alpha1.AllowUnsyncedFailoverAnnotation))
}

// aofOff reports whether the ready replicas of views but master, asked
// this pass, all say the append-only file is off. No answer counts as on.
func aofOff(views map[string]*podView, master string) bool {
	seen := false
	for name, v := range views {
		if name == master || !v.ready || v.status == nil {
			continue
		}
		if v.status.AofEnabled {
			return false
		}
		seen = true
	}
	return seen
}

// waitForRestart returns an error -- the failover is to wait -- while the
// master name, whose pod is gone or replaced and not ready yet (mv nil, or
// its replacement), may still come back on its volume; nil once the master
// is to be failed away from: the wait is off, nothing was recorded for it
// to tell whether its engine is still there, its engine still answers (the
// fence holds it), its replacement pod cannot be scheduled, or the wait is
// over -- on this pass or an earlier one, for this pod of the master.
func (r *KividbClusterReconciler) waitForRestart(ctx context.Context, c *kividbv1alpha1.KividbCluster, name string, mv *podView, last kividbv1alpha1.KividbPodStatus) error {
	key := c.Namespace + "/" + c.Name
	limit := restartWait(c)
	var w *restartWaiting
	if v, ok := r.restartWaits.Load(key); ok {
		if got := v.(*restartWaiting); got.master == name && got.incarnation == last.Incarnation {
			w = got
		}
	}
	if w != nil && w.over {
		return nil // over already: failing over, as then
	}
	// ended ends a wait under way, if there is one, saying why.
	ended := func(format string, args ...any) error {
		if w != nil {
			r.restartWaits.Delete(key)
			r.event(c, corev1.EventTypeNormal, "FailoverWait", format, args...)
		}
		return nil
	}
	if limit <= 0 {
		return ended("%s: no longer waited for: failover.restartWaitSeconds is 0; failing over", name)
	}
	if last.PodIP == "" || last.RunID == "" {
		// Nothing recorded (a kividbclusters CRD from before
		// status.pods[].podIP, or an agent before 0.7.0): whether its engine
		// is still there cannot be told, and waiting would only add
		// downtime. The fence says why it cannot hold it either.
		return ended("%s: no longer waited for: no address or engine run recorded for it; failing over", name)
	}
	if runID, err := r.runOf(ctx, last.PodIP); err == nil && runID == last.RunID {
		return ended("%s: no longer waited for: its engine answers at its address again, and its writes are held before the failover", name)
	}
	now := r.now()
	if w == nil {
		w = &restartWaiting{master: name, incarnation: last.Incarnation, since: now}
		r.restartWaits.Store(key, w)
		if mv != nil {
			if why := cannotRun(mv.pod); why != "" {
				w.over = true
				r.event(c, corev1.EventTypeWarning, "FailoverWait", "%s: not waiting for it to come back on its volume: its pod %s; failing over", name, why)
				return nil
			}
		}
		r.event(c, corev1.EventTypeWarning, "FailoverWait",
			"%s: its pod is gone and nothing answers at its address; waiting up to %s for it to come back on its volume, which may hold writes its replicas do not, before promoting one",
			name, limit)
	} else if mv != nil {
		if why := cannotRun(mv.pod); why != "" {
			w.over = true
			r.event(c, corev1.EventTypeWarning, "FailoverWait", "%s: no longer waited for: its pod %s; failing over", name, why)
			return nil
		}
	}
	if mv != nil && !mv.pod.CreationTimestamp.IsZero() && mv.pod.CreationTimestamp.Time.Before(w.since) {
		w.since = mv.pod.CreationTimestamp.Time // made before this operator saw it gone
	}
	if waited := now.Sub(w.since); waited < limit {
		return heldBack("WaitingForMaster", fmt.Errorf("waiting up to %s for master %s to come back on its volume (%s so far)", limit, name, waited.Round(time.Second)))
	}
	w.over = true
	if w.unsynced {
		r.event(c, corev1.EventTypeWarning, "FailoverWait", "%s did not come back on its volume within %s; failing over without a replica in sync with it, as %s is set",
			name, limit, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
		return nil
	}
	r.event(c, corev1.EventTypeWarning, "FailoverWait", "%s did not come back on its volume within %s; failing over", name, limit)
	return nil
}

// logRoles logs why a pass left the roles as they were. A wait it chose,
// for a master to come back, is logged on every pass while it lasts, so at
// info level: it is not a failure. Anything else is an error.
func logRoles(ctx context.Context, err error) {
	if held, ok := err.(*heldError); ok {
		logf.FromContext(ctx).Info("roles held back", "reason", held.reason, "detail", held.Error())
		return
	}
	logf.FromContext(ctx).Error(err, "role reconciliation failed")
}

// waitEnd is how a wait for a master to come back ended, outside
// waitForRestart.
type waitEnd int

const (
	// waitCameBack: its pod is back, ready, and has reported. It is
	// compared with its replicas next, as a master that restarted in place.
	waitCameBack waitEnd = iota
	// waitFailoverOff: failover.enabled is false now.
	waitFailoverOff
	// waitMasterChanged: another pod, or none, is the master now.
	waitMasterChanged
)

// restartWaitOver ends the wait for a master of c to come back, if one is
// under way, says how it ended, and returns it; master is the master now. A
// wait that was over already said so then, and is only forgotten.
func (r *KividbClusterReconciler) restartWaitOver(c *kividbv1alpha1.KividbCluster, end waitEnd, master string) *restartWaiting {
	v, ok := r.restartWaits.LoadAndDelete(c.Namespace + "/" + c.Name)
	if !ok || v.(*restartWaiting).over {
		return nil
	}
	w := v.(*restartWaiting)
	switch {
	case end == waitCameBack && master == w.master && w.unsynced && c.Annotations[kividbv1alpha1.AllowUnsyncedFailoverAnnotation] == "true":
		r.event(c, corev1.EventTypeNormal, "FailoverWait",
			"%s came back after %s; it is compared with its replicas before anything is promoted, and %s is removed: it was set for a failover that did not happen",
			w.master, r.now().Sub(w.since).Round(time.Second), kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	case end == waitCameBack && master == w.master:
		r.event(c, corev1.EventTypeNormal, "FailoverWait",
			"%s came back after %s; it is compared with its replicas, as a master that restarted in place, before anything is promoted",
			w.master, r.now().Sub(w.since).Round(time.Second))
	case w.unsynced && master != "" && master != w.master:
		r.event(c, corev1.EventTypeNormal, "FailoverWait", "%s: no longer waited for: the failover went ahead without a replica in sync with it, as %s was set; the master is now %s",
			w.master, kividbv1alpha1.AllowUnsyncedFailoverAnnotation, master)
	case end == waitFailoverOff:
		r.event(c, corev1.EventTypeNormal, "FailoverWait", "%s: no longer waited for: failover is disabled", w.master)
	case master == "":
		r.event(c, corev1.EventTypeNormal, "FailoverWait", "%s: no longer waited for: no master is recorded now", w.master)
	case master != w.master:
		r.event(c, corev1.EventTypeNormal, "FailoverWait", "%s: no longer waited for: the master is now %s", w.master, master)
	default:
		r.event(c, corev1.EventTypeNormal, "FailoverWait", "%s: no longer waited for", w.master)
	}
	return w
}

// cannotRun says why pod is not going to run soon, or "": no node will
// take it -- its volume pinned to a node that is not Ready, which the
// scheduler keeps new pods off, say.
func cannotRun(pod *corev1.Pod) string {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && cond.Reason == corev1.PodReasonUnschedulable {
			return fmt.Sprintf("cannot be scheduled (%s)", cond.Message)
		}
	}
	return ""
}
