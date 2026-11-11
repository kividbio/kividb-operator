package controller

import (
	"context"
	"fmt"
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
// by its volume shows as unschedulable, and the cap covers the rest. Each
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

// restartWaiting is a wait for a master to come back: which, and since when.
type restartWaiting struct {
	master string
	since  time.Time
}

// waitForRestart returns an error -- the failover is to wait -- while the
// master name, whose pod is gone or replaced and not ready yet (mv nil, or
// its replacement), may still come back on its volume; nil once the master
// is to be failed away from: the wait is off, its engine still answers (the
// fence holds it), its replacement pod cannot be scheduled, or the wait is
// over.
func (r *KividbClusterReconciler) waitForRestart(ctx context.Context, c *kividbv1alpha1.KividbCluster, name string, mv *podView, last kividbv1alpha1.KividbPodStatus) error {
	key := c.Namespace + "/" + c.Name
	limit := restartWait(c)
	stop := func(format string, args ...any) error {
		r.restartWaits.Delete(key)
		if format != "" {
			r.event(c, corev1.EventTypeWarning, "FailoverWait", format, args...)
		}
		return nil
	}
	if limit <= 0 {
		return stop("")
	}
	if last.PodIP != "" && last.RunID != "" {
		if runID, err := r.runOf(ctx, last.PodIP); err == nil && runID == last.RunID {
			return stop("") // still there: the fence holds it
		}
	}
	if mv != nil {
		if why := cannotRun(mv.pod); why != "" {
			return stop("%s: not waiting for it to come back on its volume: its pod %s; failing over", name, why)
		}
	}
	now := r.now()
	w := &restartWaiting{master: name, since: now}
	if v, ok := r.restartWaits.Load(key); ok && v.(*restartWaiting).master == name {
		w = v.(*restartWaiting)
	} else {
		r.restartWaits.Store(key, w)
		r.event(c, corev1.EventTypeWarning, "FailoverWait",
			"%s: its pod is gone and nothing answers at its address; waiting up to %s for it to come back on its volume, which may hold writes its replicas do not, before promoting one",
			name, limit)
	}
	if mv != nil && !mv.pod.CreationTimestamp.IsZero() && mv.pod.CreationTimestamp.Time.Before(w.since) {
		w.since = mv.pod.CreationTimestamp.Time // made before this operator saw it gone
	}
	if waited := now.Sub(w.since); waited < limit {
		return heldBack("WaitingForMaster", fmt.Errorf("waiting up to %s for master %s to come back on its volume (%s so far)", limit, name, waited.Round(time.Second)))
	}
	return stop("%s did not come back on its volume within %s; failing over", name, limit)
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

// restartWaitOver ends the wait for the master of c, if there is one: its
// pod is back, ready, and has reported. It is compared with its replicas
// next, as a master that restarted in place.
func (r *KividbClusterReconciler) restartWaitOver(c *kividbv1alpha1.KividbCluster) {
	if v, ok := r.restartWaits.LoadAndDelete(c.Namespace + "/" + c.Name); ok {
		w := v.(*restartWaiting)
		r.event(c, corev1.EventTypeNormal, "FailoverWait",
			"%s came back after %s; it is compared with its replicas, as a master that restarted in place, before anything is promoted",
			w.master, r.now().Sub(w.since).Round(time.Second))
	}
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
