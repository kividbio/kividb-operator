package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// A planned switchover in a replicated (not sharded) cluster: the master
// role moves from a live master to one of its replicas without losing an
// acknowledged write. kividb has no FAILOVER command (it answers "ERR
// FAILOVER is not supported"), so the operator runs the steps Redis's
// FAILOVER runs itself:
//
//  1. CLIENT PAUSE <ms> WRITE on the old master: from here on it
//     acknowledges no write. Reads go on. The pause has a timeout of its
//     own, so an operator that dies half-way leaves a master that resumes
//     by itself.
//  2. Wait until the target's applied offset equals the old master's,
//     which no longer moves, with the target's link to it up. A write that
//     slipped past the pause check just as the pause began shows up as the
//     master's offset moving, and is waited for too.
//  3. REPLICAOF NO ONE on the target. It keeps the old master's offset and
//     replication ID, so the other replicas continue from it.
//  4. REPLICAOF <target> on the old master: it now refuses writes.
//  5. The master label moves: onto the target first, then off the old
//     master, so the master Service is never without an endpoint.
//  6. The other replicas are pointed at the target.
//  7. The old master's clients are disconnected (CLIENT KILL TYPE normal),
//     so they reconnect through the master Service to the new master
//     instead of being answered READONLY, and the pause is lifted.
//
// If the target does not catch up within switchoverCatchUpTimeout, nothing
// has changed but a few seconds of paused writes: the pause is lifted, the
// old master stays the master, and the attempt is reported.

var (
	// switchoverCatchUpTimeout bounds how long writes are held on the old
	// master while the target catches up. A replica in sync is milliseconds
	// behind; one that is not caught up in this long is not in sync.
	switchoverCatchUpTimeout = 5 * time.Second
	// switchoverPoll is how often the offsets are compared meanwhile.
	switchoverPoll = 20 * time.Millisecond
	// switchoverRetryAfter is how long after an aborted switchover the
	// next one is tried (unless the master is being deleted: then there is
	// no later). Every attempt holds writes for up to the catch-up timeout.
	switchoverRetryAfter = 30 * time.Second
)

// switchoverPauseMargin is how much longer than the catch-up timeout the
// pause lasts, for the promotion and the relabelling that follow. The
// pause is lifted as soon as they are done.
const switchoverPauseMargin = 5 * time.Second

// switchoverRequest says why a switchover is wanted, and to whom.
type switchoverRequest struct {
	// reason is for the event: "rollout", "step-down", "requested", "the
	// master pod is being deleted".
	reason string
	// preferred is the replica asked for (the switchover-to annotation);
	// "" picks the most caught-up replica in sync.
	preferred string
	// urgent skips the wait after an aborted attempt: the master is going
	// away whether or not the role moves.
	urgent bool
}

// replicationRole is what ROLE says about a pod.
type replicationRole struct {
	master bool
	offset int64
	host   string // a replica's master
	linkUp bool   // a replica's link to its master is up ("connected")
}

// roleOf asks the pod at ip for ROLE.
func (r *KividbClusterReconciler) roleOf(ctx context.Context, ip string) (replicationRole, error) {
	reply, err := r.Agent.Exec(ctx, ip, "ROLE")
	if err != nil {
		return replicationRole{}, err
	}
	return parseRole(reply)
}

// parseRole reads a ROLE reply: ["master", offset, [...]] or
// ["slave", host, port, state, offset].
func parseRole(reply *agentapi.ExecResponse) (replicationRole, error) {
	if reply == nil || len(reply.Array) == 0 {
		return replicationRole{}, fmt.Errorf("empty ROLE reply")
	}
	a := reply.Array
	switch a[0].Str {
	case "master":
		out := replicationRole{master: true}
		if len(a) > 1 {
			out.offset = a[1].Int
		}
		return out, nil
	case "slave", "replica":
		if len(a) < 5 {
			return replicationRole{}, fmt.Errorf("short ROLE reply from a replica")
		}
		return replicationRole{host: a[1].Str, linkUp: a[3].Str == "connected", offset: a[4].Int}, nil
	}
	return replicationRole{}, fmt.Errorf("ROLE: unknown role %q", a[0].Str)
}

// switchoverTarget picks the replica to hand over to: the preferred one if
// it is eligible, else the most caught-up eligible one. Eligible is a
// Ready replica, in sync, that is not being deleted.
func switchoverTarget(byName map[string]*corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod, preferred string) (*corev1.Pod, error) {
	var target *corev1.Pod
	var targetOffset int64
	for _, s := range statuses {
		p := byName[s.Name]
		if s.Name == masterPod || p == nil || p.DeletionTimestamp != nil || !s.Ready || !s.Synced || s.Role != kividbv1alpha1.RoleReplica {
			continue
		}
		if preferred != "" && s.Name == preferred {
			return p, nil
		}
		if target == nil || s.ReplicationOffset > targetOffset || (s.ReplicationOffset == targetOffset && s.Name < target.Name) {
			target, targetOffset = p, s.ReplicationOffset
		}
	}
	if preferred != "" {
		return nil, fmt.Errorf("%s is not a Ready replica in sync with %s", preferred, masterPod)
	}
	if target == nil {
		return nil, fmt.Errorf("no replica is in sync to take over from %s", masterPod)
	}
	return target, nil
}

// errSwitchoverDeferred: an earlier attempt was aborted a moment ago.
var errSwitchoverDeferred = errors.New("a switchover was aborted a moment ago; trying again later")

func switchoverKey(c *kividbv1alpha1.KividbCluster) string { return c.Namespace + "/" + c.Name }

// switchover hands the master role from masterPod to a replica in sync, as
// described at the top of this file, and returns the new master's name.
// When it returns an error, masterPod is still the master and accepts
// writes again.
func (r *KividbClusterReconciler) switchover(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod string, req switchoverRequest) (string, error) {
	log := logf.FromContext(ctx)
	byName := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		byName[pods[i].Name] = &pods[i]
	}
	old, ok := byName[masterPod]
	if !ok {
		return "", fmt.Errorf("master pod %s not found", masterPod)
	}
	target, err := switchoverTarget(byName, statuses, masterPod, req.preferred)
	if err != nil {
		return "", err
	}
	if at, ok := r.switchovers.Load(switchoverKey(c)); ok && !req.urgent && r.now().Sub(at.(time.Time)) < switchoverRetryAfter {
		return "", errSwitchoverDeferred
	}
	oldIP, targetIP, port := old.Status.PodIP, target.Status.PodIP, getPort(c)

	// The target must be following the old master, with its link up,
	// before writes are held for it.
	if t, err := r.roleOf(ctx, targetIP); err != nil {
		return "", fmt.Errorf("reading the role of %s: %w", target.Name, err)
	} else if t.master || t.host != oldIP || !t.linkUp {
		return "", fmt.Errorf("%s is not replicating from %s with its link up", target.Name, masterPod)
	}

	log.Info("switchover: pausing writes on the master until the replica has caught up", "from", masterPod, "to", target.Name, "reason", req.reason)
	pauseMs := strconv.FormatInt((switchoverCatchUpTimeout + switchoverPauseMargin).Milliseconds(), 10)
	paused := true
	if _, err := r.Agent.Exec(ctx, oldIP, "CLIENT", "PAUSE", pauseMs, "WRITE"); err != nil {
		var reply *agentapi.ExecResponse
		if reply, _ = r.Agent.Exec(ctx, oldIP, "PING"); reply == nil {
			return "", fmt.Errorf("pausing writes on %s: %w", masterPod, err)
		}
		// The engine is up and refuses the command: one from before
		// kividb 1.1, which has no CLIENT PAUSE ... WRITE. Hand over
		// without it, as earlier operators did, and say what that costs.
		paused = false
		r.event(c, corev1.EventTypeWarning, "SwitchoverWithoutPause",
			"%s refused CLIENT PAUSE ... WRITE (%v): its engine predates kividb 1.1. Moving the master role to %s without holding writes; a write acknowledged by %s during the next moments can be lost. Upgrade the engine to make switchovers lossless.",
			masterPod, err, target.Name, masterPod)
	}
	unpause := func() {
		if !paused {
			return
		}
		if _, err := r.Agent.Exec(ctx, oldIP, "CLIENT", "UNPAUSE"); err != nil {
			log.Error(err, "switchover: lifting the write pause (it ends by itself)", "pod", masterPod)
		}
	}
	abort := func(why string) (string, error) {
		unpause()
		r.switchovers.Store(switchoverKey(c), r.now())
		r.event(c, corev1.EventTypeWarning, "SwitchoverAborted",
			"did not move the master role from %s to %s (%s): %s; writes go on on %s, which stays the master",
			masterPod, target.Name, req.reason, why, masterPod)
		return "", fmt.Errorf("switchover from %s to %s aborted: %s", masterPod, target.Name, why)
	}

	started := time.Now()
	var offset int64
	if paused {
		caughtUp, at, why := r.waitCaughtUp(ctx, oldIP, targetIP)
		if !caughtUp {
			return abort(why)
		}
		offset = at
	}

	if err := r.Agent.Promote(ctx, targetIP); err != nil {
		return abort(fmt.Sprintf("promoting %s: %v", target.Name, err))
	}
	if err := r.Agent.ReplicaOf(ctx, oldIP, targetIP, port); err != nil {
		// Two masters now, the old one still paused and holding every
		// write: give the target its old role back.
		if rerr := r.Agent.ReplicaOf(ctx, targetIP, oldIP, port); rerr != nil {
			log.Error(rerr, "switchover: turning the target back into a replica", "pod", target.Name)
		}
		return abort(fmt.Sprintf("pointing %s at %s: %v", masterPod, target.Name, err))
	}
	r.switchovers.Delete(switchoverKey(c))

	// The target carries the label first: the master Service then always
	// has an endpoint, and the one it gains already takes writes.
	if err := r.setRoleLabel(ctx, target, kividbv1alpha1.RoleMaster); err != nil {
		log.Error(err, "switchover: labelling the new master (the next pass adopts it)", "pod", target.Name)
	}
	if err := r.setRoleLabel(ctx, old, kividbv1alpha1.RoleReplica); err != nil {
		log.Error(err, "switchover: relabelling the old master", "pod", masterPod)
	}
	for _, s := range statuses {
		p := byName[s.Name]
		if p == nil || p == old || p == target || p.DeletionTimestamp != nil || !s.Ready || p.Status.PodIP == "" {
			continue
		}
		if err := r.Agent.ReplicaOf(ctx, p.Status.PodIP, targetIP, port); err != nil {
			log.Error(err, "switchover: pointing a replica at the new master (the next pass retries)", "pod", p.Name)
		}
	}
	if paused {
		// Disconnected rather than answered READONLY: most clients do not
		// reconnect on READONLY, and would keep writing to a replica.
		if _, err := r.Agent.Exec(ctx, oldIP, "CLIENT", "KILL", "TYPE", "normal"); err != nil {
			log.V(1).Info("switchover: disconnecting the old master's clients", "pod", masterPod, "error", err.Error())
		}
	}
	unpause()
	// The status written after this pass was read before the role moved:
	// the old master's offset there is no reading of the new one.
	forgetMasterOffsets(statuses, masterPod, target.Name)

	if paused {
		r.event(c, corev1.EventTypeNormal, "Switchover",
			"moved the master role from %s to %s (%s); writes were held on %s for %s until %s had applied everything up to offset %d",
			masterPod, target.Name, req.reason, masterPod, time.Since(started).Round(time.Millisecond), target.Name, offset)
	} else {
		r.event(c, corev1.EventTypeNormal, "Switchover", "moved the master role from %s to %s (%s)", masterPod, target.Name, req.reason)
	}
	return target.Name, nil
}

// waitCaughtUp waits, for at most switchoverCatchUpTimeout, until the
// replica at targetIP has applied everything the (paused) master at oldIP
// has: the master's offset read before and after the replica's is the
// same, and the replica's is not below it, with its link up. It returns
// that offset, or why it gave up.
func (r *KividbClusterReconciler) waitCaughtUp(ctx context.Context, oldIP, targetIP string) (bool, int64, string) {
	deadline := time.Now().Add(switchoverCatchUpTimeout)
	why := "no answer yet"
	for {
		m1, err := r.roleOf(ctx, oldIP)
		switch {
		case err != nil:
			why = fmt.Sprintf("reading the master's offset: %v", err)
		case !m1.master:
			return false, 0, "the old master is no longer a master"
		default:
			t, terr := r.roleOf(ctx, targetIP)
			m2, merr := r.roleOf(ctx, oldIP)
			switch {
			case terr != nil:
				why = fmt.Sprintf("reading the replica's offset: %v", terr)
			case merr != nil:
				why = fmt.Sprintf("reading the master's offset: %v", merr)
			case t.master || t.host != oldIP:
				return false, 0, "the replica no longer replicates from the master"
			case !t.linkUp:
				why = "the replica's link to the master is down"
			case m1.offset != m2.offset:
				why = "the master's offset is still moving"
			case t.offset < m2.offset:
				why = fmt.Sprintf("the replica is at offset %d, the master at %d", t.offset, m2.offset)
			default:
				return true, m2.offset, ""
			}
		}
		if time.Now().After(deadline) {
			return false, 0, fmt.Sprintf("not caught up within %s (%s)", switchoverCatchUpTimeout, why)
		}
		select {
		case <-ctx.Done():
			return false, 0, ctx.Err().Error()
		case <-time.After(switchoverPoll):
		}
	}
}

// SwitchoverToAnnotation, on a KividbCluster, asks for the master role to
// move to the named pod (a replica in sync) with a lossless switchover.
// The operator removes it once that pod is the master, or when it cannot
// be (the pod does not exist); the GUI's promote action sets it.
const SwitchoverToAnnotation = "kividb.io/switchover-to"

// handoverReason says whether, and why, the master should hand its role
// over now: it is being deleted, a step-down was asked for on its pod, or
// a switchover to a given pod was asked for on the cluster.
func handoverReason(c *kividbv1alpha1.KividbCluster, master *corev1.Pod) (switchoverRequest, bool) {
	switch {
	case master.DeletionTimestamp != nil:
		return switchoverRequest{reason: "the master pod is being deleted", urgent: true}, true
	case c.Annotations[SwitchoverToAnnotation] != "" && c.Annotations[SwitchoverToAnnotation] != master.Name:
		return switchoverRequest{reason: "requested", preferred: c.Annotations[SwitchoverToAnnotation]}, true
	case master.Annotations[StepDownAnnotation] == "true":
		return switchoverRequest{reason: "step-down"}, true
	}
	return switchoverRequest{}, false
}
