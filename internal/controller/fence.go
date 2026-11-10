package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Holding the writes of the master a failover replaces (#53).
//
// A master whose pod is force-deleted is gone from the API at once, and
// the operator fails over within the second -- but its container runs on
// for a moment: its preStop hook holds the engine while it still has
// replicas attached, and only the promotion and the re-pointing of the
// others detach them. Until it stops it acknowledges the writes that come
// in on the connections clients already had to it, and every one of them
// is lost when a replica takes over: on kind, 689 to 954 per force-delete,
// all acknowledged in the 160-245 ms between the promotion and the
// engine's SIGTERM; 176 in the AKS test on engine 1.2.1.
//
// So before promoting, the operator holds that master's writes (CLIENT
// PAUSE WRITE through its agent) at the address its status entry last
// recorded, the pod object -- and with it the address -- being gone, and
// only if the engine answering there is still the one that entry recorded.
// Then it waits for the replica to have everything the master
// acknowledged before the pause, as a switchover does (waitCaughtUp),
// records the fenced master in the status (status.fencedMaster), and once
// the replica is promoted makes the old master its replica
// (demoteFencedMaster) -- on a later pass, should that fail, for the pod's
// replacement soon has its name.
//
// A master that is gone needs no fence, and one that is cut off from the
// operator cannot be given one: an address nothing answers on holds the
// failover up for fenceTimeout, once. An engine that has answered is
// there, and paused costs availability, not writes: its pause is sent
// again until it is answered (fenceRetryTimeout at most), and the replica
// is waited for while it answers (switchoverCatchUpTimeout at most). A
// failover that goes on without either says so (FailoverUnfenced).

// fenceTimeout is how long a failover waits for the master it replaces to
// answer at all, and for each try at holding its writes. Its agent answers
// within milliseconds while its container still runs, and within about
// 150 ms (p99) under a tight CPU limit and write load (250m for the
// engine, 100m for the agent).
const fenceTimeout = 250 * time.Millisecond

// fenceRetryTimeout is how long a failover goes on trying to hold the
// writes of a master that has answered with the run recorded for it.
var fenceRetryTimeout = 3 * time.Second

// fencePause is how long the fence holds the old master's writes, should
// it not be made the new master's replica right after the promotion: the
// pod's termination grace period, the longest its container can take to
// stop, and minFencePause at least -- CLIENT PAUSE 0 holds nothing. Each
// later try to make it a replica holds them as long again
// (demoteFencedMaster).
func fencePause(c *kividbv1alpha1.KividbCluster) time.Duration {
	return max(time.Duration(*gracePeriod(c))*time.Second, minFencePause)
}

const (
	// minFencePause is the shortest fence (terminationGracePeriodSeconds
	// may be 0).
	minFencePause = 10 * time.Second
	// fencedMasterRetry is how soon a pass that could not make the fenced
	// master a replica comes back to try again: well within its pause.
	fencedMasterRetry = 2 * time.Second
)

// fenceOldMaster holds the writes of the master being failed away from,
// last being its entry in the last persisted status, and waits for the
// replica cand to catch up with it. It returns the address it holds the
// writes at, or "" when it does not (the failover goes on either way).
func (r *KividbClusterReconciler) fenceOldMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, last kividbv1alpha1.KividbPodStatus, cand *podView) string {
	log := logf.FromContext(ctx).WithValues("pod", last.Name, "ip", last.PodIP)
	unfenced := func(why string, args ...any) string {
		msg := fmt.Sprintf(why, args...)
		log.Info("failing over without holding the old master's writes", "why", msg)
		r.event(c, corev1.EventTypeWarning, "FailoverUnfenced",
			"%s: %s; writes it acknowledged after this may be lost", last.Name, msg)
		return ""
	}
	if last.PodIP == "" || last.RunID == "" {
		// A kividbclusters CRD from before status.pods[].podIP drops the
		// field on every status write (Helm does not upgrade CRDs).
		return unfenced("no address or engine run recorded for it, so its writes were not held before the failover; apply the current kividbclusters CRD (Helm does not upgrade it)")
	}

	// One command each for the run and the pause: under a CPU limit every
	// round trip to a loaded engine can take tens of milliseconds, and
	// GET /status makes seven.
	ictx, cancel := context.WithTimeout(ctx, fenceTimeout)
	info, err := r.Agent.Exec(ictx, last.PodIP, "INFO", "server")
	cancel()
	if err != nil {
		return unfenced("nothing answered at %s within %s (%v)", last.PodIP, fenceTimeout, err)
	}
	if runID := infoField(info.Str, "run_id"); runID != last.RunID {
		log.Info("not holding the writes at the old master's address: another engine answers there", "runID", runID, "recordedRunID", last.RunID)
		return ""
	}

	// It is there. A pause that is not answered may have landed all the
	// same: send it again (it cannot get shorter), until it is answered or
	// refused.
	pauseMs := strconv.FormatInt(fencePause(c).Milliseconds(), 10)
	deadline := time.Now().Add(fenceRetryTimeout)
	for {
		pctx, cancel := context.WithTimeout(ctx, fenceTimeout)
		reply, err := r.Agent.Exec(pctx, last.PodIP, "CLIENT", "PAUSE", pauseMs, "WRITE")
		cancel()
		if err == nil {
			break
		}
		if reply != nil {
			return unfenced("its engine refused CLIENT PAUSE WRITE (%v)", err)
		}
		if time.Now().After(deadline) {
			return unfenced("its engine answered, but no pause of its writes was within %s (%v)", fenceRetryTimeout, err)
		}
		log.Info("holding the old master's writes went unanswered; trying again", "error", err.Error())
		select {
		case <-ctx.Done():
			return unfenced("%v", ctx.Err())
		case <-time.After(switchoverPoll):
		}
	}
	log.Info("fenced the master being failed away from")

	if fm := c.Status.FencedMaster; fm != nil && *fm == (kividbv1alpha1.FencedMaster{Name: last.Name, PodIP: last.PodIP, RunID: last.RunID}) &&
		cand.status != nil && cand.status.Role == agentapi.RoleMaster {
		// An earlier pass fenced it and promoted this replica, the reply of
		// which was lost: it has waited for the replica already.
		return last.PodIP
	}
	if ok, _, why := r.waitCaughtUp(ctx, last.PodIP, cand.pod.Status.PodIP, true); !ok {
		log.Info("promoting a replica that has not caught up with the fenced master", "replica", cand.pod.Name, "why", why)
		r.event(c, corev1.EventTypeWarning, "FailoverUnfenced",
			"%s: promoting %s before it caught up with it (%s); writes %s acknowledged that %s does not have are lost",
			last.Name, cand.pod.Name, why, last.Name, cand.pod.Name)
	}
	return last.PodIP
}

// recordFence persists, before the promotion, which master the failover
// holds the writes of (status.fencedMaster), so that it is made the new
// master's replica even if that cannot be done right after the promotion,
// or the operator restarts in between. Only that field is written; c keeps
// the rest of this pass's status for updateStatus to write.
func (r *KividbClusterReconciler) recordFence(ctx context.Context, c *kividbv1alpha1.KividbCluster, fm kividbv1alpha1.FencedMaster) error {
	patched := c.DeepCopy()
	patched.Status.FencedMaster = &fm
	err := r.Status().Patch(ctx, patched, client.MergeFromWithOptions(c, client.MergeFromWithOptimisticLock{}))
	switch {
	case err == nil:
		// Nothing was written since c was read: updateStatus goes on from
		// the version this made.
		c.ResourceVersion = patched.ResourceVersion
	case apierrors.IsConflict(err):
		// c was read before an overlapping pass's status write (the cache
		// lags behind the API server during a failover). Write the field
		// over the newer status, and leave c's version as it was:
		// updateStatus then fails on it, as it would have, rather than
		// write this pass's status over the newer one.
		patched = c.DeepCopy()
		patched.Status.FencedMaster = &fm
		if err := r.Status().Patch(ctx, patched, client.MergeFrom(c)); err != nil {
			return err
		}
	default:
		return err
	}
	c.Status.FencedMaster = &fm
	return nil
}

// demoteFencedMaster makes the master a failover holds the writes of
// (status.fencedMaster) a replica of the master at masterIP, disconnects
// its clients and lifts the pause, and forgets it. Each try holds its
// writes as long again first: the pause cannot get shorter. It is tried
// right after the promotion and on every pass after that, until it is
// done or the engine there is gone -- nothing, or another engine, answers
// at its address. It returns the address of the master it demoted, or "".
func (r *KividbClusterReconciler) demoteFencedMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, masterIP string, port int32) string {
	fm := c.Status.FencedMaster
	if fm == nil || masterIP == "" {
		return ""
	}
	log := logf.FromContext(ctx).WithValues("pod", fm.Name, "ip", fm.PodIP)
	if fm.PodIP == masterIP {
		// The master answers there: it is not a master to hold.
		c.Status.FencedMaster = nil
		return ""
	}
	info, err := r.Agent.Exec(ctx, fm.PodIP, "INFO", "server")
	if err != nil {
		log.Info("the fenced master no longer answers; nothing left to hold", "error", err.Error())
		c.Status.FencedMaster = nil
		return ""
	}
	if runID := infoField(info.Str, "run_id"); runID != fm.RunID {
		log.Info("another engine answers at the fenced master's address; nothing left to hold", "runID", runID, "fencedRunID", fm.RunID)
		c.Status.FencedMaster = nil
		return ""
	}
	if _, err := r.Agent.Exec(ctx, fm.PodIP, "CLIENT", "PAUSE", strconv.FormatInt(fencePause(c).Milliseconds(), 10), "WRITE"); err != nil {
		log.Info("could not hold the fenced master's writes again", "error", err.Error())
	}
	if err := r.Agent.ReplicaOf(ctx, fm.PodIP, masterIP, port); err != nil {
		log.Info("could not make the fenced master a replica of the new master; trying again", "error", err.Error(), "in", fencedMasterRetry)
		return ""
	}
	r.releaseDemotedMaster(ctx, fm.PodIP)
	log.Info("made the fenced master a replica of the new master and released it")
	c.Status.FencedMaster = nil
	return fm.PodIP
}

// releaseDemotedMaster lets go of a master that has just been made a
// replica -- the old master of a failover, come back, or one that
// restarted behind its replicas -- which may still be holding writes
// (fenceOldMaster, pauseWrites). Its clients are disconnected first, so
// that they reconnect through the master Service and the writes it held
// fail on the dropped connections instead of landing; then the pause is
// lifted. On an engine that was not paused, UNPAUSE does nothing.
func (r *KividbClusterReconciler) releaseDemotedMaster(ctx context.Context, ip string) {
	if _, err := r.Agent.Exec(ctx, ip, "CLIENT", "KILL", "TYPE", "normal"); err != nil {
		logf.FromContext(ctx).V(1).Info("disconnecting a demoted master's clients", "ip", ip, "error", err.Error())
	}
	r.unpauseWrites(ctx, ip)
}

// infoField is the value of key in an INFO reply ("key:value" lines), or
// "".
func infoField(info, key string) string {
	for line := range strings.SplitSeq(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+":"); ok {
			return v
		}
	}
	return ""
}
