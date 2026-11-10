package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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
// engine's SIGTERM; 68 in the AKS test.
//
// So before promoting, the operator holds that master's writes (CLIENT
// PAUSE WRITE through its agent) at the address its status entry last
// recorded, the pod object -- and with it the address -- being gone, and
// only if the engine answering there is still the one that entry recorded.
// It is a best effort, and a bounded one: the failover waits fenceTimeout
// at most and goes on whatever happened. A master that is gone needs no
// fence, and one that is cut off from the operator cannot be given one;
// neither may hold the failover up.

// fenceTimeout is how long a failover waits, at most, to hold the writes
// of the master it replaces. Its agent answers within milliseconds while
// its container still runs; an address nothing answers on costs this much,
// once.
const fenceTimeout = 100 * time.Millisecond

// fencePause is how long the fence holds the old master's writes: the
// pod's termination grace period, the longest its container can take to
// stop. An old master that comes back sooner is made a replica of the new
// one, its clients are disconnected and the pause is lifted
// (releaseDemotedMaster).
func fencePause(c *kividbv1alpha1.KividbCluster) time.Duration {
	return time.Duration(*gracePeriod(c)) * time.Second
}

// fenceOldMaster holds the writes of the master being failed away from,
// last being its entry in the last persisted status. It returns the
// address it holds them at, or "" when it did not (the failover goes on
// either way).
func (r *KividbClusterReconciler) fenceOldMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, last kividbv1alpha1.KividbPodStatus) string {
	log := logf.FromContext(ctx)
	if last.PodIP == "" || last.RunID == "" {
		// A kividbclusters CRD from before status.pods[].podIP drops the
		// field on every status write (Helm does not upgrade CRDs).
		r.event(c, corev1.EventTypeWarning, "FailoverUnfenced",
			"%s: no address or engine run recorded for it, so its writes were not held before the failover; apply the current kividbclusters CRD (Helm does not upgrade it)",
			last.Name)
		return ""
	}
	fctx, cancel := context.WithTimeout(ctx, fenceTimeout)
	defer cancel()
	// One command each for the run and the pause: under a CPU limit every
	// round trip to a loaded engine can take tens of milliseconds, and
	// GET /status makes seven.
	info, err := r.Agent.Exec(fctx, last.PodIP, "INFO", "server")
	if err == nil {
		if runID := infoField(info.Str, "run_id"); runID != last.RunID {
			log.Info("not holding the writes at the old master's address: another engine answers there",
				"pod", last.Name, "ip", last.PodIP, "runID", runID, "recordedRunID", last.RunID)
			return ""
		}
	}
	if err == nil {
		_, err = r.Agent.Exec(fctx, last.PodIP, "CLIENT", "PAUSE", strconv.FormatInt(fencePause(c).Milliseconds(), 10), "WRITE")
	}
	switch {
	case err == nil:
		log.Info("fenced the master being failed away from", "pod", last.Name, "ip", last.PodIP)
		return last.PodIP
	case errors.Is(fctx.Err(), context.DeadlineExceeded):
		log.Info("the fence timed out; failing over without it", "pod", last.Name, "ip", last.PodIP, "after", fenceTimeout)
	default:
		log.Info("could not hold the old master's writes; failing over without it", "pod", last.Name, "ip", last.PodIP, "error", err.Error())
	}
	return ""
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
