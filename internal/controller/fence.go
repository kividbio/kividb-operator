package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
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
// operator cannot be given one: an address where nothing listens is asked
// once, and one that does not answer is asked again for
// fenceRunCheckTimeout (a slow engine is there). An engine that has
// answered is there, and paused costs availability, not writes: its pause
// is sent again until it is answered (fenceRetryTimeout at most), and the
// replica is waited for while it answers (switchoverCatchUpTimeout at
// most). A failover that goes on without either says so
// (FailoverUnfenced).

// fenceTimeout is how long each request to the master a failover replaces
// may take. Its agent answers within milliseconds while its container
// still runs, and within about 150 ms (p99) under a tight CPU limit and
// write load (250m for the engine, 100m for the agent).
const fenceTimeout = 250 * time.Millisecond

// fenceRunCheckTimeout is how long a failover goes on asking an address
// that does not answer (a request that times out, not one refused) which
// engine runs there.
var fenceRunCheckTimeout = time.Second

// fenceRetryTimeout is how long a failover goes on trying to hold the
// writes of a master that has answered with the run recorded for it: no new
// try starts after it.
var fenceRetryTimeout = 3 * time.Second

// pauseCallTimeout is how long a CLIENT PAUSE call may take (the agent is
// asked for a little less, agentWait), or the agent client's per-call
// timeout if that is longer. Since kividb 1.2.1 the engine waits out the
// writes in progress before it answers, up to 5 s, and refuses after that
// (kividbio/kividb#629): longer than that wait, and than the agent's and
// the agent client's usual 5 s.
var pauseCallTimeout = 7 * time.Second

// fencePause is how long the fence holds the old master's writes, should
// it not be made the new master's replica right after the promotion: the
// pod's termination grace period, the longest its container can take to
// stop -- minFencePause at least, CLIENT PAUSE 0 holding nothing -- and
// fencePauseMargin past it, for the kubelet gives a preStop hook 2 s more.
// Each later try to make it a replica holds them as long again
// (demoteFencedMaster).
func fencePause(c *kividbv1alpha1.KividbCluster) time.Duration {
	return max(time.Duration(*gracePeriod(c))*time.Second, minFencePause) + fencePauseMargin
}

const (
	// minFencePause is the shortest fence before its margin
	// (terminationGracePeriodSeconds may be 0).
	minFencePause = 10 * time.Second
	// fencePauseMargin is how long the fence outlasts the pod's grace
	// period.
	fencePauseMargin = 5 * time.Second
	// fencedMasterRetry is how soon a pass that could not make the fenced
	// master a replica comes back to try again: well within its pause.
	fencedMasterRetry = 2 * time.Second
	// fencedMasterMisses is how many passes in a row the fenced master's
	// address must not answer before it is taken for gone: one may be a
	// moment's trouble.
	fencedMasterMisses = 2
)

// fenceOldMaster holds the writes of the master being failed away from,
// last being its entry in the last persisted status, has pick choose the
// replica to promote -- with the old master held, the replicas' offsets
// stop moving (#78) -- and waits for that replica to catch up with it. It
// returns the address it holds the writes at, or may hold them at, or ""
// when it does not (the failover goes on either way). pick is called only
// once a pause was answered; the caller picks otherwise.
func (r *KividbClusterReconciler) fenceOldMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, last kividbv1alpha1.KividbPodStatus, pick func() *podView) (string, bool) {
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
		return unfenced("no address or engine run recorded for it, so its writes were not held before the failover; apply the current kividbclusters CRD (Helm does not upgrade it)"), false
	}

	// One command each for the run and the pause: under a CPU limit every
	// round trip to a loaded engine can take tens of milliseconds, and
	// GET /status makes seven.
	runID, err := r.runOf(ctx, last.PodIP)
	if err != nil {
		return unfenced("no answer at %s (%v)", last.PodIP, err), false
	}
	if runID != last.RunID {
		log.Info("not holding the writes at the old master's address: another engine answers there", "runID", runID, "recordedRunID", last.RunID)
		return "", false
	}

	// It is there. A pause that is not answered may have landed all the
	// same, and one that could not take hold yet (a write in progress, see
	// pauseCallTimeout) may next time: send it again (it cannot get
	// shorter), until it is answered, or refused by an engine without it.
	// Each try may take pauseCallTimeout, for the engine waits out the
	// writes in progress before it answers.
	// Every try is given the whole call: the engine may take its 5 s
	// before it answers, and a pause cut short on this side may still land
	// on the engine. fenceRetryTimeout, counted from the end of the first
	// try (a refusal comes back only after the engine's wait), only says
	// whether a new try may start: two calls and the window at most, 17 s
	// with the default timeouts (#82).
	pauseMs := strconv.FormatInt(fencePause(c).Milliseconds(), 10)
	var deadline time.Time
	unanswered := false
	for {
		reply, err := r.Agent.ExecWithin(ctx, pauseCallTimeout, last.PodIP, "CLIENT", "PAUSE", pauseMs, "WRITE")
		if err == nil {
			break
		}
		if reply != nil && pauseUnsupported(err) {
			return unfenced("its engine does not have CLIENT PAUSE WRITE (%v)", err), false
		}
		unanswered = unanswered || reply == nil
		if deadline.IsZero() {
			deadline = time.Now().Add(fenceRetryTimeout)
		} else if !time.Now().Before(deadline) {
			if unanswered {
				// A try that got no answer may have landed, once the write
				// it waited for finished: the master is recorded and
				// demoted as a fenced one, rather than left to wake up a
				// master when that pause runs out (#82).
				unfenced("its engine answered, but no pause of its writes was confirmed (%v); it is demoted after the promotion in case one landed", err)
				return last.PodIP, false
			}
			return unfenced("its engine answered, but its writes could not be paused (%v)", err), false
		}
		log.Info("holding the old master's writes did not take; trying again", "error", err.Error())
		select {
		case <-ctx.Done():
			return unfenced("%v", ctx.Err()), false
		case <-time.After(switchoverPoll):
		}
	}
	log.Info("fenced the master being failed away from")

	cand := pick()
	if fm := c.Status.FencedMaster; fm != nil && *fm == (kividbv1alpha1.FencedMaster{Name: last.Name, PodIP: last.PodIP, RunID: last.RunID}) &&
		cand.status != nil && cand.status.Role == agentapi.RoleMaster {
		// An earlier pass fenced it and promoted this replica, the reply of
		// which was lost: it has waited for the replica already.
		return last.PodIP, true
	}
	ok, _, why := r.waitCaughtUp(ctx, last.PodIP, cand.pod.Status.PodIP, true)
	if !ok {
		log.Info("promoting a replica that has not caught up with the fenced master", "replica", cand.pod.Name, "why", why)
		r.event(c, corev1.EventTypeWarning, "FailoverUnfenced",
			"%s: promoting %s before it caught up with it (%s); writes %s acknowledged that %s does not have are lost",
			last.Name, cand.pod.Name, why, last.Name, cand.pod.Name)
	}
	return last.PodIP, ok
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
// (status.fencedMaster) a replica of the master at masterIP -- a ready
// pod that reports itself master -- disconnects its clients and lifts the
// pause, and forgets it. Each try holds its writes as long again first:
// the pause cannot get shorter. It is tried right after the promotion and
// on every pass after that, until it is done or the engine there is gone:
// another engine answers at its address, or nothing does on
// fencedMasterMisses passes in a row. It returns the address of the
// master it demoted, or "".
func (r *KividbClusterReconciler) demoteFencedMaster(ctx context.Context, c *kividbv1alpha1.KividbCluster, masterIP string, port int32) string {
	fm := c.Status.FencedMaster
	if fm == nil || masterIP == "" {
		return ""
	}
	log := logf.FromContext(ctx).WithValues("pod", fm.Name, "ip", fm.PodIP)
	key := c.Namespace + "/" + c.Name
	tries := r.fencedTries(key, *fm)
	forget := func() {
		c.Status.FencedMaster = nil
		r.fencedRetries.Delete(key)
	}
	if fm.PodIP == masterIP {
		// The master answers there: it is not a master to hold.
		forget()
		return ""
	}
	runID, err := r.runOf(ctx, fm.PodIP)
	if err != nil {
		if tries.misses++; tries.misses < fencedMasterMisses {
			log.Info("the fenced master did not answer; asking again", "error", err.Error(), "in", fencedMasterRetry)
			return ""
		}
		log.Info("the fenced master no longer answers; nothing left to hold", "error", err.Error())
		forget()
		return ""
	}
	tries.misses = 0
	if runID != fm.RunID {
		log.Info("another engine answers at the fenced master's address; nothing left to hold", "runID", runID, "fencedRunID", fm.RunID)
		forget()
		return ""
	}
	// The engine may wait out a write in progress before it answers, as for
	// the fence's own pause.
	if _, err := r.Agent.ExecWithin(ctx, pauseCallTimeout, fm.PodIP, "CLIENT", "PAUSE", strconv.FormatInt(fencePause(c).Milliseconds(), 10), "WRITE"); err != nil {
		log.Info("could not hold the fenced master's writes again", "error", err.Error())
	}
	if err := r.Agent.ReplicaOf(ctx, fm.PodIP, masterIP, port); err != nil {
		log.Info("could not make the fenced master a replica of the new master; trying again", "error", err.Error(), "in", fencedMasterRetry)
		if !tries.warned {
			// Once: the retries show in the log.
			tries.warned = true
			r.event(c, corev1.EventTypeWarning, "FencedMasterNotDemoted",
				"%s: could not make it a replica of the new master (%v); its writes stay held, and this is tried again every %s",
				fm.Name, err, fencedMasterRetry)
		}
		return ""
	}
	r.releaseDemotedMaster(ctx, fm.PodIP)
	log.Info("made the fenced master a replica of the new master and released it")
	forget()
	return fm.PodIP
}

// fencedTry is how the demotion of a cluster's fenced master has gone so
// far, kept in memory: an operator restart only costs one more check, or
// one more event.
type fencedTry struct {
	fm     kividbv1alpha1.FencedMaster
	misses int  // passes in a row its address did not answer
	warned bool // FencedMasterNotDemoted raised
}

// fencedTries is the fencedTry of fm, the fenced master of the cluster at
// key: a new one for a master fenced since.
func (r *KividbClusterReconciler) fencedTries(key string, fm kividbv1alpha1.FencedMaster) *fencedTry {
	if v, ok := r.fencedRetries.Load(key); ok {
		if t := v.(*fencedTry); t.fm == fm {
			return t
		}
	}
	t := &fencedTry{fm: fm}
	r.fencedRetries.Store(key, t)
	return t
}

// runOf is the run ID of the engine at ip (INFO server). A request that
// times out is sent again for fenceRunCheckTimeout -- a slow engine is
// there -- but not one that is refused or finds no route: nothing is.
func (r *KividbClusterReconciler) runOf(ctx context.Context, ip string) (string, error) {
	deadline := time.Now().Add(fenceRunCheckTimeout)
	for {
		actx, cancel := context.WithTimeout(ctx, fenceTimeout)
		info, err := r.Agent.Exec(actx, ip, "INFO", "server")
		cancel()
		if err == nil {
			return infoField(info.Str, "run_id"), nil
		}
		if info != nil || !isTimeout(err) || ctx.Err() != nil || time.Now().After(deadline) {
			return "", err
		}
	}
}

// isTimeout reports whether err is a request that got no answer in time.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// pauseUnsupported reports whether err, an engine's refusal of CLIENT PAUSE
// ... WRITE, says it does not know the command. Every kividb shipped has it,
// so any other refusal -- since kividb 1.2.1 one for a write in progress
// that did not finish (kividbio/kividb#629), LOADING, BUSY, NOPERM -- is
// one the engine may not repeat, and is not taken for that (#82).
func pauseUnsupported(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown command") || strings.Contains(msg, "unknown subcommand") ||
		strings.Contains(msg, "unknown client subcommand") // kividb 1.0.0
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
