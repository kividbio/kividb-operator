package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const defaultUnhealthyThreshold = 30 * time.Second

// podView is the controller's per-pod working state for a single reconcile
// pass, merging the live Pod object with its agent-reported status.
type podView struct {
	pod    *corev1.Pod
	ready  bool
	status *agentapi.StatusResponse // nil if unreachable/not-yet-queried
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func readyFalseSince(pod *corev1.Pod) (time.Time, bool) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status != corev1.ConditionTrue {
			return cond.LastTransitionTime.Time, true
		}
	}
	return time.Time{}, false
}

// reconcileRoles is the heart of automatic failover. It:
//  1. Queries every ready pod's agent for its current replication role/offset.
//  2. Confirms (or elects, on first bootstrap) the master.
//  3. Detects a dead/unready master past the configured threshold and
//     promotes the most caught-up replica in its place.
//  4. Ensures every non-master ready pod is REPLICAOF'd to the current
//     master and labeled accordingly.
//
// It returns the per-pod status list, the current master pod name, and
// whether a failover was performed on this pass (used by the caller to
// stamp status.LastFailoverTime).
func (r *KividbClusterReconciler) reconcileRoles(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod) ([]kividbv1alpha1.KividbPodStatus, string, bool, error) {
	log := logf.FromContext(ctx)
	port := getPort(c)

	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	views := make(map[string]*podView, len(pods))
	for i := range pods {
		p := &pods[i]
		v := &podView{pod: p, ready: isPodReady(p)}
		if v.ready {
			if st, err := r.Agent.Status(ctx, p.Status.PodIP); err != nil {
				log.V(1).Info("agent status query failed", "pod", p.Name, "error", err.Error())
			} else {
				v.status = st
			}
		}
		views[p.Name] = v
	}

	currentMasterName := resolveCurrentMaster(pods, views, c.Status.MasterPod)

	previouslySynced := make(map[string]bool, len(c.Status.Pods))
	previous := make(map[string]kividbv1alpha1.KividbPodStatus, len(c.Status.Pods))
	var masterKeysLastSeen int64
	for _, ps := range c.Status.Pods {
		previouslySynced[ps.Name] = ps.Synced
		previous[ps.Name] = ps
		if ps.Name == currentMasterName {
			masterKeysLastSeen = ps.Keys
		}
	}

	threshold := defaultUnhealthyThreshold
	if c.Spec.Failover.UnhealthyThresholdSeconds != nil {
		threshold = time.Duration(*c.Spec.Failover.UnhealthyThresholdSeconds) * time.Second
	}
	failoverEnabled := boolOr(c.Spec.Failover.Enabled, true)

	needsElection := currentMasterName == ""
	needsFailover := false
	failoverHappened := false
	newMasterName := currentMasterName

	if currentMasterName != "" {
		mv, exists := views[currentMasterName]
		switch {
		case !exists:
			needsFailover = failoverEnabled // labeled master pod is gone entirely
		case mv.ready && mv.status != nil && mv.status.Role == agentapi.RoleReplica:
			// The pod the operator considers master is healthy but kividb
			// itself says it is a replica: something demoted it behind the
			// operator's back (a manual REPLICAOF, the GUI's promote
			// action). Left alone this is a silent outage -- the master
			// Service keeps selecting a read-only pod, or worse, the loop
			// below REPLICAOFs the real master at it and builds a
			// replication cycle with no master at all.
			if actual := actualMasterOf(views, currentMasterName); actual != "" {
				// It is cleanly replicating from another pod of this
				// cluster that really is a master: that pod has the data,
				// so follow the topology instead of fighting it.
				log.Info("adopting pod as master: labeled master is replicating from it", "pod", actual, "previousMaster", currentMasterName)
				r.event(c, corev1.EventTypeNormal, "MasterAdopted",
					"%s is now a replica of %s; treating %s as the master", currentMasterName, actual, actual)
				newMasterName = actual
			} else {
				log.Info("re-promoting master: it reports role replica but no other pod is its master", "pod", currentMasterName)
				r.event(c, corev1.EventTypeWarning, "MasterRepromoted",
					"%s was labeled master but reported role replica with no valid master; promoted it again", currentMasterName)
				if err := r.Agent.Promote(ctx, mv.pod.Status.PodIP); err != nil {
					return nil, currentMasterName, false, fmt.Errorf("re-promoting %s: %w", currentMasterName, err)
				}
				// No stream ID: what the pod reports now is no baseline
				// for regressedMaster, the next pass's reading is.
				mv.status = &agentapi.StatusResponse{Role: agentapi.RoleMaster, ReplicationOffset: mv.status.ReplicationOffset, RunID: mv.status.RunID}
			}
		case !mv.ready || mv.status == nil || mv.status.Role != agentapi.RoleMaster:
			if since, unready := readyFalseSince(mv.pod); unready {
				needsFailover = failoverEnabled && time.Since(since) >= threshold
			} else if !mv.ready {
				needsFailover = failoverEnabled
			}
		}
	}

	// A master that restarted and came back with less than its replicas
	// hold -- from a snapshot older than its last writes -- must not have
	// them resync from it: that discards, on every copy, the writes it had
	// acknowledged since that snapshot. The replica that holds the most
	// takes over instead, and the restarted pod becomes its replica.
	behindOf := ""
	if !needsElection && !needsFailover && failoverEnabled {
		if mv, ok := views[currentMasterName]; ok && mv.ready && mv.status != nil && mv.status.Role == agentapi.RoleMaster {
			if ahead, aheadOffset := regressedMaster(previous[currentMasterName], mv.pod, mv.status, views, previouslySynced, port); ahead != "" {
				log.Info("the master restarted behind its replicas; failing over to the replica that holds the most", "pod", currentMasterName,
					"masterOffset", mv.status.ReplicationOffset, "lastSeenOffset", previous[currentMasterName].ReplicationOffset, "replica", ahead, "replicaOffset", aheadOffset)
				r.event(c, corev1.EventTypeWarning, "MasterRestartedBehind",
					"%s restarted and came back as the master at replication offset %d, behind the %d it had reached and behind %s (offset %d), which holds writes it lost; failing over rather than resyncing the replicas from its older data",
					currentMasterName, mv.status.ReplicationOffset, previous[currentMasterName].ReplicationOffset, ahead, aheadOffset)
				needsFailover = true
				behindOf = ahead
			}
		}
	}

	if needsElection || needsFailover {
		// A replica can be Ready, connected and "in sync" by every other
		// measure and still hold nothing: kividb drops a replica's dataset
		// when a full resync fails part-way, which is what happens to
		// every replica at once when the master is crash-looping (it dies
		// again mid-transfer). Promoting one of those turns a master
		// outage into total data loss -- the old master still has the
		// data on disk, and would be made to resync from the empty one
		// the moment it came back. So when the master was last seen
		// holding data, only replicas that still hold some are eligible,
		// and if none does the failover waits for the master.
		eligible := views
		if needsFailover && masterKeysLastSeen > 0 && c.Annotations[kividbv1alpha1.AllowEmptyFailoverAnnotation] != "true" {
			eligible = withoutEmptied(views)
			if electReplica(eligible, currentMasterName) == "" && electReplica(views, currentMasterName) != "" {
				r.event(c, corev1.EventTypeWarning, "FailoverBlocked",
					"master %s is unavailable, but no ready replica holds any data (the master last reported %d keys); waiting for it to return rather than promoting an empty replica. Set the %s annotation to \"true\" to fail over anyway.",
					currentMasterName, masterKeysLastSeen, kividbv1alpha1.AllowEmptyFailoverAnnotation)
				return nil, currentMasterName, false, fmt.Errorf("failover blocked: no ready replica holds any data, master %s last reported %d keys", currentMasterName, masterKeysLastSeen)
			}
		}

		// Prefer a replica that was in sync the last time that could be
		// established. Offsets alone do not rule out a replica that had
		// only just started resyncing when the master went away.
		candidate := electReplica(onlySynced(eligible, previouslySynced), currentMasterName)
		if candidate == "" {
			candidate = electReplica(eligible, currentMasterName)
		}
		if _, ok := eligible[behindOf]; ok {
			candidate = behindOf // the replica that holds what the restarted master lost
		}
		if seeded := bootstrapSeededPod(c); needsElection && seeded != "" {
			// The very first election of a cluster bootstrapped from a
			// snapshot. Only one pod holds the restored data, and it is
			// the slowest to become Ready because it has to load it; the
			// others start empty and are Ready at once. Electing any of
			// those would make the seeded pod its replica and wipe the
			// snapshot it was just given.
			if v, ok := views[seeded]; !ok || !v.ready {
				return nil, currentMasterName, false, fmt.Errorf("waiting for %s, which holds the restored snapshot, to become ready before electing a master", seeded)
			}
			candidate = seeded
		}
		if candidate == "" {
			return nil, currentMasterName, false, fmt.Errorf("no ready pod available to elect as master")
		}
		log.Info("promoting pod to master", "pod", candidate, "reason", map[bool]string{true: "failover", false: "bootstrap"}[needsFailover])

		// Take the master label off the pod being failed away from *before*
		// promoting its replacement. That pod is usually still there (hung,
		// partitioned, crash-looping) and will come back believing it is a
		// master; if it still carried the label it would rejoin the master
		// Service next to the new master, and the next reconcile could pick
		// it as "the" master again and REPLICAOF the real one at it,
		// discarding every write taken since the failover.
		//
		// A master that restarted behind its replicas is still up and
		// taking writes, every one of which its resync from the new
		// master will discard. Hold them, as a planned switchover does,
		// from before the promotion until it is the new master's replica
		// and its clients have been sent there.
		fenced := ""
		if old, ok := views[currentMasterName]; ok && behindOf != "" {
			fenced = r.pauseWrites(ctx, old.pod, restartedMasterPause)
		}
		if old, ok := views[currentMasterName]; ok {
			if err := r.setRoleLabel(ctx, old.pod, kividbv1alpha1.RoleReplica); err != nil {
				r.unpauseWrites(ctx, fenced)
				return nil, currentMasterName, false, fmt.Errorf("demoting %s: %w", currentMasterName, err)
			}
		}
		candidateIP := views[candidate].pod.Status.PodIP
		if err := r.Agent.Promote(ctx, candidateIP); err != nil {
			r.unpauseWrites(ctx, fenced)
			return nil, currentMasterName, false, fmt.Errorf("promoting %s: %w", candidate, err)
		}
		if err := r.setRoleLabel(ctx, views[candidate].pod, kividbv1alpha1.RoleMaster); err != nil {
			return nil, currentMasterName, false, err
		}
		if fenced != "" {
			// Its pause lasts restartedMasterPause: if this fails, it ends
			// by itself, and the loop below asks again.
			old := views[currentMasterName]
			if err := r.Agent.ReplicaOf(ctx, fenced, candidateIP, port); err != nil {
				log.Error(err, "pointing the restarted master at the new master", "pod", currentMasterName)
			} else {
				old.status = &agentapi.StatusResponse{Role: agentapi.RoleReplica, MasterHost: candidateIP, MasterPort: port,
					KeyCount: old.status.KeyCount, KeyCountKnown: old.status.KeyCountKnown, RunID: old.status.RunID}
				if _, err := r.Agent.Exec(ctx, fenced, "CLIENT", "KILL", "TYPE", "normal"); err != nil {
					log.V(1).Info("disconnecting the restarted master's clients", "pod", currentMasterName, "error", err.Error())
				}
				r.unpauseWrites(ctx, fenced)
			}
		}
		switch {
		case behindOf != "":
			r.event(c, corev1.EventTypeWarning, "Failover", "master %s restarted behind its replicas; promoted %s", currentMasterName, candidate)
		case needsFailover:
			r.event(c, corev1.EventTypeWarning, "Failover", "master %s is unavailable; promoted %s", currentMasterName, candidate)
		}
		newMasterName = candidate
		failoverHappened = needsFailover
		// Refresh our view of the new master so downstream REPLICAOF checks
		// below see it as master rather than stale replica state.
		views[candidate].status = &agentapi.StatusResponse{Role: agentapi.RoleMaster}
	}

	var masterIP string
	if v, ok := views[newMasterName]; ok {
		masterIP = v.pod.Status.PodIP
	}

	var masterStatus *agentapi.StatusResponse
	if v, ok := views[newMasterName]; ok && v.ready {
		masterStatus = v.status
	}

	statuses := make([]kividbv1alpha1.KividbPodStatus, 0, len(pods))
	for _, p := range pods {
		v := views[p.Name]
		role := kividbv1alpha1.RoleUnknown
		// Last observed: a pod that cannot be asked keeps what it last
		// reported. A master's offset, stream and run are how its restart
		// behind its replicas is recognised (regressedMaster), and its key
		// count how emptied replicas are (withoutEmptied).
		last := previous[p.Name]
		offset, keys, replID, runID, inc := last.ReplicationOffset, last.Keys, last.ReplID, last.RunID, last.Incarnation
		synced := false
		if v.status != nil {
			offset, keys, replID, runID, inc = 0, v.status.KeyCount, "", v.status.RunID, incarnation(v.pod)
		}

		switch {
		case p.Name == newMasterName:
			role = kividbv1alpha1.RoleMaster
			if err := r.setRoleLabel(ctx, v.pod, kividbv1alpha1.RoleMaster); err != nil {
				return nil, newMasterName, failoverHappened, err
			}
			if v.status != nil {
				offset = v.status.ReplicationOffset
				if v.status.Role == agentapi.RoleMaster {
					// The offset is "as master" only in the stream the
					// pod itself reports writing.
					replID = v.status.MasterReplID
				}
			}
			synced = v.ready && v.status != nil
		case v.ready && v.status != nil:
			role = kividbv1alpha1.RoleReplica
			offset = v.status.ReplicationOffset
			synced = replicaSynced(masterStatus, v.status, masterIP, port, previouslySynced[p.Name])
			if masterIP != "" && !followsMaster(v.status, masterIP, port) {
				if err := r.Agent.ReplicaOf(ctx, p.Status.PodIP, masterIP, port); err != nil {
					log.Error(err, "failed to point replica at master", "pod", p.Name, "master", masterIP)
				}
			}
			if err := r.setRoleLabel(ctx, v.pod, kividbv1alpha1.RoleReplica); err != nil {
				return nil, newMasterName, failoverHappened, err
			}
		default:
			replID = "" // not the master's entry
			// Not ready / agent unreachable: report unknown role until it
			// recovers. Its label is left alone unless it says "master" --
			// exactly one pod may carry that, see resolveCurrentMaster.
			if p.Labels[kividbv1alpha1.RoleLabel] == string(kividbv1alpha1.RoleMaster) {
				if err := r.setRoleLabel(ctx, v.pod, kividbv1alpha1.RoleReplica); err != nil {
					return nil, newMasterName, failoverHappened, err
				}
			}
		}

		statuses = append(statuses, kividbv1alpha1.KividbPodStatus{
			Name:              p.Name,
			Role:              role,
			Ready:             v.ready,
			ReplicationOffset: offset,
			Synced:            synced,
			Keys:              keys,
			ReplID:            replID,
			RunID:             runID,
			Incarnation:       inc,
		})
	}

	return statuses, newMasterName, failoverHappened, nil
}

// resolveCurrentMaster decides which pod the operator currently considers
// the master, before any health evaluation.
//
// The role label is the source of truth, and normally exactly one pod
// carries role=master. If none does -- the pod was force-deleted, its node
// died, or the StatefulSet recreated it from scratch -- fall back to the
// last-persisted status.masterPod. Without this, a fully vanished master
// pod looks identical to "fresh cluster, never had a master" (both have
// zero labeled pods), which would misclassify a real failover as a routine
// bootstrap: no failoverHappened signal, no status.lastFailoverTime, no
// PhaseFailingOver observability, even though a replica still gets
// promoted correctly either way.
//
// More than one labeled pod is not a state this version creates (a
// failover relabels the old master before promoting), but clusters that
// failed over under an older operator can be sitting in it. Then prefer
// the one status.masterPod names, i.e. the most recent decision that was
// actually persisted, then one that is Ready and really a master, and only
// then fall back to name order. Whichever pods lose are relabeled by
// reconcileRoles.
func resolveCurrentMaster(pods []corev1.Pod, views map[string]*podView, statusMaster string) string {
	var labeled []string
	for _, p := range pods {
		if p.Labels[kividbv1alpha1.RoleLabel] == string(kividbv1alpha1.RoleMaster) {
			labeled = append(labeled, p.Name)
		}
	}
	switch len(labeled) {
	case 0:
		return statusMaster
	case 1:
		return labeled[0]
	}
	for _, name := range labeled {
		if name == statusMaster {
			return name
		}
	}
	for _, name := range labeled {
		if v := views[name]; v.ready && v.status != nil && v.status.Role == agentapi.RoleMaster {
			return name
		}
	}
	return labeled[0]
}

// restartedMasterPause is how long a master that restarted behind its
// replicas has its writes held while the role moves away from it. The
// pause is lifted as soon as it is the new master's replica.
const restartedMasterPause = 10 * time.Second

// kividbContainerName is the engine's container in every pod.
const kividbContainerName = "kividb"

// regressedMaster recognises a master that restarted and lost writes its
// replicas still hold. last is the master pod's status from the previous
// reconcile; pod and st are the pod and what its agent reports now.
//
// It acts only on a restart of that same master. Offsets read from a
// master that has kept running are never taken as lost writes: two
// readings moments apart, one of them just after a promotion, or one
// taken in another stream, can disagree without a write being lost. (In
// the AKS run A3, a planned switchover was followed 0.3 s later by a
// failover on such a reading, which discarded 314 acknowledged writes.)
// So:
//   - last must have been recorded while the pod was the master: with an
//     agent that reports streams, in the stream it reported writing
//     (last.ReplID; cleared when the role moves);
//   - the engine must have restarted since: a new run_id, or, from an
//     agent that does not report one, a new pod or a container restart;
//   - it then lost writes if it went on with its stream from a lower
//     offset, or started a new stream (as an engine that was killed
//     does); one that went on from where it was (a clean stop) lost
//     nothing.
//
// It returns the most caught-up replica that was in sync and has not been
// resynced from the restarted master yet (it is not following its current
// address with the link up), "" if there is none.
func regressedMaster(last kividbv1alpha1.KividbPodStatus, pod *corev1.Pod, st *agentapi.StatusResponse, views map[string]*podView, synced map[string]bool, port int32) (string, int64) {
	if last.Role != kividbv1alpha1.RoleMaster || last.ReplicationOffset <= 0 || !engineRestarted(last, pod, st) {
		return "", 0
	}
	streams := st.RunID != "" || st.MasterReplID != ""
	if streams && last.ReplID == "" {
		return "", 0 // no offset recorded as this stream's master to compare with
	}
	floor := st.ReplicationOffset
	switch {
	case streams && st.MasterReplID != last.ReplID:
		floor = 0 // a new stream: its offset is no position in the old one
	case st.ReplicationOffset >= last.ReplicationOffset:
		return "", 0 // it went on from where it was
	}
	best, bestOffset := "", floor
	for name, v := range views {
		if name == last.Name || !v.ready || v.status == nil || v.status.Role != agentapi.RoleReplica || !synced[name] {
			continue
		}
		if pod.Status.PodIP != "" && followsMaster(v.status, pod.Status.PodIP, port) && (v.status.MasterLinkUp == nil || *v.status.MasterLinkUp) {
			continue // already resynced from the restarted master: nothing left to save on it
		}
		if o := v.status.ReplicationOffset; o > bestOffset || (o == bestOffset && best != "" && name < best) {
			best, bestOffset = name, o
		}
	}
	return best, bestOffset
}

// engineRestarted reports whether pod's engine has restarted since last
// was recorded: its run_id changed or, when either reading has none (an
// agent before 0.7.0), the pod was recreated or its kividb container
// restarted. Nothing known either way is no restart.
func engineRestarted(last kividbv1alpha1.KividbPodStatus, pod *corev1.Pod, st *agentapi.StatusResponse) bool {
	if last.RunID != "" && st.RunID != "" {
		return last.RunID != st.RunID
	}
	now := incarnation(pod)
	return last.Incarnation != "" && now != "" && last.Incarnation != now
}

// incarnation identifies one run of pod's kividb container: the pod's UID
// and the container's restart count. "" when the pod has no UID.
func incarnation(pod *corev1.Pod) string {
	if pod.UID == "" {
		return ""
	}
	var restarts int32
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == kividbContainerName {
			restarts = cs.RestartCount
		}
	}
	return string(pod.UID) + "/" + strconv.FormatInt(int64(restarts), 10)
}

// forgetMasterOffsets clears the recorded stream of the named pods'
// entries: after the master role has moved between them, neither entry's
// offset is a reading of the master it will be compared with.
func forgetMasterOffsets(statuses []kividbv1alpha1.KividbPodStatus, names ...string) {
	for i := range statuses {
		for _, n := range names {
			if statuses[i].Name == n {
				statuses[i].ReplID = ""
			}
		}
	}
}

// pauseWrites holds the writes of pod's engine for d (CLIENT PAUSE ...
// WRITE) and returns its address, or "" when that could not be done (the
// pause is best effort: the caller goes on without it).
func (r *KividbClusterReconciler) pauseWrites(ctx context.Context, pod *corev1.Pod, d time.Duration) string {
	ip := pod.Status.PodIP
	if ip == "" {
		return ""
	}
	if _, err := r.Agent.Exec(ctx, ip, "CLIENT", "PAUSE", strconv.FormatInt(d.Milliseconds(), 10), "WRITE"); err != nil {
		logf.FromContext(ctx).Info("could not hold the writes of the master being failed away from", "pod", pod.Name, "error", err.Error())
		return ""
	}
	return ip
}

// unpauseWrites lifts a pause set by pauseWrites ("" does nothing).
func (r *KividbClusterReconciler) unpauseWrites(ctx context.Context, ip string) {
	if ip == "" {
		return
	}
	if _, err := r.Agent.Exec(ctx, ip, "CLIENT", "UNPAUSE"); err != nil {
		logf.FromContext(ctx).Error(err, "lifting a write pause (it ends by itself)", "ip", ip)
	}
}

// followsMaster reports whether a replica is already configured to
// replicate from the master at masterIP:port. Agents before 0.4.0 always
// report master port 0 (they misread ROLE's integer port), so 0 means
// "unknown" here, not a mismatch.
func followsMaster(replica *agentapi.StatusResponse, masterIP string, port int32) bool {
	return replica.MasterHost == masterIP && (replica.MasterPort == 0 || replica.MasterPort == port)
}

// withoutEmptied returns views minus the pods whose agent positively
// reports holding no keys. A pod whose agent is unreachable or too old to
// report a key count stays in: nothing is known against it.
func withoutEmptied(views map[string]*podView) map[string]*podView {
	out := make(map[string]*podView, len(views))
	for name, v := range views {
		if v.status != nil && v.status.KeyCountKnown && v.status.KeyCount == 0 {
			continue
		}
		out[name] = v
	}
	return out
}

// onlySynced returns the views of the pods marked in synced.
func onlySynced(views map[string]*podView, synced map[string]bool) map[string]*podView {
	out := make(map[string]*podView, len(views))
	for name, v := range views {
		if synced[name] {
			out[name] = v
		}
	}
	return out
}

// actualMasterOf returns the pod that demoted (a Ready pod reporting role
// replica) is replicating from, provided that pod belongs to this cluster,
// is Ready, and itself reports role master. Returns "" otherwise -- e.g.
// when demoted points at an address outside the cluster, or at a pod that
// is itself a replica (a cycle).
func actualMasterOf(views map[string]*podView, demoted string) string {
	host := views[demoted].status.MasterHost
	if host == "" {
		return ""
	}
	for name, v := range views {
		if name == demoted || v.pod.Status.PodIP != host {
			continue
		}
		if v.ready && v.status != nil && v.status.Role == agentapi.RoleMaster {
			return name
		}
	}
	return ""
}

func (r *KividbClusterReconciler) event(c *kividbv1alpha1.KividbCluster, eventType, reason, messageFmt string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(c, eventType, reason, messageFmt, args...)
	}
}

// electReplica picks the ready, non-excluded pod with the highest reported
// replication offset (most caught-up), breaking ties by name for
// determinism. Falls back to the first ready pod if no agent has reported
// a status yet (e.g. brand-new cluster where BGSAVE offsets are all 0).
func electReplica(views map[string]*podView, exclude string) string {
	var best string
	var bestOffset int64 = -1
	for name, v := range views {
		if name == exclude || !v.ready {
			continue
		}
		offset := int64(0)
		if v.status != nil {
			offset = v.status.ReplicationOffset
		}
		if best == "" || offset > bestOffset || (offset == bestOffset && name < best) {
			best = name
			bestOffset = offset
		}
	}
	return best
}

func (r *KividbClusterReconciler) setRoleLabel(ctx context.Context, pod *corev1.Pod, role kividbv1alpha1.NodeRole) error {
	if pod.Labels[kividbv1alpha1.RoleLabel] == string(role) {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[kividbv1alpha1.RoleLabel] = string(role)
	return r.Client.Patch(ctx, pod, patch)
}
