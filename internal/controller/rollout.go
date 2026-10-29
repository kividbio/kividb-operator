package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// maxSyncLagBytes is how far behind the master's replication offset a
	// replica may be and still count as in sync. The two offsets are read
	// a few milliseconds apart (one agent call each), so under write load
	// they never match exactly; 1 MiB is that gap at well over 100 MB/s of
	// writes. It used to be 8 MiB, minutes of writes at a modest rate,
	// which let a replica whose link had failed -- its offset frozen -- go
	// on counting as in sync and be promoted. A planned switchover does not
	// rely on it: it waits for the exact offset (see switchover.go).
	maxSyncLagBytes = 1 << 20

	// maxMasterSilenceSeconds is how long a replica may have heard nothing
	// from its master (master_last_io_seconds_ago) and still count as in
	// sync. A master writes a PING into the stream every
	// repl-ping-replica-period (10 s by default), so a live link is never
	// silent for much longer than that.
	maxMasterSilenceSeconds = 20

	// stuckPodGrace is how long a pod on an outdated template must have
	// been unready before the rollout replaces it without waiting for the
	// rest of the cluster to be healthy.
	stuckPodGrace = 30 * time.Second
)

// replicaSynced reports whether a replica has finished its initial full
// sync from the current master and is keeping up with it.
//
// A replica whose link to the master is down is not in sync, whatever its
// offset says: its offset is frozen where the link failed (a replica
// restarted without the master's password, for one, retries and fails
// every second). Nor is one that has heard nothing from its master for
// longer than maxMasterSilenceSeconds. Agents before 0.7.0 do not report
// either, and are judged by their offsets alone.
//
// kividb does not say when a full sync is over: while a replica is still
// receiving and loading the snapshot, ROLE already reports "connected",
// INFO reports master_link_status:up and master_sync_in_progress:0, and
// the pod is Ready. What does change is the replica's replication offset,
// which stays 0 until the snapshot is loaded and then jumps to the
// master's. That is the signal used here.
//
// A master that has not streamed anything yet reports offset 0 itself,
// which leaves nothing to compare; in that case fall back to the key
// counts being close (the replica swaps the loaded dataset in at once, so
// mid-sync it holds either nothing or whatever stale data it started
// with).
//
// previously is what the last reconcile concluded, and stands in when the
// master cannot be queried: a replica does not stop being the best
// failover candidate because its master just died (its link went down
// with the master, so the link is not looked at then).
func replicaSynced(master, replica *agentapi.StatusResponse, masterIP string, port int32, previously bool) bool {
	if replica == nil || replica.Role != agentapi.RoleReplica {
		return false
	}
	// kividb before v1.0.4 answers ROLE on a replica with an empty master
	// host, so on those pods there is nothing to check here and the
	// offsets below have to carry the verdict. Insisting on a match would
	// leave every replica "not in sync" forever, and with it block the
	// very rollout that upgrades the engine.
	if replica.MasterHost != "" && !followsMaster(replica, masterIP, port) {
		return false
	}
	if master == nil {
		return previously
	}
	if replica.MasterLinkUp != nil && !*replica.MasterLinkUp {
		return false
	}
	if s := replica.MasterLastIOSeconds; s != nil && (*s < 0 || *s > maxMasterSilenceSeconds) {
		return false
	}
	if master.ReplicationOffset > 0 {
		return replica.ReplicationOffset > 0 && master.ReplicationOffset-replica.ReplicationOffset <= maxSyncLagBytes
	}
	slack := master.KeyCount / 100
	if slack < 16 {
		slack = 16
	}
	diff := master.KeyCount - replica.KeyCount
	if diff < 0 {
		diff = -diff
	}
	return diff <= slack
}

// reconcileHandover moves the master role when something asks for it:
//
//   - the master pod is being deleted -- by hand, by a node drain or an
//     eviction. Its preStop hook holds the engine (see the agent's
//     /prestop) while the role moves, so it is stopped as a replica rather
//     than as the master;
//   - a StepDownAnnotation on the master's pod (a KividbDbOps restart);
//   - the SwitchoverToAnnotation on the cluster (the GUI's promote action).
//
// Each is a lossless switchover (see switchover.go). It returns the new
// master's name when the role moved on this pass, and removes the requests
// that are fulfilled, or that name a pod that does not exist.
func (r *KividbClusterReconciler) reconcileHandover(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod string) string {
	log := logf.FromContext(ctx)
	var master *corev1.Pod
	exists := map[string]bool{}
	for i := range pods {
		exists[pods[i].Name] = true
		if pods[i].Name == masterPod {
			master = &pods[i]
		}
	}

	if to := c.Annotations[SwitchoverToAnnotation]; to != "" && (to == masterPod || !exists[to]) {
		if !exists[to] {
			r.event(c, corev1.EventTypeWarning, "SwitchoverRequestDropped", "%s asks for a switchover to %s, which is not a pod of this cluster", SwitchoverToAnnotation, to)
		}
		r.clearSwitchoverRequest(ctx, c)
	}

	newMaster := ""
	if master != nil && statuses != nil {
		if req, want := handoverReason(c, master); want {
			name, err := r.switchover(ctx, c, pods, statuses, masterPod, req)
			switch {
			case err == nil:
				newMaster = name
				if c.Annotations[SwitchoverToAnnotation] != "" {
					r.clearSwitchoverRequest(ctx, c)
				}
			case errors.Is(err, errSwitchoverDeferred):
				log.V(1).Info("handover deferred", "pod", masterPod, "reason", req.reason)
			case req.urgent:
				log.Error(err, "the master pod is being deleted and could not hand its role over", "pod", masterPod)
			default:
				log.V(1).Info("handover requested but not possible yet", "pod", masterPod, "reason", req.reason, "why", err.Error())
			}
		}
	}

	// A step-down request on a pod that is not the master is fulfilled.
	for i := range pods {
		p := &pods[i]
		if p.Annotations[StepDownAnnotation] != "true" || (p.Name == masterPod && newMaster == "") {
			continue
		}
		patch := client.MergeFrom(p.DeepCopy())
		delete(p.Annotations, StepDownAnnotation)
		if err := r.Client.Patch(ctx, p, patch); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "removing step-down annotation", "pod", p.Name)
		}
	}
	return newMaster
}

// clearSwitchoverRequest removes the SwitchoverToAnnotation from c.
func (r *KividbClusterReconciler) clearSwitchoverRequest(ctx context.Context, c *kividbv1alpha1.KividbCluster) {
	patch := client.MergeFrom(c.DeepCopy())
	delete(c.Annotations, SwitchoverToAnnotation)
	if err := r.Client.Patch(ctx, c, patch); err != nil && !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "removing the switchover request")
	}
}

// unreadySince returns when pod last stopped being (or never became)
// Ready.
func unreadySince(pod *corev1.Pod) time.Time {
	if since, ok := readyFalseSince(pod); ok {
		return since
	}
	return pod.CreationTimestamp.Time
}

// reconcileRollout moves pods onto the StatefulSet's current pod template,
// one at a time. The StatefulSet uses the OnDelete update strategy, so
// nothing is replaced unless this deletes it.
//
// The StatefulSet controller's own RollingUpdate is not used because it
// gets two things wrong for a replicated database:
//
//   - It moves on as soon as the replaced pod is Ready, and a kividb
//     replica is Ready long before it has finished resyncing. The rollout
//     would reach the master while no replica held the data yet, and the
//     failover that follows would promote an empty or stale replica.
//   - It refuses to replace anything while a pod is already unavailable.
//     A crash-looping pod therefore blocks the very template change meant
//     to fix it (more memory, a corrected config), and a pod deleted by
//     hand comes back on the old template.
//
// So: an outdated pod that has been unready for stuckPodGrace is replaced
// straight away -- it is serving nothing, there is nothing to protect.
// Otherwise one outdated pod is replaced at a time, replicas before the
// master, and only while every pod is Ready and every replica in sync.
//
// statuses is this reconcile's per-pod view from reconcileRoles, nil if
// role reconciliation failed (which must not stop stuck pods from being
// replaced: a cluster with no healthy pod at all is exactly that case).
func (r *KividbClusterReconciler) reconcileRollout(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod string) {
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: statefulSetName(c)}, &sts); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "rollout: reading StatefulSet")
		}
		return
	}
	target := sts.Status.UpdateRevision
	if target == "" || sts.Status.ObservedGeneration < sts.Generation {
		return // the StatefulSet controller has not seen the latest template yet
	}

	var outdated []*corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			return // one replacement at a time
		}
		if p.Labels[appsv1.StatefulSetRevisionLabel] != target {
			outdated = append(outdated, p)
		}
	}
	if len(outdated) == 0 {
		r.rolloutDone(c)
		return
	}

	replaced := false
	for _, p := range outdated {
		if isPodReady(p) || time.Since(unreadySince(p)) < stuckPodGrace {
			continue
		}
		log.Info("rollout: replacing unready pod on an outdated template", "pod", p.Name)
		if err := r.Delete(ctx, p); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "rollout: deleting pod", "pod", p.Name)
			continue
		}
		r.event(c, corev1.EventTypeWarning, "StuckPodReplaced", "%s was not ready and on an outdated pod template; replaced it", p.Name)
		r.rolloutProgressed(c, fmt.Sprintf("replaced %s, which was not ready, on an outdated pod template", p.Name))
		replaced = true
	}
	if replaced {
		return
	}

	if int32(len(pods)) != c.Spec.Replicas+1 || len(statuses) != len(pods) {
		return
	}
	healthy := make(map[string]bool, len(statuses))
	unhealthy := 0
	for _, s := range statuses {
		healthy[s.Name] = s.Ready && s.Synced
		if !healthy[s.Name] {
			unhealthy++
		}
	}

	// Replicas first, the master last, once every replica is on the new
	// template and back in sync. Among replicas, one that is not in sync
	// goes before the healthy ones (it is the least use as it stands),
	// then highest ordinal first.
	sort.Slice(outdated, func(i, j int) bool {
		a, b := outdated[i].Name, outdated[j].Name
		if (a == masterPod) != (b == masterPod) {
			return b == masterPod
		}
		if healthy[a] != healthy[b] {
			return !healthy[a]
		}
		return a > b
	})
	next := outdated[0]

	// What has to hold is that every pod *other than* the one being
	// replaced is Ready and in sync; the pod itself need not be. Requiring
	// it of that pod too deadlocks whenever an outdated pod cannot sync
	// until it is replaced -- a kividb v1.0.4 replica, for instance, has
	// no way to authenticate to a v1.0.5 master, so the last old pod of an
	// engine upgrade would wait forever to be "in sync" first.
	if unhealthy > 1 || (unhealthy == 1 && healthy[next.Name]) {
		reasons := map[string]string{}
		if at, ok := r.notSynced.Load(rolloutWaitKey(c)); ok {
			reasons = at.(map[string]string)
		}
		var blockers []rolloutBlocker
		for _, s := range statuses {
			switch {
			case healthy[s.Name] || s.Name == next.Name:
			case !s.Ready:
				blockers = append(blockers, rolloutBlocker{pod: s.Name, why: "not Ready"})
			case s.Name == masterPod:
				blockers = append(blockers, rolloutBlocker{pod: s.Name, why: "the master's agent did not answer"})
			default:
				why := reasons[s.Name]
				if why == "" {
					why = "not in sync"
				}
				blockers = append(blockers, rolloutBlocker{pod: s.Name, syncing: true, why: "replica not in sync: " + why})
			}
		}
		r.rolloutWaiting(ctx, c, next.Name, blockers)
		return
	}

	// The master is never replaced while it is the master. Its replacement
	// would come back with whatever its volume holds, every replica would
	// resync from that, and anything written since the master's last save
	// would be gone from all of them -- kividb's save on shutdown cannot
	// be relied on to close that gap. Move the role to an in-sync replica
	// first; the old master is then just another outdated replica and is
	// replaced on a later pass.
	if next.Name == masterPod && len(pods) > 1 {
		to, err := r.switchover(ctx, c, pods, statuses, masterPod, switchoverRequest{reason: "rollout"})
		switch {
		case err == nil:
			r.rolloutProgressed(c, fmt.Sprintf("moved the master role from %s to %s, to replace %s next", masterPod, to, masterPod))
		case errors.Is(err, errSwitchoverDeferred):
			log.V(1).Info("rollout: the master hands over later", "pod", masterPod, "why", err.Error())
		default:
			log.Error(err, "rollout: moving the master role before replacing the master", "pod", masterPod)
		}
		return
	}
	log.Info("rollout: replacing pod to apply the updated pod template", "pod", next.Name, "remaining", len(outdated)-1)
	if err := r.Delete(ctx, next); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "rollout: deleting pod", "pod", next.Name)
		return
	}
	r.event(c, corev1.EventTypeNormal, "RollingUpdate", "replacing %s to apply the updated pod template (%d more to go)", next.Name, len(outdated)-1)
	r.rolloutProgressed(c, fmt.Sprintf("replacing %s (%d more to go)", next.Name, len(outdated)-1))
}
