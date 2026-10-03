package controller

import (
	"context"
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
	// a moment apart, so under write load they never match exactly.
	maxSyncLagBytes = 8 << 20

	// stuckPodGrace is how long a pod on an outdated template must have
	// been unready before the rollout replaces it without waiting for the
	// rest of the cluster to be healthy.
	stuckPodGrace = 30 * time.Second
)

// replicaSynced reports whether a replica has finished its initial full
// sync from the current master and is keeping up with it.
//
// kividb itself does not say: while a replica is still receiving and
// loading the snapshot, ROLE already reports "connected", INFO reports
// master_link_status:up and master_sync_in_progress:0, and the pod is
// Ready. What does change is the replica's replication offset, which
// stays 0 until the snapshot is loaded and then jumps to the master's.
// That is the signal used here.
//
// A master that has not streamed anything yet reports offset 0 itself,
// which leaves nothing to compare; in that case fall back to the key
// counts being close (the replica swaps the loaded dataset in at once, so
// mid-sync it holds either nothing or whatever stale data it started
// with).
//
// previously is what the last reconcile concluded, and stands in when the
// master cannot be queried: a replica does not stop being the best
// failover candidate because its master just died.
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

// switchover hands the master role from masterPod to the most caught-up
// replica that is in sync, and returns that replica's name. It is the
// planned counterpart of a failover: the old master is alive, so it is
// turned into a replica of the new one rather than left behind.
//
// The order is chosen to keep the window in which a write can be lost as
// short as possible: the old master leaves the master Service first, so no
// new connection reaches it; the replica is then promoted and takes its
// place in the Service; and the old master is pointed at it last. Only a
// write sent on an already-open connection to the old master during those
// few calls is not carried over.
func (r *KividbClusterReconciler) switchover(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod string) (string, error) {
	byName := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		byName[pods[i].Name] = &pods[i]
	}
	old, ok := byName[masterPod]
	if !ok {
		return "", fmt.Errorf("master pod %s not found", masterPod)
	}

	var target *corev1.Pod
	var targetOffset int64
	for _, s := range statuses {
		p := byName[s.Name]
		if s.Name == masterPod || p == nil || !s.Ready || !s.Synced || s.Role != kividbv1alpha1.RoleReplica {
			continue
		}
		if target == nil || s.ReplicationOffset > targetOffset || (s.ReplicationOffset == targetOffset && s.Name < target.Name) {
			target, targetOffset = p, s.ReplicationOffset
		}
	}
	if target == nil {
		return "", fmt.Errorf("no replica is in sync to take over from %s", masterPod)
	}

	logf.FromContext(ctx).Info("switchover: moving the master role", "from", masterPod, "to", target.Name)
	if err := r.setRoleLabel(ctx, old, kividbv1alpha1.RoleReplica); err != nil {
		return "", err
	}
	if err := r.Agent.Promote(ctx, target.Status.PodIP); err != nil {
		// Nothing has changed on the kividb side: give the label back.
		_ = r.setRoleLabel(ctx, old, kividbv1alpha1.RoleMaster)
		return "", fmt.Errorf("promoting %s: %w", target.Name, err)
	}
	if err := r.setRoleLabel(ctx, target, kividbv1alpha1.RoleMaster); err != nil {
		return "", err
	}
	if err := r.Agent.ReplicaOf(ctx, old.Status.PodIP, target.Status.PodIP, getPort(c)); err != nil {
		// The next reconcileRoles pass sees a replica-labeled pod that
		// still reports master and re-points it.
		logf.FromContext(ctx).Error(err, "switchover: pointing the old master at the new one", "pod", masterPod)
	}
	r.event(c, corev1.EventTypeNormal, "Switchover", "moved the master role from %s to %s", masterPod, target.Name)
	return target.Name, nil
}

// reconcileStepDown performs the switchover a StepDownAnnotation on the
// master's pod asks for, then removes the annotation. If no replica is in
// sync yet the request simply stays until one is.
func (r *KividbClusterReconciler) reconcileStepDown(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, statuses []kividbv1alpha1.KividbPodStatus, masterPod string) {
	log := logf.FromContext(ctx)
	for i := range pods {
		p := &pods[i]
		if p.Annotations[StepDownAnnotation] != "true" || p.DeletionTimestamp != nil {
			continue
		}
		if p.Name == masterPod {
			if _, err := r.switchover(ctx, c, pods, statuses, masterPod); err != nil {
				log.V(1).Info("step-down requested but not possible yet", "pod", p.Name, "reason", err.Error())
				continue
			}
		}
		// Not (or no longer) the master: the request is fulfilled.
		patch := client.MergeFrom(p.DeepCopy())
		delete(p.Annotations, StepDownAnnotation)
		if err := r.Client.Patch(ctx, p, patch); err != nil {
			log.Error(err, "removing step-down annotation", "pod", p.Name)
		}
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
		if _, err := r.switchover(ctx, c, pods, statuses, masterPod); err != nil {
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
}
