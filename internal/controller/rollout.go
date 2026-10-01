package controller

import (
	"context"
	"sort"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
	for _, s := range statuses {
		if !s.Ready || !s.Synced {
			return
		}
	}

	// Replicas first, highest ordinal first; the master last, once every
	// replica is on the new template and back in sync.
	sort.Slice(outdated, func(i, j int) bool {
		if (outdated[i].Name == masterPod) != (outdated[j].Name == masterPod) {
			return outdated[j].Name == masterPod
		}
		return outdated[i].Name > outdated[j].Name
	})
	next := outdated[0]
	log.Info("rollout: replacing pod to apply the updated pod template", "pod", next.Name, "remaining", len(outdated)-1)
	if err := r.Delete(ctx, next); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "rollout: deleting pod", "pod", next.Name)
		return
	}
	r.event(c, corev1.EventTypeNormal, "RollingUpdate", "replacing %s to apply the updated pod template (%d more to go)", next.Name, len(outdated)-1)
}
