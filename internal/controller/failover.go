package controller

import (
	"context"
	"fmt"
	"sort"
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

	// Find the pod currently carrying the master label, if any.
	var currentMasterName string
	for _, p := range pods {
		if p.Labels[kividbv1alpha1.RoleLabel] == string(kividbv1alpha1.RoleMaster) {
			currentMasterName = p.Name
			break
		}
	}

	// If no live pod carries the master label -- e.g. it was force-deleted,
	// its node died, or the StatefulSet recreated it from scratch -- fall
	// back to the last-persisted status.masterPod. Without this, a fully
	// vanished master pod looks identical to "fresh cluster, never had a
	// master" (both have zero labeled pods), which would misclassify a
	// real failover as a routine bootstrap: no failoverHappened signal, no
	// status.lastFailoverTime, no PhaseFailingOver observability, even
	// though a replica still gets promoted correctly either way.
	if currentMasterName == "" {
		currentMasterName = c.Status.MasterPod
	}

	threshold := defaultUnhealthyThreshold
	if c.Spec.Failover.UnhealthyThresholdSeconds != nil {
		threshold = time.Duration(*c.Spec.Failover.UnhealthyThresholdSeconds) * time.Second
	}
	failoverEnabled := boolOr(c.Spec.Failover.Enabled, true)

	needsElection := currentMasterName == ""
	needsFailover := false
	if currentMasterName != "" {
		mv, exists := views[currentMasterName]
		if !exists {
			needsFailover = failoverEnabled // labeled master pod is gone entirely
		} else if !mv.ready || mv.status == nil || mv.status.Role != agentapi.RoleMaster {
			if since, unready := readyFalseSince(mv.pod); unready {
				needsFailover = failoverEnabled && time.Since(since) >= threshold
			} else if !mv.ready {
				needsFailover = failoverEnabled
			}
		}
	}

	failoverHappened := false
	newMasterName := currentMasterName

	if needsElection || needsFailover {
		candidate := electReplica(views, currentMasterName)
		if candidate == "" {
			return nil, currentMasterName, false, fmt.Errorf("no ready pod available to elect as master")
		}
		log.Info("promoting pod to master", "pod", candidate, "reason", map[bool]string{true: "failover", false: "bootstrap"}[needsFailover])
		if err := r.Agent.Promote(ctx, views[candidate].pod.Status.PodIP); err != nil {
			return nil, currentMasterName, false, fmt.Errorf("promoting %s: %w", candidate, err)
		}
		if err := r.setRoleLabel(ctx, views[candidate].pod, kividbv1alpha1.RoleMaster); err != nil {
			return nil, currentMasterName, false, err
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

	statuses := make([]kividbv1alpha1.KividbPodStatus, 0, len(pods))
	for _, p := range pods {
		v := views[p.Name]
		role := kividbv1alpha1.RoleUnknown
		var offset int64

		switch {
		case p.Name == newMasterName:
			role = kividbv1alpha1.RoleMaster
			if err := r.setRoleLabel(ctx, v.pod, kividbv1alpha1.RoleMaster); err != nil {
				return nil, newMasterName, failoverHappened, err
			}
			if v.status != nil {
				offset = v.status.ReplicationOffset
			}
		case v.ready && v.status != nil:
			role = kividbv1alpha1.RoleReplica
			offset = v.status.ReplicationOffset
			if masterIP != "" && (v.status.MasterHost != masterIP || v.status.MasterPort != port) {
				if err := r.Agent.ReplicaOf(ctx, p.Status.PodIP, masterIP, port); err != nil {
					log.Error(err, "failed to point replica at master", "pod", p.Name, "master", masterIP)
				}
			}
			if err := r.setRoleLabel(ctx, v.pod, kividbv1alpha1.RoleReplica); err != nil {
				return nil, newMasterName, failoverHappened, err
			}
		default:
			// Not ready / agent unreachable: leave whatever label it has
			// (usually "replica") and report unknown role until it recovers.
		}

		statuses = append(statuses, kividbv1alpha1.KividbPodStatus{
			Name:              p.Name,
			Role:              role,
			Ready:             v.ready,
			ReplicationOffset: offset,
		})
	}

	return statuses, newMasterName, failoverHappened, nil
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
