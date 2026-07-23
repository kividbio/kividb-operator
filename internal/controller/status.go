package controller

import (
	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

// computePhase derives the coarse-grained cluster phase from per-pod
// status. failoverHappened forces PhaseFailingOver for this reconcile pass
// even if all pods already look healthy again, so the transition is
// observable in status/events rather than being invisible between two
// "Running" reconciles.
func computePhase(desiredPods int32, statuses []kividbv1alpha1.KividbPodStatus, masterPod string, failoverHappened bool) kividbv1alpha1.ClusterPhase {
	if failoverHappened {
		return kividbv1alpha1.PhaseFailingOver
	}
	if masterPod == "" {
		return kividbv1alpha1.PhaseProvisioning
	}
	if int32(len(statuses)) < desiredPods {
		return kividbv1alpha1.PhaseProvisioning
	}

	readyCount := 0
	hasMaster := false
	for _, s := range statuses {
		if s.Ready {
			readyCount++
		}
		if s.Role == kividbv1alpha1.RoleMaster && s.Ready {
			hasMaster = true
		}
	}

	switch {
	case !hasMaster:
		return kividbv1alpha1.PhaseError
	case int32(readyCount) == desiredPods:
		return kividbv1alpha1.PhaseRunning
	case readyCount > 0:
		return kividbv1alpha1.PhaseDegraded
	default:
		return kividbv1alpha1.PhaseError
	}
}
