package controller

import (
	"fmt"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

func TestComputePhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		desiredPods      int32
		statuses         []kividbv1alpha1.KividbPodStatus
		masterPod        string
		failoverHappened bool
		want             kividbv1alpha1.ClusterPhase
	}{
		{
			name:             "FailoverHappened → PhaseFailingOver",
			desiredPods:      3,
			statuses:         allReadyWithMaster(3),
			masterPod:        "pod-0",
			failoverHappened: true,
			want:             kividbv1alpha1.PhaseFailingOver,
		},
		{
			name:        "empty masterPod → PhaseProvisioning",
			desiredPods: 3,
			statuses:    allReadyWithMaster(3),
			masterPod:   "",
			want:        kividbv1alpha1.PhaseProvisioning,
		},
		{
			name:        "fewer statuses than desired → PhaseProvisioning",
			desiredPods: 3,
			statuses: []kividbv1alpha1.KividbPodStatus{
				{Name: "pod-0", Role: kividbv1alpha1.RoleMaster, Ready: true},
				{Name: "pod-1", Role: kividbv1alpha1.RoleReplica, Ready: true},
			},
			masterPod: "pod-0",
			want:      kividbv1alpha1.PhaseProvisioning,
		},
		{
			name:        "all ready with master → PhaseRunning",
			desiredPods: 3,
			statuses:    allReadyWithMaster(3),
			masterPod:   "pod-0",
			want:        kividbv1alpha1.PhaseRunning,
		},
		{
			name:        "some ready with master → PhaseDegraded",
			desiredPods: 3,
			statuses: []kividbv1alpha1.KividbPodStatus{
				{Name: "pod-0", Role: kividbv1alpha1.RoleMaster, Ready: true},
				{Name: "pod-1", Role: kividbv1alpha1.RoleReplica, Ready: true},
				{Name: "pod-2", Role: kividbv1alpha1.RoleReplica, Ready: false},
			},
			masterPod: "pod-0",
			want:      kividbv1alpha1.PhaseDegraded,
		},
		{
			name:        "no ready / no master → PhaseError",
			desiredPods: 2,
			statuses: []kividbv1alpha1.KividbPodStatus{
				{Name: "pod-0", Role: kividbv1alpha1.RoleUnknown, Ready: false},
				{Name: "pod-1", Role: kividbv1alpha1.RoleUnknown, Ready: false},
			},
			masterPod: "pod-0",
			want:      kividbv1alpha1.PhaseError,
		},
		{
			name:        "master present but not ready → PhaseError",
			desiredPods: 2,
			statuses: []kividbv1alpha1.KividbPodStatus{
				{Name: "pod-0", Role: kividbv1alpha1.RoleMaster, Ready: false},
				{Name: "pod-1", Role: kividbv1alpha1.RoleReplica, Ready: true},
			},
			masterPod: "pod-0",
			want:      kividbv1alpha1.PhaseError,
		},
		{
			name:             "failover takes precedence over empty master",
			desiredPods:      1,
			statuses:         nil,
			masterPod:        "",
			failoverHappened: true,
			want:             kividbv1alpha1.PhaseFailingOver,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := computePhase(tt.desiredPods, tt.statuses, tt.masterPod, tt.failoverHappened)
			if got != tt.want {
				t.Fatalf("computePhase() = %q, want %q", got, tt.want)
			}
		})
	}
}

func allReadyWithMaster(n int) []kividbv1alpha1.KividbPodStatus {
	out := make([]kividbv1alpha1.KividbPodStatus, n)
	for i := 0; i < n; i++ {
		role := kividbv1alpha1.RoleReplica
		if i == 0 {
			role = kividbv1alpha1.RoleMaster
		}
		out[i] = kividbv1alpha1.KividbPodStatus{
			Name:  fmt.Sprintf("pod-%d", i),
			Role:  role,
			Ready: true,
		}
	}
	return out
}
