package controller

import (
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestRestartPending(t *testing.T) {
	t.Parallel()

	pod := func(uid string, ready, terminating bool) *corev1.Pod {
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "c1-1", UID: types.UID(uid)},
			Status: corev1.PodStatus{
				PodIP:      "10.0.0.2",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
			},
		}
		if terminating {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}
		return p
	}
	clusterWith := func(role kividbv1alpha1.NodeRole, synced bool) *kividbv1alpha1.KividbCluster {
		c := &kividbv1alpha1.KividbCluster{}
		c.Status.Pods = []kividbv1alpha1.KividbPodStatus{{Name: "c1-1", Role: role, Ready: true, Synced: synced}}
		return c
	}
	joined := clusterWith(kividbv1alpha1.RoleReplica, true)

	tests := []struct {
		name    string
		pod     *corev1.Pod
		cluster *kividbv1alpha1.KividbCluster
		want    string
	}{
		{"deleted pod still terminating and Ready", pod("old", true, true), joined, "to terminate"},
		{"deleted pod not yet marked terminating", pod("old", true, false), joined, "to terminate"},
		{"no pod yet", nil, joined, "to be recreated"},
		{"replacement not Ready", pod("new", false, false), joined, "to become Ready"},
		{"replacement Ready, role not assigned yet", pod("new", true, false), clusterWith(kividbv1alpha1.RoleUnknown, true), "to rejoin the cluster"},
		{"replacement Ready, still resyncing", pod("new", true, false), clusterWith(kividbv1alpha1.RoleReplica, false), "to finish syncing from the master"},
		{"replacement Ready, missing from cluster status", pod("new", true, false), &kividbv1alpha1.KividbCluster{}, "to rejoin the cluster"},
		{"replacement Ready and rejoined", pod("new", true, false), joined, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := restartPending(tt.pod, "old", tt.cluster); got != tt.want {
				t.Fatalf("restartPending() = %q, want %q", got, tt.want)
			}
		})
	}
}
