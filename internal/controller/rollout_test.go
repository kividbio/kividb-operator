package controller

import (
	"context"
	"sort"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReplicaSynced(t *testing.T) {
	t.Parallel()
	const masterIP, port = "10.0.0.1", int32(6380)
	replica := func(offset, keys int64) *agentapi.StatusResponse {
		return &agentapi.StatusResponse{Role: agentapi.RoleReplica, MasterHost: masterIP, MasterPort: port, ReplicationOffset: offset, KeyCount: keys}
	}
	masterAt := func(offset, keys int64) *agentapi.StatusResponse {
		return &agentapi.StatusResponse{Role: agentapi.RoleMaster, ReplicationOffset: offset, KeyCount: keys}
	}
	elsewhere := replica(500, 10)
	elsewhere.MasterHost = "10.0.0.9"
	portUnknown := replica(500, 10)
	portUnknown.MasterPort = 0

	tests := []struct {
		name            string
		master, replica *agentapi.StatusResponse
		previously      bool
		want            bool
	}{
		{"caught up", masterAt(500, 10), replica(500, 10), false, true},
		{"slightly behind under load", masterAt(5_000_000, 10), replica(4_900_000, 10), false, true},
		{"far behind", masterAt(50_000_000, 10), replica(1_000, 10), false, false},
		{"still loading the snapshot: offset stays 0", masterAt(30_758, 948_398), replica(0, 0), true, false},
		{"replicating from a different master", masterAt(500, 10), elsewhere, true, false},
		{"agent unreachable", masterAt(500, 10), nil, true, false},
		{"older agent that reports master port 0", masterAt(500, 10), portUnknown, false, true},
		{"idle master, same key count", masterAt(0, 948_398), replica(0, 948_398), false, true},
		{"idle master, replica still empty", masterAt(0, 948_398), replica(0, 0), false, false},
		{"idle and empty cluster", masterAt(0, 0), replica(0, 0), false, true},
		{"master unreachable keeps the last verdict (was synced)", nil, replica(500, 10), true, true},
		{"master unreachable keeps the last verdict (was not)", nil, replica(0, 0), false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := replicaSynced(tt.master, tt.replica, masterIP, port, tt.previously); got != tt.want {
				t.Fatalf("replicaSynced() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A replica that was mid-resync when the master died must not be promoted
// over one that was in sync, whatever offsets they report.
func TestReconcileRoles_FailoverPrefersSyncedReplica(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: time.Minute, agent: master(900)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 0)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 0)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Synced: true},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Synced: false},
		{Name: "c1-2", Role: kividbv1alpha1.RoleReplica, Synced: true},
	}

	if m, failover := h.reconcile(); m != "c1-2" || !failover {
		t.Fatalf("master=%q failover=%v, want a failover to the synced replica c1-2", m, failover)
	}
}

type rolloutPod struct {
	name       string
	revision   string
	ready      bool
	unreadyFor time.Duration
	synced     bool
	deleting   bool
}

func TestReconcileRollout(t *testing.T) {
	const master = "c1-0"
	healthy := func(name, revision string) rolloutPod {
		return rolloutPod{name: name, revision: revision, ready: true, synced: true}
	}

	tests := []struct {
		name        string
		pods        []rolloutPod
		noStatuses  bool // role reconciliation failed this pass
		wantDeleted []string
	}{
		{
			name:        "everything outdated and healthy: one replica, highest ordinal first",
			pods:        []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "old"), healthy("c1-2", "old")},
			wantDeleted: []string{"c1-2"},
		},
		{
			name:        "replicas done: the master goes last",
			pods:        []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "new"), healthy("c1-2", "new")},
			wantDeleted: []string{"c1-0"},
		},
		{
			name: "nothing outdated",
			pods: []rolloutPod{healthy("c1-0", "new"), healthy("c1-1", "new"), healthy("c1-2", "new")},
		},
		{
			name: "the replaced pod is not Ready yet: wait",
			pods: []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "old"), {name: "c1-2", revision: "new", unreadyFor: 5 * time.Second}},
		},
		{
			name: "the replaced pod is Ready but still resyncing: wait",
			pods: []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "old"), {name: "c1-2", revision: "new", ready: true}},
		},
		{
			name: "a pod is still terminating: wait",
			pods: []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "old"), {name: "c1-2", revision: "old", ready: true, synced: true, deleting: true}},
		},
		{
			name:        "a crash-looping outdated pod is replaced without waiting for a healthy cluster",
			pods:        []rolloutPod{healthy("c1-0", "old"), {name: "c1-1", revision: "old", unreadyFor: 5 * time.Minute}, healthy("c1-2", "old")},
			wantDeleted: []string{"c1-1"},
		},
		{
			name: "an outdated pod that only just went unready is given time",
			pods: []rolloutPod{healthy("c1-0", "old"), {name: "c1-1", revision: "old", unreadyFor: 5 * time.Second}, healthy("c1-2", "old")},
		},
		{
			name:        "no healthy pod at all: every stuck outdated pod is replaced",
			pods:        []rolloutPod{{name: "c1-0", revision: "old", unreadyFor: time.Hour}, {name: "c1-1", revision: "old", unreadyFor: time.Hour}},
			noStatuses:  true,
			wantDeleted: []string{"c1-0", "c1-1"},
		},
		{
			name: "a broken pod already on the new template is not this code's to delete",
			pods: []rolloutPod{healthy("c1-0", "old"), healthy("c1-1", "old"), {name: "c1-2", revision: "new", unreadyFor: time.Hour}},
		},
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &kividbv1alpha1.KividbCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
				Spec:       kividbv1alpha1.KividbClusterSpec{Replicas: int32(len(tt.pods) - 1)},
			}
			objs := []client.Object{&appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default", Generation: 3},
				Status:     appsv1.StatefulSetStatus{UpdateRevision: "new", ObservedGeneration: 3},
			}}
			var pods []corev1.Pod
			var statuses []kividbv1alpha1.KividbPodStatus
			for _, p := range tt.pods {
				cond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
				if !p.ready {
					cond.Status = corev1.ConditionFalse
					cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-p.unreadyFor))
				}
				pod := corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: p.name, Namespace: "default",
						Labels: map[string]string{appsv1.StatefulSetRevisionLabel: p.revision},
					},
					Status: corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{cond}},
				}
				objs = append(objs, pod.DeepCopy())
				if p.deleting {
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				pods = append(pods, pod)
				statuses = append(statuses, kividbv1alpha1.KividbPodStatus{Name: p.name, Ready: p.ready, Synced: p.synced})
			}
			if tt.noStatuses {
				statuses = nil
			}

			r := &KividbClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
				Scheme: scheme,
			}
			r.reconcileRollout(context.Background(), c, pods, statuses, master)

			var left corev1.PodList
			if err := r.List(context.Background(), &left); err != nil {
				t.Fatal(err)
			}
			remaining := map[string]bool{}
			for _, p := range left.Items {
				remaining[p.Name] = true
			}
			var deleted []string
			for _, p := range tt.pods {
				if !remaining[p.name] {
					deleted = append(deleted, p.name)
				}
			}
			sort.Strings(deleted)
			if len(deleted) != len(tt.wantDeleted) {
				t.Fatalf("deleted %v, want %v", deleted, tt.wantDeleted)
			}
			for i := range deleted {
				if deleted[i] != tt.wantDeleted[i] {
					t.Fatalf("deleted %v, want %v", deleted, tt.wantDeleted)
				}
			}
		})
	}
}
