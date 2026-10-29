package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// rolloutCondition returns c's RolloutProgressing condition, nil if none.
func rolloutCondition(c *kividbv1alpha1.KividbCluster) *metav1.Condition {
	return meta.FindStatusCondition(c.Status.Conditions, RolloutProgressingCondition)
}

// A replicated cluster's rollout that waits on a frozen replica (link
// down, its offset stuck) says so: the condition names the pod and why at
// once, turns False with a Warning event after rolloutStallAfter, and the
// event is not repeated before rolloutWaitReportEvery. Nothing is
// replaced meanwhile; once the replica is back in sync the rollout moves.
func TestRolloutReportsAReplicaNotInSync(t *testing.T) {
	frozen := replicaOf("10.0.0.1", 1_000_000)
	down := false
	frozen.MasterLinkUp = &down
	silent := int64(-1)
	frozen.MasterLastIOSeconds = &silent
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: master(5_000_000)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: frozen},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 5_000_000)},
	)
	ctx := context.Background()
	if err := h.r.Create(ctx, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default", Generation: 2},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "new", ObservedGeneration: 2},
	}); err != nil {
		t.Fatal(err)
	}
	// c1-1 has been replaced already; c1-0 and c1-2 are still to go.
	for name, rev := range map[string]string{"c1-0": "old", "c1-1": "new", "c1-2": "old"} {
		var p corev1.Pod
		if err := h.r.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, &p); err != nil {
			t.Fatal(err)
		}
		p.Labels[appsv1.StatefulSetRevisionLabel] = rev
		if err := h.r.Update(ctx, &p); err != nil {
			t.Fatal(err)
		}
	}
	recorder := record.NewFakeRecorder(100)
	h.r.Recorder = recorder
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	h.r.clock = func() time.Time { return now }

	pass := func() (deleted bool) {
		t.Helper()
		var list corev1.PodList
		if err := h.r.List(ctx, &list, client.InNamespace("default")); err != nil {
			t.Fatal(err)
		}
		statuses, masterPod, _, err := h.r.reconcileRoles(ctx, h.cluster, list.Items)
		if err != nil {
			t.Fatal(err)
		}
		h.r.reconcileRollout(ctx, h.cluster, list.Items, statuses, masterPod)
		h.cluster.Status.Pods, h.cluster.Status.MasterPod = statuses, masterPod
		var after corev1.PodList
		if err := h.r.List(ctx, &after, client.InNamespace("default")); err != nil {
			t.Fatal(err)
		}
		return len(after.Items) < len(list.Items)
	}

	if pass() {
		t.Fatal("a pod was replaced while c1-1 is not in sync")
	}
	cond := rolloutCondition(h.cluster)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "WaitingForReplicaSync" {
		t.Fatalf("condition at first: %+v", cond)
	}
	for _, want := range []string{"c1-2", "c1-1", "link down", "4000000 bytes behind the master", "no I/O from the master"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("condition message %q lacks %q", cond.Message, want)
		}
	}
	if got := events(h.r); got != "" {
		t.Fatalf("events before the wait is a stall: %s", got)
	}

	now = now.Add(rolloutStallAfter + time.Second)
	if pass() {
		t.Fatal("a pod was replaced while c1-1 is not in sync")
	}
	if cond := rolloutCondition(h.cluster); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WaitingForReplicaSync" {
		t.Fatalf("condition once stalled: %+v", cond)
	}
	got := events(h.r)
	if !strings.Contains(got, "Warning RolloutWaiting") || !strings.Contains(got, "c1-1: replica not in sync: link down") {
		t.Fatalf("stall event: %q", got)
	}

	now = now.Add(time.Minute)
	pass()
	if got := events(h.r); got != "" {
		t.Fatalf("the stall was reported again a minute later: %s", got)
	}
	now = now.Add(rolloutWaitReportEvery)
	pass()
	if got := events(h.r); !strings.Contains(got, "RolloutWaiting") {
		t.Fatalf("the stall was not reported again after %s: %q", rolloutWaitReportEvery, got)
	}

	// Back in sync: c1-2 is replaced next.
	up := true
	frozen.MasterLinkUp, frozen.MasterLastIOSeconds, frozen.ReplicationOffset = &up, nil, 5_000_000
	if !pass() {
		t.Fatal("nothing replaced once c1-1 is in sync")
	}
	if cond := rolloutCondition(h.cluster); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "ReplacingPod" || !strings.Contains(cond.Message, "c1-2") {
		t.Fatalf("condition after a replacement: %+v", cond)
	}
}

// The condition goes once nothing is outdated.
func TestRolloutConditionRemovedWhenDone(t *testing.T) {
	r := &KividbClusterReconciler{}
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"}}
	r.rolloutProgressed(c, "replacing c1-2")
	if rolloutCondition(c) == nil {
		t.Fatal("no condition while replacing")
	}
	r.rolloutDone(c)
	if cond := rolloutCondition(c); cond != nil {
		t.Fatalf("condition left after the rollout: %+v", cond)
	}
}

// A wait on other pods starts the clock again.
func TestRolloutWaitOnOtherPodsIsANewWait(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	r := &KividbClusterReconciler{clock: func() time.Time { return now }, Recorder: record.NewFakeRecorder(10)}
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"}}
	r.rolloutWaiting(context.Background(), c, "c1-2", []rolloutBlocker{{pod: "c1-1", syncing: true, why: "link down"}})
	now = now.Add(rolloutStallAfter + time.Second)
	r.rolloutWaiting(context.Background(), c, "c1-0", []rolloutBlocker{{pod: "c1-2", why: "not Ready"}})
	cond := rolloutCondition(c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "WaitingForPodReady" {
		t.Fatalf("condition: %+v", cond)
	}
	if got := events(r); got != "" {
		t.Fatalf("events: %s", got)
	}
}

func TestWhyNotSynced(t *testing.T) {
	up, down := true, false
	io := int64(25)
	m := &agentapi.StatusResponse{Role: agentapi.RoleMaster, ReplicationOffset: 3_000_000}
	tests := []struct {
		name    string
		master  *agentapi.StatusResponse
		replica *agentapi.StatusResponse
		want    string
	}{
		{"agent did not answer", m, nil, "its agent did not answer"},
		{"another master", m, replicaOf("10.0.0.9", 5), "it replicates from 10.0.0.9:6380, not from the master at 10.0.0.1:6380"},
		{"lagging, silent", m, &agentapi.StatusResponse{Role: agentapi.RoleReplica, MasterHost: "10.0.0.1", MasterPort: 6380, ReplicationOffset: 1_000_000, MasterLinkUp: &up, MasterLastIOSeconds: &io},
			"link up, 2000000 bytes behind the master (offset 1000000 of 3000000), last I/O from the master 25 s ago"},
		{"first sync", m, &agentapi.StatusResponse{Role: agentapi.RoleReplica, MasterHost: "10.0.0.1", MasterPort: 6380, MasterLinkUp: &down},
			"link down, first full sync not finished (offset 0)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := whyNotSynced(tt.master, tt.replica, "10.0.0.1", 6380); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// The sharded rollout's wait on a replica whose link is down is reported
// the same way.
func TestShardedRolloutReportsAReplicaNotInSync(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	h.r.clock = func() time.Time { return now }
	_ = events(h.r)
	h.node("s-0-1").linkDown = true
	if got := h.rolloutPass(); len(got) != 1 || got[0] != "s-0-1" {
		t.Fatalf("first replaced %v", got)
	}
	_ = events(h.r)
	if got := h.rolloutPass(); len(got) > 0 {
		t.Fatalf("replaced %v while s-0-1 is not in sync", got)
	}
	cond := rolloutCondition(h.c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "WaitingForReplicaSync" ||
		!strings.Contains(cond.Message, "s-0-1: replica of shard 0 not in sync: link down") {
		t.Fatalf("condition: %+v", cond)
	}
	if got := events(h.r); strings.Contains(got, "RolloutWaiting") {
		t.Fatalf("stall reported before rolloutStallAfter: %s", got)
	}
	now = now.Add(rolloutStallAfter + time.Second)
	h.rolloutPass()
	if cond := rolloutCondition(h.c); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("condition once stalled: %+v", cond)
	}
	if got := events(h.r); !strings.Contains(got, "Warning RolloutWaiting") || !strings.Contains(got, "s-0-1") {
		t.Fatalf("stall event: %q", got)
	}
}
