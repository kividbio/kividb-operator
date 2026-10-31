package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// In the AKS run, `kubectl delete pod` of a shard master was handed over
// as asked, and then reported as a Warning ShardFailover: by the time the
// move was seen, the deleted pod was gone (or back, without a deletion
// timestamp), so nothing said the move had been planned.
func TestHandoverOfADeletedMasterIsNotAFailover(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	ctx := context.Background()
	_ = events(h.r)
	var pod corev1.Pod
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod)
	pod.Finalizers = []string{"test/hold"}
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	if err := h.r.Delete(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if !h.r.reconcileShardStepDown(ctx, h.c, observed) {
		t.Fatal("no CLUSTER FAILOVER asked for")
	}
	// The pod goes, and the StatefulSet makes it again, before the
	// operator next looks.
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod)
	pod.Finalizers = nil
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	if err := h.r.Create(ctx, shardedPod(h.c, 1, 0, "10.0.1.1")); err != nil {
		t.Fatal(err)
	}
	h.pass()
	ev := events(h.r)
	if strings.Contains(ev, "ShardFailover") || !strings.Contains(ev, "Normal ShardSwitchover shard 1: master moved from s-1-0 to s-1-1, as asked") {
		t.Fatalf("events:\n%s", ev)
	}
	if h.c.Status.LastFailoverTime != nil {
		t.Fatal("the asked-for handover was counted as a failover")
	}
}

// The console's Promote runs CLUSTER FAILOVER itself, and notes it on the
// cluster first.
func TestConsolePromoteIsNotAFailover(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	ctx := context.Background()
	_ = events(h.r)
	now := time.Now()
	h.r.clock = func() time.Time { return now }
	h.c.Annotations = map[string]string{kividbv1alpha1.ShardPromoteAnnotation: "s-0-1 " + now.Add(-3*time.Second).UTC().Format(time.RFC3339)}
	if err := h.r.Agent.ClusterFailover(ctx, "10.0.0.2"); err != nil {
		t.Fatal(err)
	}
	h.pass()
	if ev := events(h.r); strings.Contains(ev, "ShardFailover") || !strings.Contains(ev, "Normal ShardSwitchover shard 0: master moved from s-0-0 to s-0-1, as asked") {
		t.Fatalf("events:\n%s", ev)
	}

	// A note from long ago is no such request: a move now is a failover.
	h.c.Annotations[kividbv1alpha1.ShardPromoteAnnotation] = "s-1-1 " + now.Add(-time.Hour).UTC().Format(time.RFC3339)
	if err := h.r.Agent.ClusterFailover(ctx, "10.0.1.2"); err != nil {
		t.Fatal(err)
	}
	h.pass()
	if ev := events(h.r); !strings.Contains(ev, "Warning ShardFailover shard 1: master moved from s-1-0 to s-1-1") {
		t.Fatalf("events:\n%s", ev)
	}
}

func TestPromotedPod(t *testing.T) {
	now := time.Date(2026, 10, 31, 7, 6, 15, 0, time.UTC)
	c := shardedCluster("s", 2, 1)
	for v, want := range map[string]string{
		"":                           "",
		"s-1-0":                      "",
		"s-1-0 yesterday":            "",
		"s-1-0 2026-10-31T07:06:14Z": "s-1-0",
		"s-1-0 2026-10-31T07:00:00Z": "",
		"s-1-0 2026-11-01T07:06:14Z": "", // a clock far ahead
	} {
		c.Annotations = map[string]string{kividbv1alpha1.ShardPromoteAnnotation: v}
		if got := promotedPod(c, now); got != want {
			t.Errorf("%q: %q, want %q", v, got, want)
		}
	}
}

// client-go's defaults (25 Events per object, then one every 5 minutes)
// dropped PasswordChanged after a sharded rollout.
func TestEventRateLimitLeavesRoomForARollout(t *testing.T) {
	o := EventCorrelatorOptions()
	if o.BurstSize < 4*maxShardedNodes || o.QPS < 1.0/30 {
		t.Fatalf("%+v: too tight for a rollout of %d pods", o, maxShardedNodes)
	}
}
