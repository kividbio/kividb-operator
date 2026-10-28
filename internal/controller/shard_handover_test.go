package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestHandoverStep(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	h := &shardHandover{master: "m"}
	if ask, warn := h.step(t0); !ask || warn {
		t.Fatalf("new handover: ask=%v warn=%v, want ask", ask, warn)
	}
	h.attempts, h.last = 1, t0
	if ask, warn := h.step(t0.Add(10 * time.Second)); ask || warn {
		t.Fatalf("10 s after asking: ask=%v warn=%v, want neither", ask, warn)
	}
	if ask, warn := h.step(t0.Add(failoverAbandonedAfter)); ask || !warn {
		t.Fatalf("15 s after asking: ask=%v warn=%v, want warn only", ask, warn)
	}
	h.warned = true
	if ask, warn := h.step(t0.Add(failoverRetry)); !ask || warn {
		t.Fatalf("20 s after asking: ask=%v warn=%v, want ask only", ask, warn)
	}
	// Warned and asked again in the same step when no reconcile fell
	// between 15 and 20 s.
	h.warned = false
	if ask, warn := h.step(t0.Add(time.Minute)); !ask || !warn {
		t.Fatalf("a minute after asking, not warned: ask=%v warn=%v, want both", ask, warn)
	}
	h.attempts, h.warned = maxFailoverAttempts, true
	if ask, _ := h.step(t0.Add(time.Minute)); ask || !h.exhausted() {
		t.Fatal("asked again after the last attempt")
	}
	if ask, _ := h.step(t0.Add(failoverGiveUpCooldown)); !ask || h.attempts != 0 {
		t.Fatalf("no new round after the cool-down: attempts %d", h.attempts)
	}
}

// A replica that gives every manual failover up: one Normal event, a
// Warning for each abandoned attempt, at most maxFailoverAttempts
// requests, and the state in the ShardSwitchover condition.
func TestShardSwitchoverAbandoned(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	_ = events(h.r)
	now := time.Unix(2_000_000, 0)
	h.r.clock = func() time.Time { return now }
	h.engine.abandonFailovers = true

	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	if err := h.r.requestStepDown(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	pass := func() {
		observed := h.r.observeShardedPods(ctx, h.c, h.pods())
		h.r.reconcileShardStepDown(ctx, h.c, observed)
	}
	failovers := func() int {
		n := 0
		for _, c := range h.engine.calls {
			if strings.HasPrefix(c, "/cluster/failover") {
				n++
			}
		}
		return n
	}

	pass()
	if got := failovers(); got != 1 {
		t.Fatalf("%d CLUSTER FAILOVERs, want 1", got)
	}
	cond := h.r.switchoverCondition(h.c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "InProgress" || !strings.Contains(cond.Message, "shard 1: s-1-0 handing over to s-1-1 (attempt 1 of 5)") {
		t.Fatalf("condition after the first request: %+v", cond)
	}
	for i := 0; i < 40; i++ {
		now = now.Add(5 * time.Second)
		pass()
	}
	if got := failovers(); got != maxFailoverAttempts {
		t.Fatalf("%d CLUSTER FAILOVERs, want %d", got, maxFailoverAttempts)
	}
	ev := events(h.r)
	if n := strings.Count(ev, "Normal ShardSwitchover "); n != 1 {
		t.Fatalf("%d Normal ShardSwitchover events, want 1:\n%s", n, ev)
	}
	if n := strings.Count(ev, "Warning ShardSwitchoverAbandoned"); n != maxFailoverAttempts {
		t.Fatalf("%d abandoned warnings, want %d:\n%s", n, maxFailoverAttempts, ev)
	}
	if !strings.Contains(ev, "gave up after 5 attempts") {
		t.Fatalf("no give-up in the events:\n%s", ev)
	}
	cond = h.r.switchoverCondition(h.c)
	if cond == nil || cond.Reason != "Stalled" || !strings.Contains(cond.Message, "did not hand over in 5 attempts") {
		t.Fatalf("condition after giving up: %+v", cond)
	}

	// The replica takes over at last (after the cool-down, a new round):
	// the handover is over and the condition goes.
	h.engine.abandonFailovers = false
	now = now.Add(failoverGiveUpCooldown)
	pass()
	if len(h.node("s-1-1").slots) != 1 {
		t.Fatal("no new request after the cool-down")
	}
	h.r.clearShardStepDowns(ctx, h.r.observeShardedPods(ctx, h.c, h.pods()))
	pass()
	if cond := h.r.switchoverCondition(h.c); cond != nil {
		t.Fatalf("condition after the handover: %+v", cond)
	}
}
