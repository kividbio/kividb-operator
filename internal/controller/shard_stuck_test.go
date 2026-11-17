package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// onlyMasterLeftUnready leaves s-0-0 the only pod on the old template, not
// Ready for twice stuckPodGrace, with its kividb container in the state
// given (none: the pod never got one).
func onlyMasterLeftUnready(t *testing.T, h *formingHarness, kividb *corev1.ContainerStatus) {
	t.Helper()
	ctx := context.Background()
	var other corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 1, 0)}, &other); err != nil {
		t.Fatal(err)
	}
	other.Labels[appsv1.StatefulSetRevisionLabel] = "new"
	if err := h.r.Update(ctx, &other); err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 0)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * stuckPodGrace))}}
	pod.Status.ContainerStatuses = nil
	if kividb != nil {
		kividb.Name = kividbContainerName
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{*kividb}
	}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
}

// A master the rollout holds (no replica, no annotation: one not Ready is
// not heard from, so held whatever the KividbConfig says) is replaced as a
// stuck pod only when its engine holds nothing in memory: its kividb
// container is not running (a crash loop, an image it cannot pull,
// terminated), it never got one (unschedulable, still being created), or it
// restarted within stuckPodGrace (a crash loop runs for a moment between
// restarts). One that runs, loading a large snapshot or too busy to answer
// its probe, stays held, and the hold says it is alive but not Ready (#113).
func TestShardedRolloutReplacesAHeldMasterOnlyIfItsEngineIsDead(t *testing.T) {
	ago := func(d time.Duration) metav1.Time { return metav1.NewTime(time.Now().Add(-d)) }
	waiting := func(reason string) *corev1.ContainerStatus {
		return &corev1.ContainerStatus{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}}
	}
	running := func(restarts int32, started time.Duration) *corev1.ContainerStatus {
		return &corev1.ContainerStatus{RestartCount: restarts,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(started)}}}
	}
	for _, tc := range []struct {
		name     string
		kividb   *corev1.ContainerStatus
		replaced bool
	}{
		{"a crash loop", waiting("CrashLoopBackOff"), true},
		{"an image it cannot pull", waiting("ImagePullBackOff"), true},
		{"terminated", &corev1.ContainerStatus{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}, true},
		{"never got a container", nil, true},
		{"its container still being created", waiting("ContainerCreating"), true},
		{"restarted within the grace", running(4, 5*time.Second), true},
		{"running", running(0, 10*time.Minute), false},
		{"running, restarted long ago", running(1, 10*time.Minute), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 2, 0)
			onlyMasterLeftUnready(t, h, tc.kividb)
			_ = events(h.r)
			for i := 0; i < 2; i++ {
				deleted := h.rolloutPass()
				if tc.replaced {
					if i == 0 && fmt.Sprint(deleted) != "[s-0-0]" {
						t.Fatalf("replaced %v, want s-0-0 replaced as a stuck pod", deleted)
					}
					break
				}
				if len(deleted) > 0 {
					t.Fatalf("pass %d replaced %v, want s-0-0 held", i, deleted)
				}
			}
			ev := events(h.r)
			if tc.replaced {
				if !strings.Contains(ev, "StuckPodReplaced") {
					t.Fatalf("events:\n%s\nwant StuckPodReplaced", ev)
				}
				return
			}
			cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
			if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0 is alive but not Ready") ||
				!strings.Contains(cond.Message, "it is not Ready, so whether its append-only file is on is not known") {
				t.Fatalf("condition %+v, want WouldLoseData saying s-0-0 is not Ready, and alive", cond)
			}
			if strings.Contains(ev, "StuckPodReplaced") {
				t.Fatalf("events:\n%s\nwant no StuckPodReplaced", ev)
			}
		})
	}
}

// The annotation lets the rollout restart a held master anyway: one that
// runs but is not Ready is replaced as a stuck pod, as before.
func TestShardedRolloutAnnotationReplacesAnUnreadyMasterThatRuns(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.allowRestartWithoutPersistence()
	onlyMasterLeftUnready(t, h, &corev1.ContainerStatus{
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now().Add(-10 * time.Minute))}}})
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want s-0-0", deleted)
	}
}

// With replicas, nothing is held: an unready pod on an old template is
// replaced as a stuck pod after stuckPodGrace whatever its container does,
// as before.
func TestShardedRolloutReplacesAnUnreadyPodThatRunsWithReplicas(t *testing.T) {
	h := formedForRollout(t, 1, 1)
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardPodName(h.c, 0, 1)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * stuckPodGrace))}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: kividbContainerName,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now().Add(-10 * time.Minute))}}}}
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-1]" {
		t.Fatalf("replaced %v, want the unready replica s-0-1", deleted)
	}
}
