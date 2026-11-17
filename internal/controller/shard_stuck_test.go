package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// podState is what the kubelet reports of a pod, for these tests.
type podState struct {
	phase      corev1.PodPhase
	containers []corev1.ContainerStatus // the kividb container is the one named so
	inits      []corev1.ContainerStatus
}

func ago(d time.Duration) metav1.Time { return metav1.NewTime(time.Now().Add(-d)) }

func kividbStatus(state corev1.ContainerState, restarts int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: kividbContainerName, State: state, RestartCount: restarts}
}

func waitingIn(reason string) corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
}

func runningFor(d time.Duration) corev1.ContainerState {
	return corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(d)}}
}

// setUnready marks pod not Ready since unready ago, in state st.
func setUnready(t *testing.T, h *formingHarness, name string, unready time.Duration, st podState) {
	t.Helper()
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: name}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse,
		LastTransitionTime: ago(unready)}}
	if st.phase != "" {
		pod.Status.Phase = st.phase
	}
	pod.Status.ContainerStatuses = st.containers
	pod.Status.InitContainerStatuses = st.inits
	if err := h.r.Status().Update(ctx, &pod); err != nil {
		if err := h.r.Update(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
}

// onNewTemplate moves pod onto the current template, as if replaced already.
func onNewTemplate(t *testing.T, h *formingHarness, name string) {
	t.Helper()
	ctx := context.Background()
	var pod corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: name}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Labels[appsv1.StatefulSetRevisionLabel] = "new"
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
}

// A master the rollout holds (no replica, no annotation: one not Ready is
// not heard from, so held whatever the KividbConfig says) is replaced as a
// stuck pod only when its engine holds nothing in memory: its kividb
// container (found by name) is anything but running, it never got one, or
// it restarted within stuckPodGrace (a crash loop runs for a moment between
// restarts). One that runs, loading a large snapshot or too busy to answer
// its probe, stays held pass after pass, and the hold says it is alive but
// not Ready and how to restart it alone (#113).
func TestShardedRolloutReplacesAHeldMasterOnlyIfItsEngineIsDead(t *testing.T) {
	agentCrashing := corev1.ContainerStatus{Name: "agent", State: waitingIn("CrashLoopBackOff"), RestartCount: 7}
	initFailing := corev1.ContainerStatus{Name: "init", State: waitingIn("CrashLoopBackOff"), RestartCount: 3}
	for _, tc := range []struct {
		name     string
		st       podState
		replaced bool
	}{
		{"a crash loop", podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("CrashLoopBackOff"), 5)}}, true},
		{"an image it cannot pull", podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("ImagePullBackOff"), 0)}}, true},
		{"ErrImagePull", podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("ErrImagePull"), 0)}}, true},
		{"CreateContainerConfigError", podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("CreateContainerConfigError"), 0)}}, true},
		{"PodInitializing behind a failing init container", podState{phase: corev1.PodPending,
			inits:      []corev1.ContainerStatus{initFailing},
			containers: []corev1.ContainerStatus{kividbStatus(waitingIn("PodInitializing"), 0)}}, true},
		{"terminated", podState{containers: []corev1.ContainerStatus{kividbStatus(corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}, 0)}}, true},
		{"never got a container", podState{phase: corev1.PodPending}, true},
		{"its container still being created", podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("ContainerCreating"), 0)}}, true},
		{"phase Failed, no statuses", podState{phase: corev1.PodFailed}, true},
		{"restarted within the grace", podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(5*time.Second), 4)}}, true},
		{"running", podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}}, false},
		{"running, restarted long ago", podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 1)}}, false},
		{"running, restarted exactly stuckPodGrace ago", podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(stuckPodGrace), 1)}}, false},
		{"running, its agent crashing and listed first", podState{containers: []corev1.ContainerStatus{
			agentCrashing, kividbStatus(runningFor(10*time.Minute), 0)}}, false},
		{"phase Unknown, a stale Running status (a lost node)", podState{phase: corev1.PodUnknown,
			containers: []corev1.ContainerStatus{kividbStatus(runningFor(time.Hour), 0)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForRollout(t, 2, 0)
			onNewTemplate(t, h, shardPodName(h.c, 1, 0))
			setUnready(t, h, shardPodName(h.c, 0, 0), 2*stuckPodGrace, tc.st)
			_ = events(h.r)
			if tc.replaced {
				if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
					t.Fatalf("replaced %v, want s-0-0 replaced as a stuck pod", deleted)
				}
				if ev := events(h.r); !strings.Contains(ev, "StuckPodReplaced") {
					t.Fatalf("events:\n%s\nwant StuckPodReplaced", ev)
				}
				return
			}
			for i := 0; i < 5; i++ {
				if deleted := h.rolloutPass(); len(deleted) > 0 {
					t.Fatalf("pass %d replaced %v, want s-0-0 held", i, deleted)
				}
			}
			cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
			if cond == nil || cond.Reason != "WouldLoseData" ||
				!strings.Contains(cond.Message, "it is not Ready, so whether its append-only file is on is not known") ||
				!strings.Contains(cond.Message, "s-0-0 is alive but not Ready") ||
				!strings.Contains(cond.Message, "delete pod s-0-0 by hand") {
				t.Fatalf("condition %+v, want WouldLoseData saying s-0-0 is not Ready, alive, and how to restart it alone", cond)
			}
			if ev := events(h.r); strings.Contains(ev, "StuckPodReplaced") || strings.Count(ev, "RolloutHeld") != 1 {
				t.Fatalf("events:\n%s\nwant one RolloutHeld and no StuckPodReplaced", ev)
			}
		})
	}
}

// A master that loads, or hangs, has its slots seen as failed by the others
// after the node timeout: the cluster state is then fail, and the pass
// stops before its usual checks. The hold of an alive, not-Ready master is
// reported before that, so that it is not silent where it matters most.
func TestShardedRolloutReportsAnAliveHeldMasterWhileTheClusterFails(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	onNewTemplate(t, h, shardPodName(h.c, 1, 0))
	setUnready(t, h, shardPodName(h.c, 0, 0), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	for _, n := range h.engine.nodes {
		n.clusterFail = true
	}
	for i := 0; i < 3; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want s-0-0 held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0 is alive but not Ready") {
		t.Fatalf("condition %+v, want WouldLoseData saying s-0-0 is alive but not Ready", cond)
	}
}

// Two held masters not Ready, one alive and one dead: the dead one is
// replaced as a stuck pod, and the hold then names the alive one alone.
func TestShardedRolloutReplacesTheDeadOfTwoHeldMasters(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	setUnready(t, h, shardPodName(h.c, 0, 0), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	setUnready(t, h, shardPodName(h.c, 1, 0), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(waitingIn("CrashLoopBackOff"), 9)}})
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-1-0]" {
		t.Fatalf("replaced %v, want the dead s-1-0", deleted)
	}
	for i := 0; i < 3; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want s-0-0 held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0") || strings.Contains(cond.Message, "s-1-0") {
		t.Fatalf("condition %+v, want WouldLoseData naming s-0-0 alone", cond)
	}
}

// With its file on in the KividbConfig, an alive master that is not Ready
// is held too, and that is intended: the pass did not hear from it, so
// whether its engine has the file off (CONFIG SET appendonly no) is not
// known, and the rollout does not restart on a guess (#110).
func TestShardedRolloutHoldsAnAliveUnreadyMasterWithTheFileOn(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.kdbConfig = &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "aof", Namespace: "default"},
		Spec: kividbv1alpha1.KividbConfigSpec{Directives: map[string]string{"appendonly": "yes"}}}
	onNewTemplate(t, h, shardPodName(h.c, 1, 0))
	setUnready(t, h, shardPodName(h.c, 0, 0), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	if deleted := h.rolloutPass(); len(deleted) > 0 {
		t.Fatalf("replaced %v, want s-0-0 held", deleted)
	}
}

// Before stuckPodGrace a pod is not eligible to be replaced as stuck either
// way: the hold says it is not Ready, but not yet that it is kept from the
// stuck-pod pass.
func TestShardedRolloutSaysNothingOfTheStuckPassBeforeTheGrace(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	onNewTemplate(t, h, shardPodName(h.c, 1, 0))
	setUnready(t, h, shardPodName(h.c, 0, 0), 5*time.Second,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	h.rolloutPass()
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "it is not Ready") ||
		strings.Contains(cond.Message, "alive but not Ready") {
		t.Fatalf("condition %+v, want WouldLoseData saying s-0-0 is not Ready, without the stuck-pod sentence", cond)
	}
}

// The annotation lets the rollout restart a held master anyway: one that
// runs but is not Ready is replaced as a stuck pod, as before.
func TestShardedRolloutAnnotationReplacesAnUnreadyMasterThatRuns(t *testing.T) {
	h := formedForRollout(t, 2, 0)
	h.allowRestartWithoutPersistence()
	onNewTemplate(t, h, shardPodName(h.c, 1, 0))
	setUnready(t, h, shardPodName(h.c, 0, 0), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-0]" {
		t.Fatalf("replaced %v, want s-0-0", deleted)
	}
}

// With replicas, nothing is held: an unready pod on an old template is
// replaced as a stuck pod after stuckPodGrace whatever its container does,
// as before.
func TestShardedRolloutReplacesAnUnreadyPodThatRunsWithReplicas(t *testing.T) {
	h := formedForRollout(t, 1, 1)
	setUnready(t, h, shardPodName(h.c, 0, 1), 2*stuckPodGrace,
		podState{containers: []corev1.ContainerStatus{kividbStatus(runningFor(10*time.Minute), 0)}})
	if deleted := h.rolloutPass(); fmt.Sprint(deleted) != "[s-0-1]" {
		t.Fatalf("replaced %v, want the unready replica s-0-1", deleted)
	}
}
