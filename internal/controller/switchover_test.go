package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// newSwitchoverHarness is a master c1-0 (10.0.0.1) at offset 100 with two
// replicas in sync, c1-1 at 100 and c1-2 at 90, and the statuses the last
// reconcile found.
func newSwitchoverHarness(t *testing.T) (*rolesHarness, []kividbv1alpha1.KividbPodStatus) {
	t.Helper()
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: master(100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 90)},
	)
	h.r.Recorder = record.NewFakeRecorder(100)
	statuses := []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 100},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 100},
		{Name: "c1-2", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 90},
	}
	return h, statuses
}

func (h *rolesHarness) list() []corev1.Pod {
	h.t.Helper()
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

func shortSwitchoverWaits(t *testing.T) {
	t.Helper()
	timeout, poll := switchoverCatchUpTimeout, switchoverPoll
	switchoverCatchUpTimeout, switchoverPoll = 150*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { switchoverCatchUpTimeout, switchoverPoll = timeout, poll })
}

// The whole sequence: writes held on the old master, the target promoted
// only once it has applied everything, the old master and the other
// replica pointed at it, the old master's clients sent to the new one, and
// the writes let go last.
func TestSwitchoverHoldsWritesUntilTheTargetHasCaughtUp(t *testing.T) {
	shortSwitchoverWaits(t)
	h, statuses := newSwitchoverHarness(t)
	h.agents.status["10.0.0.2"].ReplicationOffset = 80
	h.agents.catchUp = map[string]int{"10.0.0.2": 3} // three looks behind, then caught up

	got, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"})
	if err != nil || got != "c1-1" {
		t.Fatalf("switchover = %q, %v; want c1-1", got, err)
	}
	want := []string{
		"exec 10.0.0.1 CLIENT PAUSE 5150 WRITE",
		"promote 10.0.0.2",
		"replicaof 10.0.0.1 -> 10.0.0.2",
		"replicaof 10.0.0.3 -> 10.0.0.2",
		"exec 10.0.0.1 CLIENT KILL TYPE normal",
		"exec 10.0.0.1 CLIENT UNPAUSE",
	}
	if fmt.Sprint(h.agents.calls) != fmt.Sprint(want) {
		t.Fatalf("calls\n %v\nwant\n %v", h.agents.calls, want)
	}
	if h.agents.catchUp["10.0.0.2"] != 0 {
		t.Fatalf("promoted before the replica had caught up")
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	if len(h.agents.paused) != 0 {
		t.Fatalf("still paused: %v", h.agents.paused)
	}
	if ev := events(h.r); !strings.Contains(ev, "Switchover moved the master role from c1-0 to c1-1 (rollout)") {
		t.Fatalf("events: %s", ev)
	}
}

// A target that does not catch up in time: the writes are let go again,
// nothing else has changed, and the next attempt waits a while -- unless
// the master is going away anyway.
func TestSwitchoverAbortsWhenTheTargetDoesNotCatchUp(t *testing.T) {
	shortSwitchoverWaits(t)
	h, statuses := newSwitchoverHarness(t)
	h.agents.status["10.0.0.2"].ReplicationOffset = 80
	h.agents.catchUp = map[string]int{"10.0.0.2": -1}
	now := time.Now()
	h.r.clock = func() time.Time { return now }

	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); err == nil {
		t.Fatal("switchover succeeded, want it aborted")
	}
	want := []string{"exec 10.0.0.1 CLIENT PAUSE 5150 WRITE", "exec 10.0.0.1 CLIENT UNPAUSE"}
	if fmt.Sprint(h.agents.calls) != fmt.Sprint(want) {
		t.Fatalf("calls %v, want %v", h.agents.calls, want)
	}
	h.assertLabels(map[string]string{"c1-0": "master", "c1-1": "replica", "c1-2": "replica"})
	h.assertRole("10.0.0.1", agentapi.RoleMaster, "")
	if ev := events(h.r); !strings.Contains(ev, "SwitchoverAborted") || !strings.Contains(ev, "the replica is at offset 80, the master at 100") {
		t.Fatalf("events: %s", ev)
	}

	h.agents.calls = nil
	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); !errors.Is(err, errSwitchoverDeferred) || len(h.agents.calls) != 0 {
		t.Fatalf("a second attempt right away: %v, calls %v; want it deferred, writes untouched", err, h.agents.calls)
	}
	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "deleted", urgent: true}); errors.Is(err, errSwitchoverDeferred) {
		t.Fatalf("an urgent attempt was deferred")
	}
	now = now.Add(switchoverRetryAfter)
	h.agents.calls = nil
	h.agents.catchUp["10.0.0.2"] = 0
	if got, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); err != nil || got != "c1-1" {
		t.Fatalf("after the wait: %q, %v", got, err)
	}
}

// A replica whose link is down is never handed the role, and writes are
// not even held for it.
func TestSwitchoverRefusesAReplicaWhoseLinkIsDown(t *testing.T) {
	h, statuses := newSwitchoverHarness(t)
	down := false
	h.agents.status["10.0.0.2"].MasterLinkUp = &down
	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); err == nil {
		t.Fatal("switched over to a replica whose link is down")
	}
	if len(h.agents.calls) != 0 {
		t.Fatalf("calls %v, want none", h.agents.calls)
	}
}

func TestSwitchoverToTheRequestedReplica(t *testing.T) {
	h, statuses := newSwitchoverHarness(t)
	got, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "requested", preferred: "c1-2"})
	if err != nil || got != "c1-2" {
		t.Fatalf("switchover = %q, %v; want c1-2, not the most caught-up c1-1", got, err)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "replica", "c1-2": "master"})

	statuses[1].Synced = false
	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-2", switchoverRequest{reason: "requested", preferred: "c1-1"}); err == nil {
		t.Fatal("switched over to a replica that is not in sync")
	}
}

// The old master cannot be made a replica: the target is given its old
// role back and the old master goes on, with all the writes.
func TestSwitchoverIsUndoneWhenTheOldMasterCannotFollow(t *testing.T) {
	h, statuses := newSwitchoverHarness(t)
	h.agents.failReplicaOf = map[string]bool{"10.0.0.1": true}
	if _, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); err == nil {
		t.Fatal("switchover succeeded")
	}
	h.assertRole("10.0.0.2", agentapi.RoleReplica, "10.0.0.1")
	h.assertLabels(map[string]string{"c1-0": "master", "c1-1": "replica"})
	if len(h.agents.paused) != 0 {
		t.Fatalf("still paused: %v", h.agents.paused)
	}
}

// An engine without CLIENT PAUSE ... WRITE (before kividb 1.1) is still
// switched over, as before, and the cost is said out loud.
func TestSwitchoverWithoutAWritePause(t *testing.T) {
	h, statuses := newSwitchoverHarness(t)
	h.agents.noPause = true
	if got, err := h.r.switchover(context.Background(), h.cluster, h.list(), statuses, "c1-0", switchoverRequest{reason: "rollout"}); err != nil || got != "c1-1" {
		t.Fatalf("switchover = %q, %v", got, err)
	}
	if ev := events(h.r); !strings.Contains(ev, "SwitchoverWithoutPause") {
		t.Fatalf("events: %s", ev)
	}
}

// The master label goes onto the new master before it comes off the old
// one: the master Service never has no endpoint.
func TestSwitchoverMovesTheLabelWithoutAGap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"}, Spec: kividbv1alpha1.KividbClusterSpec{Replicas: 1}}
	pod := func(name, ip, role string) *corev1.Pod {
		labels := selectorLabels(c)
		labels[kividbv1alpha1.RoleLabel] = role
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Status:     corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
	}
	var patched []string
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod("c1-0", "10.0.0.1", "master"), pod("c1-1", "10.0.0.2", "replica")).
		WithInterceptorFuncs(interceptor.Funcs{Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patched = append(patched, obj.GetName()+"="+obj.GetLabels()[kividbv1alpha1.RoleLabel])
			return cl.Patch(ctx, obj, patch, opts...)
		}}).Build()
	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{"10.0.0.1": master(100), "10.0.0.2": replicaOf("10.0.0.1", 100)}}
	r := &KividbClusterReconciler{Client: cl, Scheme: scheme, Agent: &AgentClient{http: &http.Client{Transport: agents}}}
	var list corev1.PodList
	if err := cl.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	statuses := []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true},
	}
	if _, err := r.switchover(context.Background(), c, list.Items, statuses, "c1-0", switchoverRequest{reason: "rollout"}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(patched) != "[c1-1=master c1-0=replica]" {
		t.Fatalf("label patches %v, want the new master labelled first", patched)
	}
}

func handoverHarness(t *testing.T) (*rolesHarness, []kividbv1alpha1.KividbPodStatus) {
	t.Helper()
	h, statuses := newSwitchoverHarness(t)
	scheme := h.r.Scheme
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	var objs []client.Object
	for _, p := range h.list() {
		objs = append(objs, p.DeepCopy())
	}
	objs = append(objs, h.cluster)
	h.r.Client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return h, statuses
}

// A master pod being deleted -- by hand, a drain, an eviction -- hands its
// role over before it stops (its preStop hook waits for that).
func TestHandoverWhenTheMasterPodIsDeleted(t *testing.T) {
	h, statuses := handoverHarness(t)
	ctx := context.Background()
	var p corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	p.Finalizers = []string{"test/hold"} // so the fake keeps it, terminating
	if err := h.r.Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
	if err := h.r.Delete(ctx, &p); err != nil {
		t.Fatal(err)
	}
	if got := h.r.reconcileHandover(ctx, h.cluster, h.list(), statuses, "c1-0"); got != "c1-1" {
		t.Fatalf("handover = %q, want c1-1", got)
	}
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	if ev := events(h.r); !strings.Contains(ev, "the master pod is being deleted") {
		t.Fatalf("events: %s", ev)
	}
}

// The GUI's promote: the cluster's switchover-to annotation, removed once
// the role has moved.
func TestHandoverToTheAnnotatedPod(t *testing.T) {
	h, statuses := handoverHarness(t)
	ctx := context.Background()
	h.cluster.Annotations = map[string]string{SwitchoverToAnnotation: "c1-2"}
	if err := h.r.Update(ctx, h.cluster); err != nil {
		t.Fatal(err)
	}
	if got := h.r.reconcileHandover(ctx, h.cluster, h.list(), statuses, "c1-0"); got != "c1-2" {
		t.Fatalf("handover = %q, want c1-2", got)
	}
	var c kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, client.ObjectKeyFromObject(h.cluster), &c); err != nil {
		t.Fatal(err)
	}
	if _, still := c.Annotations[SwitchoverToAnnotation]; still {
		t.Fatalf("the request was not removed: %v", c.Annotations)
	}

	// A request for a pod that does not exist is dropped, with a warning.
	h.cluster.Annotations = map[string]string{SwitchoverToAnnotation: "c1-9"}
	if err := h.r.Update(ctx, h.cluster); err != nil {
		t.Fatal(err)
	}
	h.agents.calls = nil
	if got := h.r.reconcileHandover(ctx, h.cluster, h.list(), statuses, "c1-2"); got != "" || len(h.agents.calls) != 0 {
		t.Fatalf("handover = %q, calls %v; want nothing", got, h.agents.calls)
	}
	if ev := events(h.r); !strings.Contains(ev, "SwitchoverRequestDropped") {
		t.Fatalf("events: %s", ev)
	}
}

func TestHandoverOnStepDownRemovesTheAnnotation(t *testing.T) {
	h, statuses := handoverHarness(t)
	ctx := context.Background()
	var p corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	p.Annotations = map[string]string{StepDownAnnotation: "true"}
	if err := h.r.Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
	if got := h.r.reconcileHandover(ctx, h.cluster, h.list(), statuses, "c1-0"); got != "c1-1" {
		t.Fatalf("handover = %q, want c1-1", got)
	}
	if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	if p.Annotations[StepDownAnnotation] != "" {
		t.Fatalf("step-down annotation left: %v", p.Annotations)
	}
}
