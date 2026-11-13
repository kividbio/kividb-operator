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

// A master that is gone, when no replica was in sync with it (#75): the
// one replica of a replicas: 1 cluster in the middle of its full resync,
// say, holding the snapshot it was sent but not the writes queued after
// it. The master's volume holds those writes. Promoting the replica and
// then resyncing the master from it when it comes back loses them.

// unsynced marks every replica out of sync with the master, as the last
// pass that could tell found them.
func (h *rolesHarness) unsynced() {
	for i := range h.cluster.Status.Pods {
		if h.cluster.Status.Pods[i].Role == kividbv1alpha1.RoleReplica {
			h.cluster.Status.Pods[i].Synced = false
		}
	}
}

// annotate sets an annotation on the stored cluster and on h.cluster,
// leaving h.cluster's status as the test made it (an Update of h.cluster
// itself would load the stored status into it).
func (h *rolesHarness) annotate(name, value string) {
	h.t.Helper()
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		h.t.Fatal(err)
	}
	if stored.Annotations == nil {
		stored.Annotations = map[string]string{}
	}
	stored.Annotations[name] = value
	if err := h.r.Update(context.Background(), &stored); err != nil {
		h.t.Fatal(err)
	}
	h.cluster.Annotations, h.cluster.ResourceVersion = stored.Annotations, stored.ResourceVersion
}

// The failover waits for the master for as long as it takes, not only for
// restartWaitSeconds, says so once, and keeps the master when it comes back.
func TestFailover_WaitsForAMasterNoReplicaWasInSyncWith(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(16 * time.Second)
	h.assertWaiting("once restartWaitSeconds have passed")
	*now = now.Add(time.Hour)
	h.assertWaiting("an hour later")
	events := eventsWith(rec, "")
	var blocked []string
	for _, e := range events {
		if strings.Contains(e, "FailoverBlocked") {
			blocked = append(blocked, e)
		}
		if strings.Contains(e, "failing over") {
			t.Errorf("event %q says it fails over; want it waiting", e)
		}
	}
	if len(blocked) != 1 || !strings.Contains(blocked[0], "in sync") || !strings.Contains(blocked[0], kividbv1alpha1.AllowUnsyncedFailoverAnnotation) {
		t.Errorf("events %v, want one FailoverBlocked saying no replica was in sync, and naming %s", events, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	}

	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(5)})
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 kept once it came back", m, failover)
	}
	h.assertNoCall("promote")
}

// kividb.io/allow-unsynced-failover lets that failover go ahead once
// restartWaitSeconds have passed. A Warning says what is lost, and the
// operator removes the annotation, so a later failover waits again.
func TestFailover_TheAnnotationLetsAFailoverGoAheadWithNoReplicaInSync(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(16 * time.Second)
	h.annotate(kividbv1alpha1.AllowUnsyncedFailoverAnnotation, "true")
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverForced"); len(e) != 1 || !strings.Contains(e[0], "c1-1") ||
		!strings.Contains(e[0], kividbv1alpha1.AllowUnsyncedFailoverAnnotation) || !strings.Contains(e[0], "removed") {
		t.Errorf("events %v, want one FailoverForced naming c1-1 and the annotation, and saying it is removed", e)
	}
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	if v, ok := stored.Annotations[kividbv1alpha1.AllowUnsyncedFailoverAnnotation]; ok {
		t.Errorf("%s is still %q after the failover it allowed", kividbv1alpha1.AllowUnsyncedFailoverAnnotation, v)
	}
}

// What else ends the wait for a master that is gone -- restartWaitSeconds
// 0, or a replacement pod that cannot be scheduled -- does not end it when
// no replica was in sync: promoting one would still lose the writes.
func TestFailover_WaitsForAMasterNoReplicaWasInSyncWithWhateverElseEndsTheWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(h *rolesHarness)
	}{
		{"restartWaitSeconds 0", func(h *rolesHarness) { h.noRestartWait() }},
		{"replacement cannot be scheduled", func(h *rolesHarness) {
			h.addPod(testPod{name: "c1-0", uid: "new", unschedulable: true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newGoneMasterHarness(t)
			h.unsynced()
			tc.edit(h)
			h.assertWaiting("with no replica in sync")
		})
	}
}

// A master that restarts in place -- the same pod, its engine killed for
// its memory, say -- is failed away from once unhealthyThresholdSeconds
// have passed. Not when no replica was in sync with it: it reloads the
// writes they lack from its volume. It is waited for until it is back, and
// kept.
func TestFailover_WaitsForAMasterRestartingInPlaceNoReplicaWasInSyncWith(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	rec := h.recordEvents()
	h.cluster.Status.Pods[0].Incarnation = "same/0"
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.1", label: "master", uid: "same", unreadyFor: time.Minute})
	h.assertWaiting("past unhealthyThresholdSeconds")
	*now = now.Add(time.Hour)
	h.assertWaiting("an hour later")
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "in sync") {
		t.Errorf("events %v, want one FailoverBlocked saying no replica was in sync", e)
	}

	var p corev1.Pod
	if err := h.r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	if err := h.r.Delete(context.Background(), &p); err != nil {
		t.Fatal(err)
	}
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, uid: "same", agent: backOnItsVolume(5)})
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 kept once it is back", m, failover)
	}
	h.assertNoCall("promote")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "came back") {
		t.Errorf("events %v, want one FailoverWait saying it came back", e)
	}
}

// The annotation lets that failover go ahead too.
func TestFailover_TheAnnotationLetsAFailoverFromAMasterRestartingInPlaceGoAhead(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.unsynced()
	h.cluster.Status.Pods[0].Incarnation = "same/0"
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.1", label: "master", uid: "same", unreadyFor: time.Minute})
	h.assertWaiting("without the annotation")
	h.annotate(kividbv1alpha1.AllowUnsyncedFailoverAnnotation, "true")
	h.assertFailoverTo("c1-1")
}

// A replica that was in sync is promoted once restartWaitSeconds have
// passed, as before, even if another was not.
func TestFailover_PromotesTheReplicaThatWasInSync(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.cluster.Status.Pods[1].Synced = false // c1-1 resyncing; c1-2 in sync
	h.assertWaiting("at once")
	*now = now.Add(16 * time.Second)
	h.assertFailoverTo("c1-2")
}

// A master whose engine still answers is held by the fence, and the replica
// promoted catches up with it first: it need not have been in sync.
func TestFailover_FencesAMasterThatStillAnswersWithNoReplicaInSync(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.unsynced()
	h.agents.status["10.0.0.1"] = masterRun(100, "stream-a", "run-0")
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}
