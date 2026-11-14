package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A master that is gone, when no replica was in sync with it (#75): the
// one replica of a replicas: 1 cluster in the middle of its full resync,
// say, holding the snapshot it was sent but not the writes queued after
// it. The master's volume holds those writes. Promoting the replica and
// then resyncing the master from it when it comes back loses them.

// unsynced marks every replica out of sync with the master, as the last
// pass that could tell found them, with the append-only file on: the
// master's volume holds what it acknowledged.
func (h *rolesHarness) unsynced() {
	for i := range h.cluster.Status.Pods {
		if h.cluster.Status.Pods[i].Role == kividbv1alpha1.RoleReplica {
			h.cluster.Status.Pods[i].Synced = false
		}
	}
	for _, st := range h.agents.status {
		if st.Role == agentapi.RoleReplica {
			st.AofEnabled = true
		}
	}
}

// heldErr is the error a pass that holds the failover returns.
func (h *rolesHarness) heldErr() error {
	h.t.Helper()
	_, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods())
	if err == nil {
		h.t.Fatal("reconcileRoles succeeded, want it holding the failover")
	}
	h.assertNoCall("promote")
	return err
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
	first := h.heldErr()
	*now = now.Add(time.Hour)
	// Events expire after about an hour: the Ready condition's message,
	// this error, says what ends the wait too. It is the same on every
	// pass: the status is written each time, and a write starts the next.
	err := h.heldErr()
	if !strings.Contains(err.Error(), kividbv1alpha1.AllowUnsyncedFailoverAnnotation) {
		t.Errorf("an hour later: %v, want it naming %s", err, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	}
	if err.Error() != first.Error() {
		t.Errorf("an hour later %q, want the same message as before, %q", err, first)
	}
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
	events := eventsWith(rec, "")
	var forced, ended []string
	for _, e := range events {
		if strings.Contains(e, "FailoverForced") {
			forced = append(forced, e)
		}
		if strings.Contains(e, "did not come back") {
			ended = append(ended, e)
		}
	}
	if len(forced) != 1 || !strings.Contains(forced[0], "c1-1") ||
		!strings.Contains(forced[0], kividbv1alpha1.AllowUnsyncedFailoverAnnotation) || !strings.Contains(forced[0], "removed") {
		t.Errorf("events %v, want one FailoverForced naming c1-1 and the annotation, and saying it is removed", events)
	}
	// The wait ran its course, but the annotation is what ended it.
	if len(ended) != 1 || !strings.Contains(ended[0], kividbv1alpha1.AllowUnsyncedFailoverAnnotation) {
		t.Errorf("events %v, want the end of the wait to name %s", events, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
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

// A master whose engine still answers is held by the fence, and the replica
// promoted gets the catch-up wait. One that was not in sync and has not
// caught up when it ends is not promoted: that would lose what the master
// holds and it does not. The pause is lifted, the master keeps serving if
// it can, and the replica finishes its sync from it.
func TestFailover_HoldsWhenAReplicaNotInSyncDoesNotCatchUpWithTheFencedMaster(t *testing.T) {
	shortSwitchoverWaits(t)
	h, _ := newGoneMasterHarness(t)
	h.unsynced()
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": -1, "10.0.0.3": -1}
	err := h.heldErr()
	if !strings.Contains(err.Error(), kividbv1alpha1.AllowUnsyncedFailoverAnnotation) {
		t.Errorf("%v, want it naming %s", err, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	}
	paused, unpaused := -1, -1
	for i, call := range h.agents.calls {
		if strings.HasPrefix(call, "exec 10.0.0.1 CLIENT PAUSE") && paused < 0 {
			paused = i
		}
		if call == "exec 10.0.0.1 CLIENT UNPAUSE" && paused >= 0 {
			unpaused = i
		}
	}
	if paused < 0 || unpaused < 0 {
		t.Errorf("agent calls %v, want the old master paused, then unpaused", h.agents.calls)
	}
	if fm := h.storedFence(); fm != nil {
		t.Errorf("fencedMaster %+v recorded, want none: no one was promoted", fm)
	}
	events := eventsWith(rec, "")
	var blocked []string
	for _, e := range events {
		if strings.Contains(e, "FailoverBlocked") {
			blocked = append(blocked, e)
		}
		if strings.Contains(e, "FailoverUnfenced") {
			t.Errorf("event %q, want none: nothing was promoted", e)
		}
	}
	if len(blocked) != 1 || !strings.Contains(blocked[0], "in sync") || !strings.Contains(blocked[0], "c1-1") {
		t.Errorf("events %v, want one FailoverBlocked saying c1-1 was not in sync and did not catch up", events)
	}

	h.annotate(kividbv1alpha1.AllowUnsyncedFailoverAnnotation, "true")
	h.assertFailoverTo("c1-1")
}

// A replica far behind the old master (in the middle of a full resync, say)
// cannot catch up within the fence's wait: holding the master's writes for
// it would only stall them, every pass, to end in the hold anyway. The
// failover is held without a pause, until a replica is near enough.
func TestFailover_DoesNotPauseAMasterNoReplicaIsNearlyCaughtUpWith(t *testing.T) {
	shortSwitchoverWaits(t)
	h, _ := newGoneMasterHarness(t)
	h.unsynced()
	rec := h.recordEvents()
	h.agents.status["10.0.0.1"] = masterRun(5<<20, "stream-a", "run-0")
	h.agents.catchUp = map[string]int{"10.0.0.2": -1, "10.0.0.3": -1}
	err := h.heldErr()
	for _, call := range h.agents.calls {
		if strings.HasPrefix(call, "exec 10.0.0.1 CLIENT PAUSE") {
			t.Errorf("agent calls %v: the old master's writes were held, want no pause while no replica is near", h.agents.calls)
			break
		}
	}
	if !strings.Contains(err.Error(), "behind") || !strings.Contains(err.Error(), kividbv1alpha1.AllowUnsyncedFailoverAnnotation) {
		t.Errorf("%v, want it saying how far behind the nearest replica is, and naming %s", err, kividbv1alpha1.AllowUnsyncedFailoverAnnotation)
	}
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "in sync") {
		t.Errorf("events %v, want one FailoverBlocked saying no replica was in sync", e)
	}
	// The status is written on every pass, and a write starts the next
	// one: a message that changed with the offsets would have the operator
	// run passes back to back. It names no offset.
	h.agents.status["10.0.0.1"].ReplicationOffset = 6 << 20
	if again := h.heldErr(); again.Error() != err.Error() {
		t.Errorf("held again with %q, want the same message as %q", again, err)
	}

	// Once one is near (here: it has the master's offset), its writes are
	// held and it is promoted.
	h.agents.catchUp = nil
	h.assertFailoverTo("c1-1")
}

// The master's offset is read first, the replica's after it: under writes
// the replica can read ahead of it. That is near, not far.
func TestFailover_AReplicaReadAheadOfTheMasterIsNearEnough(t *testing.T) {
	shortSwitchoverWaits(t)
	h, _ := newGoneMasterHarness(t)
	h.unsynced()
	h.agents.status["10.0.0.1"] = masterRun(120, "stream-a", "run-0")
	h.agents.liveOffset = map[string]int64{"10.0.0.2": 130, "10.0.0.3": 125} // both read ahead of it
	h.agents.catchUp = map[string]int{"10.0.0.3": -1}
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}

// A replica marked in sync that holds nothing is no candidate (the
// empty-replica check, when the master was last seen holding data), so it
// does not end the wait for the master either.
func TestFailover_AnEmptyReplicaInSyncDoesNotEndTheWait(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	h.cluster.Status.Pods[0].Keys = 5      // the master was last seen holding data
	h.cluster.Status.Pods[2].Synced = true // c1-2 in sync ...
	keyed(h.agents.status["10.0.0.3"], 0)  // ... but holding nothing
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(16 * time.Second)
	h.assertWaiting("once restartWaitSeconds have passed")
	if e := eventsWith(rec, "FailoverBlocked"); len(e) != 1 || !strings.Contains(e[0], "in sync") {
		t.Errorf("events %v, want one FailoverBlocked saying no replica was in sync", e)
	}
}

// With the append-only file off, a master that is gone comes back with
// only its last save: nothing on its volume is worth more waiting, so the
// failover goes ahead after restartWaitSeconds, as before.
func TestFailover_DoesNotWaitOnWithTheAppendOnlyFileOff(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	for _, st := range h.agents.status {
		st.AofEnabled = false
	}
	h.assertWaiting("at once")
	*now = now.Add(16 * time.Second)
	h.assertFailoverTo("c1-1")
}

// The master comes back after the wait with fewer keys than a replica
// holds, and without the append-only file holding every write it
// acknowledged (an engine before 1.2.0 acknowledges a write before it is
// in the file): the replica that holds the most takes over, rather than be
// resynced from it. With that guarantee the master is kept: what a replica
// holds is a part of its history.
func TestFailover_ComparesKeysWhenTheMasterComesBackFromTheWait(t *testing.T) {
	for _, tc := range []struct {
		name, version, want string
	}{
		{"engine 1.1: no such guarantee", "1.1.0", "c1-1"},
		{"engine 1.2: acknowledged once in the file", "1.2.0", "c1-0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, now := newGoneMasterHarness(t)
			h.unsynced()
			h.assertWaiting("at once")
			*now = now.Add(time.Minute)
			st := backOnItsVolume(2) // fewer keys than the replicas' 5
			st.EngineVersion = tc.version
			h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: st})
			if tc.want != "c1-0" {
				h.assertFailoverTo(tc.want)
				return
			}
			if m, failover := h.reconcile(); m != "c1-0" || failover {
				t.Fatalf("master=%q failover=%v, want c1-0 kept", m, failover)
			}
		})
	}
}

// The annotation was set, but the master came back before
// restartWaitSeconds had passed: it is kept, and the annotation removed,
// so that it does not end some later wait unasked.
func TestFailover_RemovesTheAnnotationWhenTheMasterComesBackFirst(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.unsynced()
	h.assertWaiting("at once")
	h.annotate(kividbv1alpha1.AllowUnsyncedFailoverAnnotation, "true")
	*now = now.Add(3 * time.Second)
	h.assertWaiting("within restartWaitSeconds")
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(5)})
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 kept", m, failover)
	}
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	if v, ok := stored.Annotations[kividbv1alpha1.AllowUnsyncedFailoverAnnotation]; ok {
		t.Errorf("%s is still %q after the master came back", kividbv1alpha1.AllowUnsyncedFailoverAnnotation, v)
	}
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
