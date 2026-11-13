package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// A master whose pod is gone, and whose engine no longer answers, may be
// coming back on its volume, with writes no replica has (#78).

// newGoneMasterHarness: the master c1-0 is gone -- its pod object, and its
// engine with it -- after acknowledging writes on stream-a; its replicas
// hold 5 keys each. The pass is timed by *now.
func newGoneMasterHarness(t *testing.T) (*rolesHarness, *time.Time) {
	t.Helper()
	h := newFenceHarness(t)
	delete(h.agents.status, "10.0.0.1")
	h.cluster.Status.Pods[0].ReplID = "stream-a"
	h.cluster.Status.Pods[0].Incarnation = "old/0"
	keyed(h.agents.status["10.0.0.2"], 5)
	keyed(h.agents.status["10.0.0.3"], 5)
	now := time.Now()
	h.r.clock = func() time.Time { return now }
	return h, &now
}

// backOnItsVolume is c1-0's engine started again from its append-only file,
// in a new stream, holding keys keys.
func backOnItsVolume(keys int64) *agentapi.StatusResponse {
	st := keyed(masterRun(0, "stream-b", "run-new"), keys)
	st.AofEnabled, st.AofAckAfterWrite, st.EngineVersion = true, "yes", "1.2.0"
	return st
}

// noRestartWait turns the wait for a master to come back off, for a test of
// what follows it.
func (h *rolesHarness) noRestartWait() {
	off := int32(0)
	h.cluster.Spec.Failover.RestartWaitSeconds = &off
}

func (h *rolesHarness) assertWaiting(when string) {
	h.t.Helper()
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
		h.t.Fatalf("%s: reconcileRoles succeeded, want it waiting for the master", when)
	}
	h.assertNoCall("promote")
}

// It is waited for, for restartWaitSeconds (15 s by default), and then
// failed away from, each said in an event.
func TestFailover_WaitsForAMasterWhosePodIsGone(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(10 * time.Second)
	h.assertWaiting("10 s later")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "15s") {
		t.Errorf("events %v, want one FailoverWait saying it waits up to 15s", e)
	}
	*now = now.Add(6 * time.Second)
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "did not come back") {
		t.Errorf("events %v, want one FailoverWait saying it did not come back", e)
	}
}

// The Ready condition says since when it is waited for, in the same words
// on every pass: the status is written on each, and a write starts the
// next pass.
func TestFailover_TheWaitForTheMasterSaysTheSameOnEveryPass(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	start := *now
	first := h.heldErr()
	*now = now.Add(5 * time.Second)
	if err := h.heldErr(); err.Error() != first.Error() || !strings.Contains(err.Error(), start.UTC().Format(time.RFC3339)) {
		t.Errorf("5 s later %q, want the same words as before (%q), with the time it began", err, first)
	}
}

// It comes back holding at least the keys its replicas hold, with the AOF
// acknowledging only what is in the file: it stays the master, as a master
// that restarted in place does (#42), and the replicas follow it.
func TestFailover_KeepsAMasterThatCameBackOnItsVolume(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(3 * time.Second)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(5)})
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 kept", m, failover)
	}
	h.assertNoCall("promote")
	h.assertRole("10.0.0.2", agentapi.RoleReplica, "10.0.0.9")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 2 || !strings.Contains(e[1], "came back") {
		t.Errorf("events %v, want FailoverWait saying it came back", e)
	}
}

// It comes back with less than its replicas hold: the replica that holds
// the most takes over, as from a master that restarted behind them.
func TestFailover_FailsOverFromAMasterThatCameBackBehind(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.assertWaiting("at once")
	*now = now.Add(3 * time.Second)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(2)})
	h.assertFailoverTo("c1-1")
}

// failover.restartWaitSeconds: 0 turns the wait off.
func TestFailover_DoesNotWaitWithRestartWaitOff(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	off := int32(0)
	h.cluster.Spec.Failover.RestartWaitSeconds = &off
	h.assertFailoverTo("c1-1")
}

// A master whose engine still answers is held by the fence, not waited for.
func TestFailover_DoesNotWaitForAMasterThatStillAnswers(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.agents.status["10.0.0.1"] = masterRun(100, "stream-a", "run-0")
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}

// Its replacement pod cannot be scheduled -- its volume is pinned to a node
// that is not Ready, say: it is not coming back soon, and waiting would only
// add downtime.
func TestFailover_DoesNotWaitForAPodThatCannotBeScheduled(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.addPod(testPod{name: "c1-0", uid: "new", unschedulable: true})
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "cannot be scheduled") {
		t.Errorf("events %v, want one FailoverWait saying why it did not wait", e)
	}
}

// The replacement pod has started -- it has a Ready condition, False -- but
// is not ready: it is still waited for, but only until restartWaitSeconds
// after the master was found gone, not unhealthyThresholdSeconds after it
// started, so that a force-deleted master costs that much at most.
func TestFailover_CapsTheWaitForAReplacementThatIsNotReady(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(5 * time.Second)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", uid: "new", unreadyFor: time.Second, agent: backOnItsVolume(5)})
	h.assertWaiting("its pod started, not ready")
	*now = now.Add(11 * time.Second)
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 2 || !strings.Contains(e[1], "did not come back") {
		t.Errorf("events %v, want FailoverWait saying it did not come back in time", e)
	}
}

// The cap counts from when the replacement pod was made, not from when this
// operator first saw it: one restarted meanwhile does not wait it out again.
func TestFailover_CapsTheWaitFromWhenTheReplacementWasMade(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", uid: "new", unreadyFor: time.Second, createdAgo: 20 * time.Second, agent: backOnItsVolume(5)})
	h.assertFailoverTo("c1-1")
}

// A master that restarts in place -- the same pod -- keeps the unhealthy
// threshold: it is not a force-deleted master, and is not waited out
// against restartWaitSeconds.
func TestFailover_KeepsTheThresholdForAMasterThatRestartsInPlace(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.cluster.Status.Pods[0].Incarnation = "same/0"
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.1", label: "master", uid: "same", unreadyFor: 5 * time.Second})
	*now = now.Add(time.Minute)
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want no failover before unhealthyThresholdSeconds", m, failover)
	}
	if e := eventsWith(rec, "FailoverWait"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}

// A replacement pod with no Ready condition yet is still on its way: the
// failover waits for it, as for a pod that is gone.
func TestFailover_WaitsForAReplacementPodNotStartedYet(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.addPod(testPod{name: "c1-0", uid: "new", pending: true})
	h.assertWaiting("at once")
}

// The wait for a master to come back is logged on every pass while it
// lasts. It is a decision, not a failure: logged at info level, with its
// reason, not as an error with a stack trace. Anything else that keeps the
// roles from being settled is still an error.
func TestAWaitForTheMasterIsNotLoggedAsAnError(t *testing.T) {
	var buf bytes.Buffer
	ctx := logf.IntoContext(context.Background(), zap.New(zap.WriteTo(&buf)))
	logRoles(ctx, heldBack("WaitingForMaster", errors.New("waiting up to 15s for master c1-0 to come back on its volume (4s so far)")))
	logRoles(ctx, errors.New("reading the pods' roles: connection refused"))

	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(l), &line); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		lines = append(lines, line)
	}
	if len(lines) != 2 {
		t.Fatalf("%d log lines, want 2: %s", len(lines), buf.String())
	}
	if wait := lines[0]; wait["level"] != "info" || wait["reason"] != "WaitingForMaster" || wait["stacktrace"] != nil {
		t.Errorf("the wait was logged as %v, want at info level with reason WaitingForMaster and no stack trace", wait)
	}
	if failed := lines[1]; failed["level"] != "error" {
		t.Errorf("the failure was logged as %v, want at error level", failed)
	}
}

// promotes counts the promotions tried on ip.
func (h *rolesHarness) promotes(ip string) int {
	n := 0
	for _, call := range h.agents.calls {
		if call == "promote "+ip {
			n++
		}
	}
	return n
}

// The wait ran out, and the failover after it did not go through (the
// promotion failed). The next pass fails over again at once: the wait is
// over for this master -- this pod of it -- until the master changes. It
// does not wait again, or say again that it waits and did not come back.
func TestFailover_DoesNotWaitAgainOnceTheWaitRanOut(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replacement bool
	}{{"its pod gone", false}, {"its replacement pending", true}} {
		t.Run(tc.name, func(t *testing.T) {
			h, now := newGoneMasterHarness(t)
			rec := h.recordEvents()
			if tc.replacement {
				h.addPod(testPod{name: "c1-0", uid: "new", pending: true, createdAgo: time.Second})
			}
			h.assertWaiting("at once")
			*now = now.Add(16 * time.Second)
			h.agents.failPromote = map[string]bool{"10.0.0.2": true}
			if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
				t.Fatal("reconcileRoles succeeded, want the promotion's error")
			}
			if e := eventsWith(rec, "FailoverWait"); len(e) != 2 || !strings.Contains(e[1], "did not come back") {
				t.Fatalf("events %v, want the wait and that it ran out", e)
			}
			*now = now.Add(5 * time.Second)
			tried := h.promotes("10.0.0.2")
			if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods()); err == nil {
				t.Fatal("reconcileRoles succeeded, want the promotion's error")
			}
			if h.promotes("10.0.0.2") == tried {
				t.Error("no promotion tried on the next pass: it waited again")
			}
			if e := eventsWith(rec, "FailoverWait"); len(e) > 0 {
				t.Errorf("events %v on the next pass, want none: the wait ran out already", e)
			}
			h.agents.failPromote = nil
			h.assertFailoverTo("c1-1")
		})
	}
}

// A wait that ends other than by the master coming back says how it ended.
func TestFailover_SaysWhyTheWaitEnded(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		end        func(h *rolesHarness)
	}{
		{"failover turned off", "failover is disabled", func(h *rolesHarness) {
			off := false
			h.cluster.Spec.Failover.Enabled = &off
		}},
		{"another master", "the master is now c1-1", func(h *rolesHarness) {
			h.cluster.Status.MasterPod = "c1-1"
		}},
		{"the wait turned off", "restartWaitSeconds is 0", func(h *rolesHarness) {
			h.noRestartWait()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newGoneMasterHarness(t)
			rec := h.recordEvents()
			h.assertWaiting("at once")
			eventsWith(rec, "")
			tc.end(h)
			_, _, _, _ = h.r.reconcileRoles(context.Background(), h.cluster, h.cluster.Status.DeepCopy(), h.pods())
			e := eventsWith(rec, "FailoverWait")
			if len(e) != 1 || !strings.Contains(e[0], tc.want) || strings.Contains(e[0], "came back") {
				t.Errorf("events %v, want one FailoverWait saying %q", e, tc.want)
			}
		})
	}
}

// Nothing recorded for the master -- no address or engine run: the CRD was
// not applied again, or its agent is from before 0.7.0 -- tells nothing
// about whether its engine is still there: it is not waited for.
func TestFailover_DoesNotWaitWithNothingRecorded(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.cluster.Status.Pods[0].PodIP, h.cluster.Status.Pods[0].RunID = "", ""
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) > 0 {
		t.Errorf("unexpected %v", e)
	}
}
