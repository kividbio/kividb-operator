package controller

import (
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
)

func container(t *testing.T, tmpl corev1.PodTemplateSpec, name string) corev1.Container {
	t.Helper()
	for _, c := range tmpl.Spec.Containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s container", name)
	return corev1.Container{}
}

// B2a: a deadlocked engine accepts TCP connections and answers nothing.
// Liveness asks it to answer, and is patient enough for a slow engine and
// for the failover of a hung master to come first.
func TestKividbLivenessAsksTheEngineToAnswer(t *testing.T) {
	c := testCluster("c1")
	k := container(t, podTemplate(c, nil, nil, nil), "kividb")
	live := k.LivenessProbe
	if live == nil || live.HTTPGet == nil || live.HTTPGet.Path != "/livez" || live.HTTPGet.Port.IntValue() != AgentPort || live.TCPSocket != nil {
		t.Fatalf("liveness %+v, want GET /livez on the agent", live)
	}
	if window := live.PeriodSeconds * live.FailureThreshold; window < 60 || window > 120 {
		t.Fatalf("liveness gives up after %d s, want 60-120", window)
	}
	// It must outlast readiness (15 s) plus the default failover threshold.
	if window := live.PeriodSeconds * live.FailureThreshold; window <= int32(defaultUnhealthyThreshold.Seconds())+15 {
		t.Fatalf("liveness gives up after %d s, before a hung master is failed over", window)
	}
	if s := k.StartupProbe; s == nil || s.HTTPGet == nil || s.HTTPGet.Path != "/livez" || s.PeriodSeconds*s.FailureThreshold < 600 {
		t.Fatalf("startup probe %+v, want /livez with room for a long load", s)
	}
}

// A2a: a master deleted, drained or evicted waits for the operator to hand
// its role over; the agent waits with it, to answer meanwhile.
func TestBothContainersWaitForTheHandoverBeforeStopping(t *testing.T) {
	c := testCluster("c1")
	tmpl := podTemplate(c, nil, nil, nil)
	for _, name := range []string{"kividb", "agent"} {
		l := container(t, tmpl, name).Lifecycle
		if l == nil || l.PreStop == nil || l.PreStop.HTTPGet == nil || l.PreStop.HTTPGet.Path != "/prestop" {
			t.Fatalf("%s: preStop %+v, want GET /prestop", name, l)
		}
	}
	var hold string
	for _, e := range container(t, tmpl, "agent").Env {
		if e.Name == agentapi.PreStopTimeoutEnv {
			hold = e.Value
		}
	}
	if hold != "20" {
		t.Fatalf("%s = %q, want 20 (a third of the 60 s grace period)", agentapi.PreStopTimeoutEnv, hold)
	}
	for grace, want := range map[int64]int64{9: 5, 60: 20, 300: 30} {
		c.Spec.TerminationGracePeriodSeconds = &grace
		if got := preStopHold(c); got != want {
			t.Errorf("grace %d s: hold %d s, want %d", grace, got, want)
		}
	}
}

func TestReplicatedClusterHasADisruptionBudget(t *testing.T) {
	c := testCluster("c1")
	pdb := desiredPDB(c)
	if pdb.Name != "c1" || pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("pdb %+v", pdb.Spec)
	}
	for k, v := range selectorLabels(c) {
		if pdb.Spec.Selector.MatchLabels[k] != v {
			t.Fatalf("pdb selector %v, want the cluster's pods", pdb.Spec.Selector.MatchLabels)
		}
	}
	if _, ok := pdb.Spec.Selector.MatchLabels[kividbv1alpha1.RoleLabel]; ok {
		t.Fatal("the budget must cover every pod, whatever its role")
	}
}
