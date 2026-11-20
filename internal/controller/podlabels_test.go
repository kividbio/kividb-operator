package controller

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/tools/record"
)

// Issue #122: a spec.podLabels key must not take over a label the
// StatefulSet selects its pods by, or the API server refuses the
// StatefulSet, nor RoleLabel, the controller's to set on each live pod.
// Any other key, app.kubernetes.io/managed-by too, is the user's.
func TestPodLabelsDoNotOverrideTheOperatorsLabels(t *testing.T) {
	for _, tc := range []struct {
		key, value string
		ignored    bool // listed by ignoredPodLabels
	}{
		{"team", "a", false},
		{"app.kubernetes.io/part-of", "shop", false},
		{kividbv1alpha1.ClusterLabel, "other", true},
		{kividbv1alpha1.ClusterLabel, "c1", false}, // the operator's own value
		{"app.kubernetes.io/instance", "other", true},
		{"app.kubernetes.io/name", "other", true},
		{"app.kubernetes.io/managed-by", "other", false}, // not in the selector: the user's
		{kividbv1alpha1.RoleLabel, string(kividbv1alpha1.RoleMaster), true},
		{kividbv1alpha1.RoleLabel, string(kividbv1alpha1.RoleReplica), true},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			c := testCluster("c1")
			c.Spec.Storage.Size = "1Gi"
			c.Spec.PodLabels = map[string]string{tc.key: tc.value}
			shard := desiredShardStatefulSet(c, 1, nil, nil, nil)
			for name, sts := range map[string]*appsv1.StatefulSet{
				"replicated": desiredStatefulSet(c, nil, nil, nil),
				"shard 1":    shard,
			} {
				got := sts.Spec.Template.Labels
				for k, v := range sts.Spec.Selector.MatchLabels {
					if got[k] != v {
						t.Errorf("%s: template label %s=%q, the selector wants %q", name, k, got[k], v)
					}
				}
				for k, v := range selectorLabels(c) {
					if got[k] != v {
						t.Errorf("%s: template label %s=%q, want the operator's %q", name, k, got[k], v)
					}
				}
				if v, ok := got[kividbv1alpha1.RoleLabel]; ok {
					t.Errorf("%s: the template has %s=%q; the controller sets it on each pod", name, kividbv1alpha1.RoleLabel, v)
				}
				if !tc.ignored && got[tc.key] != tc.value {
					t.Errorf("%s: %s=%q, want %q", name, tc.key, got[tc.key], tc.value)
				}
			}
			if got := shard.Spec.Template.Labels[ShardLabel]; got != "1" {
				t.Errorf("shard 1: %s=%q, want 1", ShardLabel, got)
			}
			if got := ignoredPodLabels(c); (len(got) == 1 && got[0] == tc.key) != tc.ignored {
				t.Errorf("ignoredPodLabels = %v, want ignored %v", got, tc.ignored)
			}
		})
	}
}

// On a sharded cluster each shard's pods get their own ShardLabel, whatever
// spec.podLabels says; on a replicated one, which has no shards, the key
// is the user's like any other.
func TestPodLabelsShardLabel(t *testing.T) {
	c := testCluster("c1")
	c.Spec.Storage.Size = "1Gi"
	c.Spec.PodLabels = map[string]string{ShardLabel: "7"}
	if got := desiredStatefulSet(c, nil, nil, nil).Spec.Template.Labels[ShardLabel]; got != "7" {
		t.Errorf("replicated: %s=%q, want the user's 7", ShardLabel, got)
	}
	if got := ignoredPodLabels(c); got != nil {
		t.Errorf("replicated: ignored %v, want none", got)
	}
	shards := int32(3)
	c.Spec.Shards = &shards
	for i := int32(0); i < shards; i++ {
		sts := desiredShardStatefulSet(c, i, nil, nil, nil)
		if got, want := sts.Spec.Template.Labels[ShardLabel], sts.Spec.Selector.MatchLabels[ShardLabel]; got != want {
			t.Errorf("shard %d: %s=%q, the selector wants %q", i, ShardLabel, got, want)
		}
	}
	if got := ignoredPodLabels(c); !reflect.DeepEqual(got, []string{ShardLabel}) {
		t.Errorf("sharded: ignored %v, want [%s]", got, ShardLabel)
	}
}

// A cluster with ordinary podLabels, managed-by set to another value
// among them, gets a byte-identical pod template after #122: nothing
// rolls on upgrade. The labels are all that #122
// changes in the template, so "before" is today's template with its
// labels made the way they were: commonLabels, spec.podLabels over them,
// and on a shard's template ShardLabel over those.
func TestPodLabelsLeaveAnOrdinaryTemplateAsItWas(t *testing.T) {
	c := testCluster("c1")
	c.Spec.Storage.Size = "1Gi"
	c.Spec.PodLabels = map[string]string{
		"team": "a", "app.kubernetes.io/part-of": "shop", kividbv1alpha1.ClusterLabel: "c1",
		"app.kubernetes.io/managed-by": "other", // an override that worked before #122, and still does
	}
	oldLabels := func(extra map[string]string) map[string]string {
		l := commonLabels(c)
		for k, v := range c.Spec.PodLabels {
			l[k] = v
		}
		for k, v := range extra {
			l[k] = v
		}
		return l
	}
	shards := int32(2)
	sharded := c.DeepCopy()
	sharded.Spec.Shards = &shards
	for name, tc := range map[string]struct {
		sts   *appsv1.StatefulSet
		extra map[string]string
	}{
		"replicated": {desiredStatefulSet(c, nil, nil, nil), nil},
		"shard 1":    {desiredShardStatefulSet(sharded, 1, nil, nil, nil), map[string]string{ShardLabel: "1"}},
	} {
		after := tc.sts.Spec.Template
		before := *after.DeepCopy()
		before.Labels = oldLabels(tc.extra)
		a, err := json.Marshal(after)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(before)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Errorf("%s: template changed:\nbefore %s\nafter  %s", name, b, a)
		}
	}
}

// The ignored keys are told once per change, not on every pass; nothing
// is said when none is ignored, and the same keys set again later are
// told again.
func TestIgnoredPodLabelsAreToldOncePerChange(t *testing.T) {
	c := testCluster("c1")
	events := record.NewFakeRecorder(10)
	r := &KividbClusterReconciler{Recorder: events}
	next := func() string {
		select {
		case ev := <-events.Events:
			return ev
		default:
			return ""
		}
	}

	c.Spec.PodLabels = map[string]string{"team": "a"}
	r.emitIgnoredPodLabels(c)
	if ev := next(); ev != "" {
		t.Fatalf("nothing ignored, yet an Event: %q", ev)
	}

	c.Spec.PodLabels = map[string]string{"team": "a", "app.kubernetes.io/instance": "other", kividbv1alpha1.RoleLabel: "master"}
	for range 3 {
		r.emitIgnoredPodLabels(c)
	}
	if ev := next(); !strings.HasPrefix(ev, "Warning PodLabelsIgnored ") || !strings.Contains(ev, "app.kubernetes.io/instance, kividb.io/role") {
		t.Fatalf("event %q", ev)
	}
	if ev := next(); ev != "" {
		t.Fatalf("the same keys told twice: %q", ev)
	}

	c.Spec.PodLabels[kividbv1alpha1.ClusterLabel] = "other" // a change: told again
	r.emitIgnoredPodLabels(c)
	if ev := next(); !strings.Contains(ev, "app.kubernetes.io/instance, kividb.io/cluster, kividb.io/role") {
		t.Fatalf("after a change: event %q", ev)
	}

	c.Spec.PodLabels = map[string]string{"team": "a"} // none ignored: silent
	r.emitIgnoredPodLabels(c)
	c.Spec.PodLabels = map[string]string{kividbv1alpha1.RoleLabel: "master"} // set again: told again
	r.emitIgnoredPodLabels(c)
	if ev := next(); !strings.Contains(ev, "spec.podLabels sets kividb.io/role,") {
		t.Fatalf("set again after none: event %q", ev)
	}
	if ev := next(); ev != "" {
		t.Fatalf("an extra Event: %q", ev)
	}
}
