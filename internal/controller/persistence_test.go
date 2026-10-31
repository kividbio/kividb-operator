package controller

import (
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func kividbConfig(directives map[string]string) *kividbv1alpha1.KividbConfig {
	return &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg"}, Spec: kividbv1alpha1.KividbConfigSpec{Directives: directives}}
}

// The engine never snapshots on a schedule: without the append-only file a
// killed shard master came back empty, kept its slots, and its replica
// resynced from it (the AKS run's B2). Both modes get it unless a
// KividbConfig says otherwise.
func TestAppendOnlyFileIsOnByDefault(t *testing.T) {
	for _, c := range []*kividbv1alpha1.KividbCluster{testCluster("rep"), shardedCluster("shd", 3, 1)} {
		for _, cfg := range []*kividbv1alpha1.KividbConfig{nil, kividbConfig(map[string]string{"maxmemory": "1gb"})} {
			got := renderKividbConfWith(c, cfg, clusterSecrets{busSecret: "b"})
			if !strings.Contains(got, "\nappendonly yes\n") || !strings.Contains(got, "\nappendfsync everysec\n") {
				t.Fatalf("%s: no append-only file by default:\n%s", c.Name, got)
			}
			if cond := persistenceCondition(c, cfg); cond.Status != metav1.ConditionTrue || cond.Reason != "AppendOnlyFile" {
				t.Fatalf("%s: condition %+v", c.Name, cond)
			}
		}
	}
}

func TestAppendOnlyFileSettingsOfTheUserWin(t *testing.T) {
	c := shardedCluster("shd", 3, 1)
	cases := []struct {
		directives    map[string]string
		want, notWant []string
		disabled      bool
	}{
		// The opt-out: rendered as given, nothing added.
		{map[string]string{"appendonly": "no"}, []string{"\nappendonly no\n"}, []string{"appendonly yes", "appendfsync"}, true},
		{map[string]string{"Appendonly": "No"}, []string{"\nAppendonly No\n"}, []string{"appendonly yes", "appendfsync"}, true},
		// The engine's own name for it.
		{map[string]string{"aof": "no"}, []string{"\naof no\n"}, []string{"appendonly", "appendfsync"}, true},
		{map[string]string{"aof": "yes"}, []string{"\naof yes\n"}, []string{"appendonly", "appendfsync"}, false},
		// On, set by the user: nothing added next to it.
		{map[string]string{"appendonly": "yes"}, []string{"\nappendonly yes\n"}, []string{"appendfsync"}, false},
		// Their own fsync policy is kept.
		{map[string]string{"appendfsync": "everysec"}, []string{"\nappendonly yes\n", "\nappendfsync everysec\n"}, nil, false},
	}
	for _, tc := range cases {
		cfg := kividbConfig(tc.directives)
		got := renderKividbConfWith(c, cfg, clusterSecrets{})
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%v: want %q in:\n%s", tc.directives, w, got)
			}
		}
		for _, w := range tc.notWant {
			if strings.Contains(got, w) {
				t.Errorf("%v: %q should not be there:\n%s", tc.directives, w, got)
			}
		}
		if appendOnlyDisabled(cfg) != tc.disabled {
			t.Errorf("%v: disabled = %v", tc.directives, !tc.disabled)
		}
		if strings.Count(got, "appendfsync") > 1 || strings.Count(strings.ToLower(got), "\nappendonly ") > 1 {
			t.Errorf("%v: a directive twice:\n%s", tc.directives, got)
		}
	}
}

// Turning it off is allowed, but said: a Warning Event when the condition
// turns False (once, not on every reconcile), and the condition itself.
func TestPersistenceDisabledIsWarnedAbout(t *testing.T) {
	for _, c := range []*kividbv1alpha1.KividbCluster{testCluster("rep"), shardedCluster("shd", 3, 1)} {
		rec := record.NewFakeRecorder(10)
		r := &KividbClusterReconciler{Recorder: rec}
		off := kividbConfig(map[string]string{"appendonly": "no"})

		r.notePersistence(c, nil)
		if cond := findCondition(c.Status.Conditions, PersistenceCondition); cond == nil || cond.Status != metav1.ConditionTrue {
			t.Fatalf("%s: %+v", c.Name, c.Status.Conditions)
		}
		for i := 0; i < 3; i++ {
			r.notePersistence(c, off)
		}
		cond := findCondition(c.Status.Conditions, PersistenceCondition)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "AppendOnlyDisabled" || !strings.Contains(cond.Message, "KividbConfig cfg") {
			t.Fatalf("%s: %+v", c.Name, cond)
		}
		if isSharded(c) != strings.Contains(cond.Message, "shard") {
			t.Errorf("%s: message does not fit the mode: %s", c.Name, cond.Message)
		}
		ev := events(r)
		if n := strings.Count(ev, "Warning PersistenceDisabled"); n != 1 {
			t.Fatalf("%s: %d PersistenceDisabled events, want 1:\n%s", c.Name, n, ev)
		}

		// Back on: True again, and turning it off again warns again.
		r.notePersistence(c, nil)
		r.notePersistence(c, off)
		if n := strings.Count(events(r), "Warning PersistenceDisabled"); n != 1 {
			t.Fatalf("%s: turning it off a second time: %d events", c.Name, n)
		}
	}
}
