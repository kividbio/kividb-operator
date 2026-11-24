package controller

import (
	"context"
	"reflect"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// topologySpreadConstraints and priorityClassName reach the database pods
// verbatim, on a replicated cluster and on each shard, beside the default
// anti-affinity, and priorityClassName the backup and restore pods; unset,
// no pod has either.
func TestTopologySpreadAndPriorityClassPassThrough(t *testing.T) {
	t.Parallel()
	spread := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{kividbv1alpha1.ClusterLabel: "c1"},
		},
	}}

	c := testCluster("c1")
	plain := podTemplate(c, nil, nil, nil)
	if plain.Spec.TopologySpreadConstraints != nil || plain.Spec.PriorityClassName != "" {
		t.Fatalf("unset: spread %v, priority class %q; want neither",
			plain.Spec.TopologySpreadConstraints, plain.Spec.PriorityClassName)
	}

	c.Spec.TopologySpreadConstraints = spread
	c.Spec.PriorityClassName = "kividb-high"
	for name, tmpl := range map[string]corev1.PodTemplateSpec{
		"replicated": podTemplate(c, nil, nil, nil),
		"shard 1":    shardPodTemplate(c, 1, nil, nil, nil),
	} {
		if !reflect.DeepEqual(tmpl.Spec.TopologySpreadConstraints, spread) {
			t.Errorf("%s: spread %v, want %v", name, tmpl.Spec.TopologySpreadConstraints, spread)
		}
		if tmpl.Spec.PriorityClassName != "kividb-high" {
			t.Errorf("%s: priority class %q, want kividb-high", name, tmpl.Spec.PriorityClassName)
		}
		if tmpl.Spec.Affinity == nil || tmpl.Spec.Affinity.PodAntiAffinity == nil {
			t.Errorf("%s: the default anti-affinity was dropped", name)
		}
	}

	// The backup CronJob's pods and the restore Job's get the priority
	// class too (not the spread: one pod each, placed beside the data).
	snapCfg := &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "default"}}
	snapCfg.Spec.Schedule = "0 * * * *"
	backup := desiredBackupCronJob(c, snapCfg).Spec.JobTemplate.Spec.Template.Spec
	if backup.PriorityClassName != "kividb-high" {
		t.Errorf("backup pods: priority class %q, want kividb-high", backup.PriorityClassName)
	}
	if unset := desiredBackupCronJob(testCluster("c2"), snapCfg).Spec.JobTemplate.Spec.Template.Spec; unset.PriorityClassName != "" {
		t.Errorf("backup pods, unset: priority class %q, want none", unset.PriorityClassName)
	}
	snap := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
		Status:     kividbv1alpha1.KividbSnapshotStatus{Phase: kividbv1alpha1.SnapshotSucceeded, ObjectKey: "k"},
	}
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	job, err := shardedReconciler(t, c).ensureBootstrapJob(context.Background(), c, nil, snap, snapCfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := job.Spec.Template.Spec.PriorityClassName; got != "kividb-high" {
		t.Errorf("restore Job: priority class %q, want kividb-high", got)
	}

	// A copy of the cluster carries its own copy of the constraints.
	cp := c.DeepCopy()
	cp.Spec.TopologySpreadConstraints[0].MaxSkew = 2
	if c.Spec.TopologySpreadConstraints[0].MaxSkew != 1 {
		t.Fatal("DeepCopy shares topologySpreadConstraints")
	}
}
