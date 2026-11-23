package controller

import (
	"reflect"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// topologySpreadConstraints and priorityClassName reach every pod verbatim,
// on a replicated cluster and on each shard, beside the default
// anti-affinity; unset, the pod has neither.
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

	// A copy of the cluster carries its own copy of the constraints.
	cp := c.DeepCopy()
	cp.Spec.TopologySpreadConstraints[0].MaxSkew = 2
	if c.Spec.TopologySpreadConstraints[0].MaxSkew != 1 {
		t.Fatal("DeepCopy shares topologySpreadConstraints")
	}
}
