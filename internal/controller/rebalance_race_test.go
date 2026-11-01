package controller

import (
	"context"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// In the AKS run's 4 -> 3, a pass that still read the rebalance as under
// way (its status was a moment behind) made the removed shard's
// StatefulSet again, just deleted: two new pods on two new disks, gone
// again 30 s later. A removed shard's StatefulSet is kept while it
// exists, never made.
func TestRemovedShardStatefulSetIsNotMadeAgain(t *testing.T) {
	ctx := context.Background()
	c := shardedCluster("s", 3, 1)
	c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{Shards: 4, Rebalance: &kividbv1alpha1.RebalanceStatus{From: 4, To: 3}}
	r := shardedReconciler(t, c)
	if _, err := r.reconcileSharded(ctx, c, nil, nil, nil, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-3"}, &sts); err == nil {
		t.Fatal("the removed shard's StatefulSet was made again")
	}
	var pdb policyv1.PodDisruptionBudget
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-3"}, &pdb); err == nil {
		t.Fatal("the removed shard's disruption budget was made again")
	}
	for s := 0; s < 3; s++ {
		if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: shardStatefulSetName(c, int32(s))}, &sts); err != nil {
			t.Fatalf("shard %d: %v", s, err)
		}
	}

	// While it still exists (draining), it is kept up to date.
	c2 := shardedCluster("t", 3, 1)
	c2.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{Shards: 4, Rebalance: &kividbv1alpha1.RebalanceStatus{From: 4, To: 3}}
	old := desiredShardStatefulSet(c2, 3, nil, nil, nil)
	old.Spec.Template.Annotations = map[string]string{"stale": "yes"}
	r2 := shardedReconciler(t, c2, old)
	if _, err := r2.reconcileSharded(ctx, c2, nil, nil, nil, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := r2.Get(ctx, client.ObjectKey{Namespace: "default", Name: "t-3"}, &sts); err != nil || sts.Spec.Template.Annotations["stale"] != "" {
		t.Fatalf("draining shard's StatefulSet: %v %v", err, sts.Spec.Template.Annotations)
	}
}
