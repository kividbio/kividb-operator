package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// In the AKS run a replicated cluster bootstrapped from a sharded cluster's
// backup ran its restore Job on the run's manifest ("gzip: invalid
// header"), failed, and said "restore Job running". It is refused before
// any Job, with the reason.
func TestBootstrapFromAShardedBackupIsRefused(t *testing.T) {
	ctx := context.Background()
	snap := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "shd-backup-now", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbSnapshotSpec{SnapshotConfigRef: corev1.LocalObjectReference{Name: "s3"}},
		Status: kividbv1alpha1.KividbSnapshotStatus{
			Phase:     kividbv1alpha1.SnapshotSucceeded,
			ObjectKey: "e2e/shd/20261031T065615Z/manifest.json",
			Shards:    []kividbv1alpha1.SnapshotShardStatus{{Index: 0}, {Index: 1}, {Index: 2}},
		},
	}
	cfg := &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "default"}}
	c := testCluster("shdr")
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	c.Spec.BootstrapFromSnapshot = &kividbv1alpha1.BootstrapFromSnapshotSpec{SnapshotRef: corev1.LocalObjectReference{Name: snap.Name}}
	r := shardedReconciler(t, c, snap, cfg)

	for i := 0; i < 2; i++ {
		block, err := r.reconcileBootstrap(ctx, c)
		if !block || err == nil {
			t.Fatalf("pass %d: block %v, err %v", i, block, err)
		}
	}
	b := c.Status.Bootstrap
	if b.Phase != kividbv1alpha1.BootstrapFailed || !strings.Contains(b.Error, "sharded cluster's backup") || b.Message != "bootstrap refused" {
		t.Fatalf("bootstrap status %+v", b)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("restore Jobs %v (%v)", jobs.Items, err)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapRefused") != 1 {
		t.Fatalf("events:\n%s", ev)
	}

	// A replicated cluster's backup is still taken.
	if err := bootstrapSnapshotUsable(&kividbv1alpha1.KividbSnapshot{Status: kividbv1alpha1.KividbSnapshotStatus{ObjectKey: "e2e/rep/rep-0-20261031T062239Z.tar.gz"}}); err != nil {
		t.Fatal(err)
	}
}
