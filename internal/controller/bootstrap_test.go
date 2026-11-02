package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// bootstrapFixture is a cluster asking to be bootstrapped from a
// replicated cluster's Succeeded snapshot, and the objects it needs.
func bootstrapFixture(name string) (*kividbv1alpha1.KividbCluster, []client.Object) {
	snap := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "old-backup", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbSnapshotSpec{SnapshotConfigRef: corev1.LocalObjectReference{Name: "s3"}},
		Status:     kividbv1alpha1.KividbSnapshotStatus{Phase: kividbv1alpha1.SnapshotSucceeded, ObjectKey: "rep/rep-0-20260101T000000Z.tar.gz"},
	}
	cfg := &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "default"}}
	c := testCluster(name)
	c.Spec.Replicas = 2
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	c.Spec.BootstrapFromSnapshot = &kividbv1alpha1.BootstrapFromSnapshotSpec{SnapshotRef: corev1.LocalObjectReference{Name: snap.Name}}
	return c, []client.Object{snap, cfg}
}

// bootstrapFromSnapshot added to a cluster that is already running scaled
// it to 0 and restored the snapshot over pod-0's volume. It is skipped,
// with a Warning, and the StatefulSet keeps its pods.
func TestBootstrapIsSkippedOnARunningCluster(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("live")
	three := int32(3)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "default"}, Spec: appsv1.StatefulSetSpec{Replicas: &three}}
	r := shardedReconciler(t, append(objs, c, sts)...)

	for i := 0; i < 2; i++ {
		block, err := r.reconcileBootstrap(ctx, c)
		if block || err != nil {
			t.Fatalf("pass %d: block %v, err %v; want the bootstrap skipped", i, block, err)
		}
	}
	if err := r.reconcileStatefulSet(ctx, c, nil, nil, nil, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	var got appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &got); err != nil {
		t.Fatal(err)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 || *got.Spec.Replicas != 3 {
		t.Fatalf("restore Jobs %d, StatefulSet replicas %d; want no restore and 3 replicas", len(jobs.Items), *got.Spec.Replicas)
	}
	if b := c.Status.Bootstrap; b == nil || !b.Completed || !strings.Contains(b.Message, "skipped") {
		t.Fatalf("bootstrap status %+v; want completed, and saying it was skipped", b)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapSkipped") != 1 {
		t.Fatalf("events:\n%s", ev)
	}
}

// The StatefulSet of a cluster being bootstrapped is created, at 0
// replicas, by the bootstrap itself: a later pass must not take it for a
// running cluster and skip the restore.
func TestBootstrapOfANewClusterIsNotSkipped(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("fresh")
	r := shardedReconciler(t, append(objs, c)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	var got kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, req.NamespacedName, &sts); err != nil {
		t.Fatal(err)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs); err != nil {
		t.Fatal(err)
	}
	if b := got.Status.Bootstrap; b == nil || b.Completed || b.Phase != kividbv1alpha1.BootstrapInProgress {
		t.Fatalf("bootstrap status %+v; want the restore in progress", b)
	}
	if len(jobs.Items) != 1 || *sts.Spec.Replicas != 0 {
		t.Fatalf("restore Jobs %d, StatefulSet replicas %d; want the restore Job and the StatefulSet held at 0", len(jobs.Items), *sts.Spec.Replicas)
	}
	if ev := events(r); strings.Contains(ev, "BootstrapSkipped") {
		t.Fatalf("events:\n%s", ev)
	}
}

// Issue #33 put the namespace in backup keys (<ns>.<cluster>/). A
// snapshot taken before still restores: the restore Job reads the key the
// snapshot recorded, never one rebuilt from today's layout.
func TestBootstrapRestoresAnOldLayoutSnapshotByItsStoredKey(t *testing.T) {
	ctx := context.Background()
	const key = "prod/cache/cache-1-20261001T000000Z.tar.gz"
	snap := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-old", Namespace: "default"},
		Status:     kividbv1alpha1.KividbSnapshotStatus{Phase: kividbv1alpha1.SnapshotSucceeded, ObjectKey: key},
	}
	cfg := &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "default"}}
	cfg.Spec.S3.PathPrefix = "prod"
	c := testCluster("cache")
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	job, err := shardedReconciler(t, c).ensureBootstrapJob(ctx, c, snap, cfg)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--object-key "+key+" ") {
		t.Fatalf("restore args %q, want --object-key %s", args, key)
	}
}
