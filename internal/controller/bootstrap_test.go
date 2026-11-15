package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// A refused bootstrap ends the pass before updateStatus, so the phase
// stayed empty, and the Bootstrapped condition said RestoreFailed though no
// restore ran (#51). Once the spec names a usable snapshot, the restore
// runs and the refusal is no longer reported.
func TestARefusedBootstrapShowsInThePhaseUntilFixed(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("refused")
	sharded := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "shd-backup-now", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbSnapshotSpec{SnapshotConfigRef: corev1.LocalObjectReference{Name: "s3"}},
		Status: kividbv1alpha1.KividbSnapshotStatus{
			Phase:     kividbv1alpha1.SnapshotSucceeded,
			ObjectKey: "e2e/shd/20261031T065615Z/manifest.json",
			Shards:    []kividbv1alpha1.SnapshotShardStatus{{Index: 0}, {Index: 1}},
		},
	}
	c.Spec.BootstrapFromSnapshot.SnapshotRef.Name = sharded.Name
	r := shardedReconciler(t, append(objs, c, sharded)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
	get := func() *kividbv1alpha1.KividbCluster {
		t.Helper()
		var got kividbv1alpha1.KividbCluster
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return &got
	}

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	got := get()
	cond := findCondition(got.Status.Conditions, BootstrapCondition)
	if got.Status.Phase != kividbv1alpha1.PhaseError || cond == nil || cond.Reason != "Refused" || !strings.Contains(cond.Message, "sharded cluster's backup") {
		t.Fatalf("phase %q, Bootstrapped condition %+v; want Error, and Refused saying why", got.Status.Phase, cond)
	}

	// The spec now names a replicated cluster's backup.
	got.Spec.BootstrapFromSnapshot.SnapshotRef.Name = "old-backup"
	if err := r.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got = get()
	if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || b.Error != "" {
		t.Fatalf("bootstrap status %+v; want the restore in progress, with no error", b)
	}
	cond = findCondition(got.Status.Conditions, BootstrapCondition)
	if got.Status.Phase != kividbv1alpha1.PhaseProvisioning || cond == nil || cond.Reason != "Restoring" {
		t.Fatalf("phase %q, Bootstrapped condition %+v; want Provisioning, and Restoring", got.Status.Phase, cond)
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
	c.UID = types.UID("uid-" + name)
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
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c, kividbv1alpha1.GroupVersion.WithKind("KividbCluster"))}},
		Spec: appsv1.StatefulSetSpec{Replicas: &three}}
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

// A StatefulSet with the cluster's name that this cluster does not
// control, or that is being deleted, or pods left after it, are from an
// earlier cluster of the same name: the bootstrap waits for them to go,
// without touching them, and then restores.
func TestBootstrapWaitsForAnEarlierClustersStatefulSet(t *testing.T) {
	now := metav1.Now()
	isController := true
	three := int32(3)
	sts := func(meta metav1.ObjectMeta) func(*kividbv1alpha1.KividbCluster) client.Object {
		return func(c *kividbv1alpha1.KividbCluster) client.Object {
			meta.Name, meta.Namespace = c.Name, c.Namespace
			return &appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{Replicas: &three}}
		}
	}
	for _, tc := range []struct {
		name    string
		earlier func(c *kividbv1alpha1.KividbCluster) client.Object
	}{
		{"owned by another cluster", func(c *kividbv1alpha1.KividbCluster) client.Object {
			return sts(metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "kividb.io/v1alpha1", Kind: "KividbCluster",
				Name: c.Name, UID: "uid-earlier", Controller: &isController}}})(c)
		}},
		{"orphaned", sts(metav1.ObjectMeta{})},
		{"ours, being deleted", func(c *kividbv1alpha1.KividbCluster) client.Object {
			return sts(metav1.ObjectMeta{DeletionTimestamp: &now, Finalizers: []string{"test/hold"},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c, kividbv1alpha1.GroupVersion.WithKind("KividbCluster"))}})(c)
		}},
		// The garbage collector deletes the StatefulSet at once; its
		// pod-0 then terminates, still on data-<name>-0.
		{"its pod still terminating", func(c *kividbv1alpha1.KividbCluster) client.Object {
			return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: c.Name + "-0", Namespace: c.Namespace, Labels: selectorLabels(c),
				DeletionTimestamp: &now, Finalizers: []string{"test/hold"}},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrapPVCName(c)}}}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, objs := bootstrapFixture("again")
			old := tc.earlier(c)
			r := shardedReconciler(t, append(objs, c, old)...)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
			if err := r.Get(ctx, client.ObjectKeyFromObject(old), old); err != nil {
				t.Fatal(err)
			}
			rv := old.GetResourceVersion()

			for i := 0; i < 2; i++ {
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatalf("pass %d: %v", i, err)
				}
			}
			var got kividbv1alpha1.KividbCluster
			if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if b := got.Status.Bootstrap; b == nil || b.Completed || b.Phase != kividbv1alpha1.BootstrapPending ||
				b.SnapshotName != "old-backup" || b.Message != "waiting for the StatefulSet, pods and restore Job of an earlier cluster with this name to be deleted" {
				t.Fatalf("bootstrap status %+v; want pending, waiting for the earlier cluster", b)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(old), old); err != nil {
				t.Fatal(err)
			}
			if old.GetResourceVersion() != rv {
				t.Fatalf("earlier %T written to: owners %+v", old, old.GetOwnerReferences())
			}
			var jobs batchv1.JobList
			if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
				t.Fatalf("restore Jobs %d (%v); want none while waiting", len(jobs.Items), err)
			}
			if ev := events(r); strings.Count(ev, "Warning BootstrapWaiting") != 1 || strings.Contains(ev, "BootstrapSkipped") {
				t.Fatalf("events:\n%s", ev)
			}

			// The garbage collector, or someone, removes it.
			old.SetFinalizers(nil)
			if err := r.Update(ctx, old); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(ctx, old); client.IgnoreNotFound(err) != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			var sts appsv1.StatefulSet
			if err := r.Get(ctx, req.NamespacedName, &sts); err != nil {
				t.Fatal(err)
			}
			if err := r.List(ctx, &jobs); err != nil {
				t.Fatal(err)
			}
			if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || b.Message != "restore Job running" {
				t.Fatalf("bootstrap status %+v; want the restore in progress", b)
			}
			if len(jobs.Items) != 1 || *sts.Spec.Replicas != 0 || !metav1.IsControlledBy(&sts, &got) {
				t.Fatalf("restore Jobs %d, StatefulSet replicas %d, owners %+v; want the restore Job and a StatefulSet of ours at 0",
					len(jobs.Items), *sts.Spec.Replicas, sts.OwnerReferences)
			}
		})
	}

	// Removing spec.bootstrapFromSnapshot while waiting cancels the wait:
	// the cluster gets its StatefulSet once the earlier one is gone.
	t.Run("removing the field cancels the wait", func(t *testing.T) {
		ctx := context.Background()
		c, objs := bootstrapFixture("again")
		old := sts(metav1.ObjectMeta{})(c)
		r := shardedReconciler(t, append(objs, c, old)...)
		req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
		var got kividbv1alpha1.KividbCluster
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapPending {
			t.Fatalf("bootstrap status %+v; want waiting before the field is removed", b)
		}
		got.Spec.BootstrapFromSnapshot = nil
		if err := r.Update(ctx, &got); err != nil {
			t.Fatal(err)
		}
		if err := r.Delete(ctx, old); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, req.NamespacedName, &sts); err != nil {
			t.Fatalf("no StatefulSet after the field was removed and the earlier one deleted: %v (bootstrap status %+v)", err, got.Status.Bootstrap)
		}
		var jobs batchv1.JobList
		if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 || got.Status.Bootstrap != nil {
			t.Fatalf("restore Jobs %d (%v), bootstrap status %+v; want no restore and no bootstrap status", len(jobs.Items), err, got.Status.Bootstrap)
		}
	})
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

// earlierStatefulSet is the StatefulSet a deleted cluster with c's name
// left behind: same name, controlled by the earlier cluster's UID, running.
func earlierStatefulSet(c *kividbv1alpha1.KividbCluster) *appsv1.StatefulSet {
	isController := true
	three := int32(3)
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: c.Name, Namespace: c.Namespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "kividb.io/v1alpha1", Kind: "KividbCluster",
			Name: c.Name, UID: "uid-earlier", Controller: &isController}}},
		Spec: appsv1.StatefulSetSpec{Replicas: &three}}
}

// laggingCacheClient serves the next read of a KividbCluster after status
// writes as it was before the first of them: the manager's informer cache
// has not seen those writes yet. The read after that is current.
func laggingCacheClient(r *KividbClusterReconciler) {
	var stale *kividbv1alpha1.KividbCluster
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if c, ok := obj.(*kividbv1alpha1.KividbCluster); ok && stale != nil {
				stale.DeepCopyInto(c)
				stale = nil
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*kividbv1alpha1.KividbCluster); ok && stale == nil {
				before := &kividbv1alpha1.KividbCluster{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), before); err != nil {
					return err
				}
				stale = before
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// Issue #43: re-created at once after a delete, the cluster saw the
// earlier cluster's StatefulSet and waited, then, on a read of itself from
// a cache that had not yet seen the wait, updated that StatefulSet: its
// controller reference (matched by name) got the new UID, so the garbage
// collector kept it, and the next pass took it for this cluster's, running,
// and skipped the restore. The earlier StatefulSet is never written to.
func TestBootstrapNeverTakesOverAnEarlierClustersStatefulSet(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	old := earlierStatefulSet(c)
	r := shardedReconciler(t, append(objs, c, old)...)
	laggingCacheClient(r)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
	if err := r.Get(ctx, client.ObjectKeyFromObject(old), old); err != nil {
		t.Fatal(err)
	}
	rv := old.ResourceVersion

	for i := 0; i < 3; i++ {
		// A pass that starts on the stale read fails its status write
		// (conflict) and is retried: that is fine.
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Logf("pass %d: %v", i, err)
		}
	}
	var got appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(old), &got); err != nil {
		t.Fatal(err)
	}
	if got.ResourceVersion != rv || got.OwnerReferences[0].UID != "uid-earlier" {
		t.Fatalf("earlier StatefulSet written to: replicas %d, owners %+v", *got.Spec.Replicas, got.OwnerReferences)
	}
	var cl kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &cl); err != nil {
		t.Fatal(err)
	}
	if b := cl.Status.Bootstrap; b == nil || b.Completed || b.Message != bootstrapWaitMessage {
		t.Fatalf("bootstrap status %+v; want waiting for the earlier cluster", b)
	}
	if ev := events(r); strings.Contains(ev, "BootstrapSkipped") {
		t.Fatalf("events:\n%s", ev)
	}
}

// The same, one level down: whatever the cluster's status says, a cluster
// that is to be bootstrapped does not update a StatefulSet another cluster
// (by UID) controls, or none does.
func TestStatefulSetStepLeavesAnEarlierClustersStatefulSetWhileBootstrapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *kividbv1alpha1.BootstrapStatus
	}{
		{"no status yet", nil},
		{"waiting", &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, Message: bootstrapWaitMessage}},
		{"restoring", &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapInProgress, Message: "restore Job running"}},
	} {
		for _, orphan := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, orphaned %v", tc.name, orphan), func(t *testing.T) {
				ctx := context.Background()
				c, objs := bootstrapFixture("again")
				c.Status.Bootstrap = tc.status
				old := earlierStatefulSet(c)
				if orphan {
					old.OwnerReferences = nil
				}
				r := shardedReconciler(t, append(objs, c, old)...)
				if err := r.Get(ctx, client.ObjectKeyFromObject(old), old); err != nil {
					t.Fatal(err)
				}
				rv := old.ResourceVersion
				err := r.reconcileStatefulSet(ctx, c, nil, nil, nil, map[string]string{})
				var got appsv1.StatefulSet
				if gerr := r.Get(ctx, client.ObjectKeyFromObject(old), &got); gerr != nil {
					t.Fatal(gerr)
				}
				if got.ResourceVersion != rv {
					t.Fatalf("earlier StatefulSet written to (err %v): replicas %d, owners %+v", err, *got.Spec.Replicas, got.OwnerReferences)
				}
				if err == nil || !strings.Contains(err.Error(), "earlier cluster") {
					t.Fatalf("err %v; want the refusal, naming the earlier cluster", err)
				}
			})
		}
	}

	// Once the bootstrap is done, or without one, an orphaned StatefulSet
	// is still adopted, as before.
	t.Run("not bootstrapping", func(t *testing.T) {
		ctx := context.Background()
		c := testCluster("plain")
		c.UID = "uid-plain"
		c.Spec.Replicas = 1
		c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
		old := earlierStatefulSet(c)
		old.OwnerReferences = nil
		r := shardedReconciler(t, c, old)
		if err := r.reconcileStatefulSet(ctx, c, nil, nil, nil, map[string]string{}); err != nil {
			t.Fatal(err)
		}
		var got appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKeyFromObject(old), &got); err != nil {
			t.Fatal(err)
		}
		if !metav1.IsControlledBy(&got, c) {
			t.Fatalf("owners %+v; want adopted", got.OwnerReferences)
		}
	})
}

// The StatefulSet the bootstrap itself created is held at 0 replicas: one
// of ours at 0 is that one, whatever the status says (a write lost, a stale
// read), and the restore goes on. Only one of ours that has run (replicas
// above 0) means the cluster is already running.
func TestBootstrapIsNotSkippedForItsOwnStatefulSetAtZero(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *kividbv1alpha1.BootstrapStatus
	}{
		{"no status", nil},
		{"waiting", &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, Message: bootstrapWaitMessage}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, objs := bootstrapFixture("fresh")
			c.Status.Bootstrap = tc.status
			zero := int32(0)
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: c.Name, Namespace: c.Namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c, kividbv1alpha1.GroupVersion.WithKind("KividbCluster"))}},
				Spec: appsv1.StatefulSetSpec{Replicas: &zero}}
			r := shardedReconciler(t, append(objs, c, sts)...)

			block, err := r.reconcileBootstrap(ctx, c)
			if !block || err != nil {
				t.Fatalf("block %v, err %v; want the restore going on", block, err)
			}
			if b := c.Status.Bootstrap; b == nil || b.Completed || b.Phase != kividbv1alpha1.BootstrapInProgress {
				t.Fatalf("bootstrap status %+v; want the restore in progress", b)
			}
			var jobs batchv1.JobList
			if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 1 {
				t.Fatalf("restore Jobs %d (%v); want 1", len(jobs.Items), err)
			}
			if ev := events(r); strings.Contains(ev, "BootstrapSkipped") {
				t.Fatalf("events:\n%s", ev)
			}
		})
	}
}

// earlierBootstrapJob is the restore Job an earlier cluster with c's name
// ran, and that finished: TTL keeps it a day, and with --cascade=orphan the
// garbage collector does not delete it.
func earlierBootstrapJob(c *kividbv1alpha1.KividbCluster) *batchv1.Job {
	isController := true
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: bootstrapJobName(c), Namespace: c.Namespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "kividb.io/v1alpha1", Kind: "KividbCluster",
			Name: c.Name, UID: "uid-earlier", Controller: &isController}}},
		Status: batchv1.JobStatus{Succeeded: 1}}
}

// The earlier cluster's finished restore Job is not this cluster's: it
// marked the bootstrap Completed ("PVC seeded from snapshot") with no
// restore run for it. The bootstrap waits for it to go, then restores.
func TestBootstrapWaitsForAnEarlierClustersRestoreJob(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	old := earlierBootstrapJob(c)
	r := shardedReconciler(t, append(objs, c, old)...)
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
	if b := got.Status.Bootstrap; b == nil || b.Completed || b.Phase != kividbv1alpha1.BootstrapPending || b.Message != bootstrapWaitMessage {
		t.Fatalf("bootstrap status %+v; want waiting for the earlier cluster's restore Job", b)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapWaiting") != 1 || !strings.Contains(ev, bootstrapJobName(c)) {
		t.Fatalf("events:\n%s", ev)
	}

	if err := r.Delete(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := r.Get(ctx, client.ObjectKeyFromObject(old), &job); err != nil {
		t.Fatal(err)
	}
	if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || !metav1.IsControlledBy(&job, &got) {
		t.Fatalf("bootstrap status %+v, Job owners %+v; want our own restore Job running", b, job.OwnerReferences)
	}
}

// Past the wait, too, a restore Job that is not this cluster's is never
// taken for its restore.
func TestBootstrapJobOfAnotherClusterIsNotUsed(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, SnapshotName: "old-backup", Message: "bootstrap requested"}
	r := shardedReconciler(t, append(objs, c, earlierBootstrapJob(c))...)

	block, err := r.reconcileBootstrap(ctx, c)
	if !block || err != nil || c.Status.Bootstrap.Completed || !bootstrapWaiting(c) {
		t.Fatalf("block %v, err %v, status %+v; want blocked, waiting for the earlier cluster's Job", block, err, c.Status.Bootstrap)
	}
	if cond := findCondition(c.Status.Conditions, BootstrapCondition); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WaitingForEarlierCluster" {
		t.Fatalf("Bootstrapped condition %+v; want False, WaitingForEarlierCluster", cond)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapWaiting") != 1 {
		t.Fatalf("events:\n%s", ev)
	}
}

// Issue #43: a cluster re-created with the name of a deleted one finds
// pod-0's volume kept (PVC retention is Retain); a PVC made beforehand on
// purpose is older than the cluster too. The restore replaces its data,
// as the spec asks; the status and a Warning say so, without guessing
// where the volume came from.
func TestBootstrapSaysItRestoresOverAKeptVolume(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	c.CreationTimestamp = metav1.Now()
	kept := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: bootstrapPVCName(c), Namespace: c.Namespace,
		CreationTimestamp: metav1.NewTime(c.CreationTimestamp.Add(-time.Hour))}}
	r := shardedReconciler(t, append(objs, c, kept)...)
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
	want := "restoring over the existing volume data-again-0, which is older than this cluster"
	if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || !strings.Contains(b.Message, want) {
		t.Fatalf("bootstrap status %+v; want the restore in progress, saying %q", b, want)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapOverExistingVolume") != 1 || !strings.Contains(ev, want) {
		t.Fatalf("events:\n%s", ev)
	}

	// The Job finishes: the completed status still says what it replaced.
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if b := got.Status.Bootstrap; b == nil || !b.Completed || !strings.Contains(b.Message, "data-again-0") {
		t.Fatalf("bootstrap status %+v; want completed, naming the volume it restored over", b)
	}
}

// bootstrapFromSnapshot added to a running cluster is skipped. The pass
// that saves the skip goes on to the StatefulSet with that status, not
// with one read back from a cache that has not seen it: there the
// bootstrap was still to come, and the running StatefulSet went to 0.
func TestSkippedBootstrapNeverScalesTheRunningClusterDown(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("live")
	three := int32(3)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c, kividbv1alpha1.GroupVersion.WithKind("KividbCluster"))}},
		Spec: appsv1.StatefulSetSpec{Replicas: &three}}
	r := shardedReconciler(t, append(objs, c, sts)...)
	laggingCacheClient(r)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var got appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &got); err != nil {
		t.Fatal(err)
	}
	if *got.Spec.Replicas != 3 {
		t.Fatalf("StatefulSet replicas %d; want the running cluster's 3", *got.Spec.Replicas)
	}
}

// ownBootstrapObjects are what this cluster's own bootstrap has made by the
// time its restore Job is done: the Job (Succeeded), and the StatefulSet,
// scaled up once the completion was saved.
func ownBootstrapObjects(c *kividbv1alpha1.KividbCluster) (*appsv1.StatefulSet, *batchv1.Job) {
	three := int32(3)
	ref := *metav1.NewControllerRef(c, kividbv1alpha1.GroupVersion.WithKind("KividbCluster"))
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: c.Name, Namespace: c.Namespace, OwnerReferences: []metav1.OwnerReference{ref}},
			Spec: appsv1.StatefulSetSpec{Replicas: &three}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: bootstrapJobName(c), Namespace: c.Namespace, OwnerReferences: []metav1.OwnerReference{ref}},
			Status: batchv1.JobStatus{Succeeded: 1}}
}

// A pass that reads the cluster from before its restore was saved as
// done (waiting, or no status) finds its StatefulSet already scaled up.
// With this cluster's own restore Job there, that StatefulSet is the
// restored one, not a cluster that was running without the bootstrap: no
// BootstrapSkipped, and the bootstrap stays a restore.
func TestBootstrapIsNotSkippedWhenItsOwnRestoreJobExists(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *kividbv1alpha1.BootstrapStatus
	}{
		{"no status", nil},
		{"waiting", &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, SnapshotName: "old-backup", Message: bootstrapWaitMessage}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, objs := bootstrapFixture("again")
			c.Status.Bootstrap = tc.status
			sts, job := ownBootstrapObjects(c)
			r := shardedReconciler(t, append(objs, c, sts, job)...)

			if _, err := r.reconcileBootstrap(ctx, c); err != nil {
				t.Fatal(err)
			}
			if b := c.Status.Bootstrap; b == nil || !b.Completed || strings.Contains(b.Message, "skipped") {
				t.Fatalf("bootstrap status %+v; want completed as restored, not skipped", b)
			}
			if ev := events(r); strings.Contains(ev, "BootstrapSkipped") {
				t.Fatalf("events:\n%s", ev)
			}
		})
	}
}

// conflictingStatusWrites fails the next n status writes to a
// KividbCluster with a conflict, as when another pass saved first.
func conflictingStatusWrites(r *KividbClusterReconciler, n *int) {
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*kividbv1alpha1.KividbCluster); ok && *n > 0 {
				*n--
				return apierrors.NewConflict(schema.GroupResource{Group: "kividb.io", Resource: "kividbclusters"}, obj.GetName(), errors.New("injected"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// Issue #43's sequence on the way out of the wait, with the status writes
// that the API server and the cache make racy: the restore's InProgress
// write loses to a conflict, the Job finishes, the next pass saves the
// completion and scales the StatefulSet up, and the pass after reads the
// cluster from before (still waiting). The bootstrap ends restored, with
// no BootstrapSkipped on the way.
func TestBootstrapCompletesWithoutASkipThroughConflictsAndStaleReads(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	// The earlier cluster's objects are gone; the wait was saved.
	c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, SnapshotName: "old-backup", Message: bootstrapWaitMessage}
	r := shardedReconciler(t, append(objs, c)...)
	laggingCacheClient(r)
	conflicts := 1
	conflictingStatusWrites(r, &conflicts)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}

	// The restore starts; its InProgress write loses.
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("pass 1: want the injected conflict")
	}
	// The restore Job finishes.
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	// Completed is saved, and the StatefulSet scaled up; then a pass on
	// the stale read; then one on a current read.
	for i := 2; i <= 4; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Logf("pass %d: %v", i, err)
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
	if b := got.Status.Bootstrap; b == nil || !b.Completed || strings.Contains(b.Message, "skipped") || *sts.Spec.Replicas != 3 {
		t.Fatalf("bootstrap status %+v, StatefulSet replicas %d; want restored and running", b, *sts.Spec.Replicas)
	}
	if ev := events(r); strings.Contains(ev, "BootstrapSkipped") {
		t.Fatalf("events:\n%s", ev)
	}
}

// A PVC being deleted is not restored onto: the bootstrap waits, with no
// restore Job, until it is gone, then creates pod-0's PVC again. The wait
// shows in status.bootstrap and the Bootstrapped condition, with one Normal
// Event, not only in the operator's log (issue #55).
func TestBootstrapWaitsForAPVCBeingDeleted(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("again")
	c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapPending, SnapshotName: "old-backup", Message: "bootstrap requested"}
	now := metav1.Now()
	going := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: bootstrapPVCName(c), Namespace: c.Namespace,
		DeletionTimestamp: &now, Finalizers: []string{"kubernetes.io/pvc-protection"}}}
	r := shardedReconciler(t, append(objs, c, going)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
	jobKey := types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}

	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	var job batchv1.Job
	if err := r.Get(ctx, jobKey, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("restore Job: err %v; want none while the PVC is being deleted", err)
	}
	var got kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	want := "waiting for PVC data-again-0 to finish deleting"
	if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapPending || !strings.Contains(b.Message, want) {
		t.Fatalf("bootstrap status %+v; want Pending, saying %q", b, want)
	}
	cond := findCondition(got.Status.Conditions, BootstrapCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WaitingForVolumeDeletion" || !strings.Contains(cond.Message, want) {
		t.Fatalf("Bootstrapped condition %+v; want False, WaitingForVolumeDeletion, saying %q", cond, want)
	}
	if ev := events(r); strings.Count(ev, "Normal BootstrapWaitingForVolume") != 1 || !strings.Contains(ev, want) {
		t.Fatalf("events (want one BootstrapWaitingForVolume over three passes):\n%s", ev)
	}

	// The last finalizer goes, and with it the PVC.
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKeyFromObject(going), &pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Finalizers = nil
	if err := r.Update(ctx, &pvc); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("after the PVC is gone: %v", err)
	}
	var fresh corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKeyFromObject(going), &fresh); err != nil || fresh.DeletionTimestamp != nil {
		t.Fatalf("PVC %+v, err %v; want a new one", fresh.ObjectMeta, err)
	}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		t.Fatalf("restore Job: %v; want it created once the PVC is gone", err)
	}
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress {
		t.Fatalf("bootstrap status %+v; want InProgress", b)
	}
	if cond := findCondition(got.Status.Conditions, BootstrapCondition); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Restoring" {
		t.Fatalf("Bootstrapped condition %+v; want False, Restoring", cond)
	}
}

// A restore Job whose pod failed while it has a retry left has not failed:
// status.bootstrap stays InProgress and says which attempt failed and why
// (issue #54). Only the Job's JobFailed condition fails the bootstrap.
func TestBootstrapRestoreJobRetryIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("retry")
	r := shardedReconciler(t, append(objs, c)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
	jobKey := types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := r.Get(ctx, jobKey, &job); err != nil {
		t.Fatal(err)
	}
	if got := job.Spec.Template.Spec.Containers[0].TerminationMessagePolicy; got != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatalf("restore container terminationMessagePolicy %q", got)
	}
	// The first pod fails; the Job has a retry left (backoffLimit 1).
	job.Status.Failed = 1
	job.Status.Active = 1
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-bootstrap-a1b2c", Namespace: c.Namespace, Labels: bootstrapLabels(c),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&job, batchv1.SchemeGroupVersion.WithKind("Job"))}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "restore",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error",
				Message: "downloading rep/rep-0.tar.gz\nrestore-from-s3: rename dump.kdb: permission denied\n"}}}}},
	}
	if err := r.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	var got kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	b := got.Status.Bootstrap
	if b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || b.Completed || b.Error != "" ||
		!strings.Contains(b.Message, "restore Job attempt 1 failed, retrying") ||
		!strings.Contains(b.Message, "pod retry-bootstrap-a1b2c: Error, exit code 1: restore-from-s3: rename dump.kdb: permission denied") {
		t.Fatalf("bootstrap status %+v; want InProgress, retrying, with the pod's reason", b)
	}
	cond := findCondition(got.Status.Conditions, BootstrapCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "RestoreRetrying" {
		t.Fatalf("Bootstrapped condition %+v; want False, RestoreRetrying", cond)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapRetrying") != 1 || strings.Contains(ev, "BootstrapFailed") {
		t.Fatalf("events (want one BootstrapRetrying, no BootstrapFailed):\n%s", ev)
	}

	// The retry fails too: backoffLimit is used up, and the Job has failed.
	if err := r.Get(ctx, jobKey, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Failed = 2
	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	b = got.Status.Bootstrap
	if b == nil || b.Phase != kividbv1alpha1.BootstrapFailed || b.Completed ||
		!strings.Contains(b.Error, "BackoffLimitExceeded") || !strings.Contains(b.Error, "permission denied") {
		t.Fatalf("bootstrap status %+v; want Failed, with the Job's and the pod's reasons", b)
	}
	cond = findCondition(got.Status.Conditions, BootstrapCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "RestoreFailed" {
		t.Fatalf("Bootstrapped condition %+v; want False, RestoreFailed", cond)
	}
	if ev := events(r); strings.Count(ev, "Warning BootstrapFailed") != 1 {
		t.Fatalf("events (want one BootstrapFailed):\n%s", ev)
	}
}

// The Bootstrapped condition turns True once the restore Job succeeds.
func TestBootstrapConditionIsTrueOnceRestored(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("done")
	r := shardedReconciler(t, append(objs, c)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var got kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if cond := findCondition(got.Status.Conditions, BootstrapCondition); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Restored" {
		t.Fatalf("Bootstrapped condition %+v; want True, Restored", cond)
	}
}
