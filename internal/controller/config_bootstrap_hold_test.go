package controller

import (
	"context"
	"slices"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// snapGuardReconciler is the KividbSnapshotConfig usage reconciler as
// SetupConfigUsageControllers makes it, over a fake client holding objs.
func snapGuardReconciler(t *testing.T, objs ...client.Object) (*ConfigUsageReconciler[*kividbv1alpha1.KividbSnapshotConfig], client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := newUsageTestScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kividbv1alpha1.KividbSnapshotConfig{}, &kividbv1alpha1.KividbCluster{}).
		WithIndex(&kividbv1alpha1.KividbCluster{}, ClusterSnapshotConfigRefIndex, func(obj client.Object) []string {
			c, ok := obj.(*kividbv1alpha1.KividbCluster)
			if !ok || c.Spec.SnapshotConfigRef == nil || c.Spec.SnapshotConfigRef.Name == "" {
				return nil
			}
			return []string{c.Spec.SnapshotConfigRef.Name}
		}).
		WithObjects(objs...).
		Build()
	recorder := record.NewFakeRecorder(10)
	r := &ConfigUsageReconciler[*kividbv1alpha1.KividbSnapshotConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbSnapshotConfig { return &kividbv1alpha1.KividbSnapshotConfig{} },
		IndexField: ClusterSnapshotConfigRefIndex,
		GetUsedBy:  func(s *kividbv1alpha1.KividbSnapshotConfig) []string { return s.Status.UsedBy },
		SetUsedBy:  func(s *kividbv1alpha1.KividbSnapshotConfig, u []string) { s.Status.UsedBy = u },
		ConditionsOf: func(s *kividbv1alpha1.KividbSnapshotConfig) *[]metav1.Condition {
			return &s.Status.Conditions
		},
		Recorder:       recorder,
		BootstrapUsers: true,
		ExtractClusterRefs: func(c *kividbv1alpha1.KividbCluster) []string {
			if c.Spec.SnapshotConfigRef != nil && c.Spec.SnapshotConfigRef.Name != "" {
				return []string{c.Spec.SnapshotConfigRef.Name}
			}
			return nil
		},
	}
	return r, cl, recorder
}

func snapConfig(name string) *kividbv1alpha1.KividbSnapshotConfig {
	return &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
}

// snapshotOf is a KividbSnapshot that the snapshot config cfg made.
func snapshotOf(name, cfg string) *kividbv1alpha1.KividbSnapshot {
	return &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotSpec{
			ClusterRef:        corev1.LocalObjectReference{Name: "a"},
			SnapshotConfigRef: corev1.LocalObjectReference{Name: cfg},
		},
	}
}

// restoring is a cluster bootstrapping from the snapshot snap, with no
// snapshotConfigRef of its own.
func restoring(name, snap string) *kividbv1alpha1.KividbCluster {
	return &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			BootstrapFromSnapshot: &kividbv1alpha1.BootstrapFromSnapshotSpec{
				SnapshotRef: corev1.LocalObjectReference{Name: snap},
			},
		},
	}
}

func reconcileSnapConfig(t *testing.T, r *ConfigUsageReconciler[*kividbv1alpha1.KividbSnapshotConfig], name string) {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile %s: %v", name, err)
	}
}

func getSnapConfig(t *testing.T, cl client.Client, name string) (*kividbv1alpha1.KividbSnapshotConfig, bool) {
	t.Helper()
	var cfg kividbv1alpha1.KividbSnapshotConfig
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &cfg)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}
	return &cfg, true
}

// A cluster restoring a snapshot that the config made holds the config
// until its bootstrap completes, though it does not reference the config.
// Deleting the config meanwhile leaves it Terminating with DeletionBlocked
// naming the cluster; it goes once the bootstrap is done.
func TestBootstrapHold_HeldUntilBootstrapCompletes(t *testing.T) {
	t.Parallel()
	b := restoring("b", "a-1")
	r, cl, recorder := snapGuardReconciler(t, snapConfig("nightly"), snapshotOf("a-1", "nightly"), b)
	ctx := context.Background()

	reconcileSnapConfig(t, r, "nightly")
	got, _ := getSnapConfig(t, cl, "nightly")
	if !controllerutil.ContainsFinalizer(got, ConfigInUseFinalizer) {
		t.Fatalf("config of a bootstrap's snapshot has no finalizer: %v", got.Finalizers)
	}
	if !slices.Equal(got.Status.UsedBy, []string{"b"}) {
		t.Fatalf("usedBy = %v, want [b]", got.Status.UsedBy)
	}

	if err := cl.Delete(ctx, got); err != nil {
		t.Fatalf("Delete nightly: %v", err)
	}
	reconcileSnapConfig(t, r, "nightly")
	got, ok := getSnapConfig(t, cl, "nightly")
	if !ok || got.DeletionTimestamp == nil {
		t.Fatalf("config of a bootstrap under way was removed (present=%v)", ok)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionDeletionBlocked)
	if cond == nil || cond.Status != metav1.ConditionTrue || !strings.HasSuffix(cond.Message, ": b") {
		t.Fatalf("DeletionBlocked = %+v", cond)
	}
	if e := <-recorder.Events; !strings.HasPrefix(e, "Warning InUse") || !strings.Contains(e, ": b") {
		t.Fatalf("event = %q", e)
	}

	// The restore is recorded: b no longer needs the config.
	b.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapCompleted, Completed: true}
	if err := cl.Status().Update(ctx, b); err != nil {
		t.Fatalf("Update b status: %v", err)
	}
	reconcileSnapConfig(t, r, "nightly")
	if _, ok := getSnapConfig(t, cl, "nightly"); ok {
		t.Fatal("config still there once the bootstrap completed")
	}
}

// A bootstrap that failed, or waits, is not done: it reads the config on
// its next pass, so it still holds it.
func TestBootstrapHold_FailedBootstrapStillHolds(t *testing.T) {
	t.Parallel()
	b := restoring("b", "a-1")
	b.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Phase: kividbv1alpha1.BootstrapFailed, Error: "restore Job failed"}
	r, cl, _ := snapGuardReconciler(t, snapConfig("nightly"), snapshotOf("a-1", "nightly"), b)

	reconcileSnapConfig(t, r, "nightly")
	got, _ := getSnapConfig(t, cl, "nightly")
	if !slices.Equal(got.Status.UsedBy, []string{"b"}) {
		t.Fatalf("usedBy = %v, want [b]", got.Status.UsedBy)
	}
}

// Only the config that made the snapshot is held; one the cluster
// references itself is held as before, and a cluster that both references
// the config and restores from it is named once.
func TestBootstrapHold_OnlyTheSnapshotsConfig(t *testing.T) {
	t.Parallel()
	b := restoring("b", "a-1")
	b.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "own"}
	c := restoring("c", "a-1")
	c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "nightly"}
	r, cl, _ := snapGuardReconciler(t, snapConfig("nightly"), snapConfig("own"), snapConfig("other"),
		snapshotOf("a-1", "nightly"), b, c)

	for _, name := range []string{"nightly", "own", "other"} {
		reconcileSnapConfig(t, r, name)
	}
	for name, want := range map[string][]string{
		"nightly": {"b", "c"},
		"own":     {"b"},
		"other":   nil,
	} {
		got, _ := getSnapConfig(t, cl, name)
		if !slices.Equal(got.Status.UsedBy, want) {
			t.Errorf("%s: usedBy = %v, want %v", name, got.Status.UsedBy, want)
		}
	}
}

// A bootstrap whose snapshot is not there yet holds nothing; once the
// snapshot is made, its config gains the cluster.
func TestBootstrapHold_SnapshotMadeLater(t *testing.T) {
	t.Parallel()
	r, cl, _ := snapGuardReconciler(t, snapConfig("nightly"), restoring("b", "a-1"))
	ctx := context.Background()

	reconcileSnapConfig(t, r, "nightly")
	got, _ := getSnapConfig(t, cl, "nightly")
	if len(got.Status.UsedBy) != 0 || controllerutil.ContainsFinalizer(got, ConfigInUseFinalizer) {
		t.Fatalf("held for a snapshot not found: usedBy=%v finalizers=%v", got.Status.UsedBy, got.Finalizers)
	}

	if err := cl.Create(ctx, snapshotOf("a-1", "nightly")); err != nil {
		t.Fatalf("Create a-1: %v", err)
	}
	reconcileSnapConfig(t, r, "nightly")
	got, _ = getSnapConfig(t, cl, "nightly")
	if !slices.Equal(got.Status.UsedBy, []string{"b"}) {
		t.Fatalf("usedBy = %v, want [b]", got.Status.UsedBy)
	}
}

// A cluster's events enqueue the config of its bootstrap's snapshot while
// the bootstrap is not done, and the one it references, as before.
func TestBootstrapHold_MapClusterEvents(t *testing.T) {
	t.Parallel()
	r, _, _ := snapGuardReconciler(t, snapshotOf("a-1", "nightly"))
	funcs, ok := r.mapClusterEvents().(*handler.Funcs)
	if !ok {
		t.Fatal("mapClusterEvents is not *handler.Funcs")
	}
	ctx := context.Background()
	queued := func(fire func(workqueue.TypedRateLimitingInterface[reconcile.Request])) []string {
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()
		fire(q)
		var names []string
		for q.Len() > 0 {
			item, _ := q.Get()
			names = append(names, item.Name)
			q.Done(item)
		}
		slices.Sort(names)
		return names
	}

	pending := restoring("b", "a-1")
	pending.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "own"}
	done := pending.DeepCopy()
	done.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{Completed: true}

	if got := queued(func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		funcs.Create(ctx, event.CreateEvent{Object: pending}, q)
	}); !slices.Equal(got, []string{"nightly", "own"}) {
		t.Errorf("create: queued %v, want [nightly own]", got)
	}
	// The completion is what lets the config go: it must be enqueued.
	if got := queued(func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		funcs.Update(ctx, event.UpdateEvent{ObjectOld: pending, ObjectNew: done}, q)
	}); !slices.Equal(got, []string{"nightly", "own"}) {
		t.Errorf("completion: queued %v, want [nightly own]", got)
	}
	if got := queued(func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		funcs.Delete(ctx, event.DeleteEvent{Object: done}, q)
	}); !slices.Equal(got, []string{"own"}) {
		t.Errorf("delete after completion: queued %v, want [own]", got)
	}
	if got := queued(func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		funcs.Delete(ctx, event.DeleteEvent{Object: restoring("x", "missing")}, q)
	}); len(got) != 0 {
		t.Errorf("bootstrap of a snapshot not found: queued %v, want none", got)
	}
}

// KividbConfig and KividbAclConfig do not count bootstraps.
func TestBootstrapHold_OtherConfigsUnchanged(t *testing.T) {
	t.Parallel()
	b := restoring("b", "a-1")
	b.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "cfg"}
	r, _, _ := guardTestReconciler(t, snapshotOf("a-1", "cfg"), b)
	r.ExtractClusterRefs = func(c *kividbv1alpha1.KividbCluster) []string { return nil }
	if refs := r.clusterRefs(context.Background(), b); len(refs) != 0 {
		t.Fatalf("KividbConfig refs of a bootstrapping cluster = %v, want none", refs)
	}
}
