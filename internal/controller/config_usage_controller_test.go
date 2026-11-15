package controller

import (
	"context"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newUsageTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := kividbv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func TestConfigUsageReconciler_RecomputeUsedBy(t *testing.T) {
	t.Parallel()

	scheme := newUsageTestScheme(t)

	cfg := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-shared", Namespace: "default"},
	}
	c1 := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-1", Namespace: "default"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "cfg-shared"},
		},
	}
	c2 := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-2", Namespace: "default"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "cfg-shared"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kividbv1alpha1.KividbConfig{}).
		WithIndex(&kividbv1alpha1.KividbCluster{}, ClusterConfigRefIndex, func(obj client.Object) []string {
			c, ok := obj.(*kividbv1alpha1.KividbCluster)
			if !ok || c.Spec.ConfigRef == nil || c.Spec.ConfigRef.Name == "" {
				return nil
			}
			return []string{c.Spec.ConfigRef.Name}
		}).
		WithObjects(cfg, c1, c2).
		Build()

	r := &ConfigUsageReconciler[*kividbv1alpha1.KividbConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbConfig { return &kividbv1alpha1.KividbConfig{} },
		IndexField: ClusterConfigRefIndex,
		GetUsedBy: func(c *kividbv1alpha1.KividbConfig) []string {
			return c.Status.UsedBy
		},
		SetUsedBy: func(c *kividbv1alpha1.KividbConfig, usedBy []string) {
			c.Status.UsedBy = usedBy
		},
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "cfg-shared"}}

	// Step 1: Two clusters referencing cfg-shared -> both in usedBy sorted.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	var updated kividbv1alpha1.KividbConfig
	if err := cl.Get(ctx, req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get cfg: %v", err)
	}
	if len(updated.Status.UsedBy) != 2 || updated.Status.UsedBy[0] != "cluster-1" || updated.Status.UsedBy[1] != "cluster-2" {
		t.Fatalf("step 1: expected [cluster-1, cluster-2], got %v", updated.Status.UsedBy)
	}

	// Step 2: Delete cluster-1 -> usedBy drops cluster-1 immediately.
	if err := cl.Delete(ctx, c1); err != nil {
		t.Fatalf("Delete cluster-1: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile step 2: %v", err)
	}
	if err := cl.Get(ctx, req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get cfg step 2: %v", err)
	}
	if len(updated.Status.UsedBy) != 1 || updated.Status.UsedBy[0] != "cluster-2" {
		t.Fatalf("step 2: expected [cluster-2], got %v", updated.Status.UsedBy)
	}

	// Step 3: Switch cluster-2 to another config -> cfg-shared usedBy becomes empty.
	cfgAlt := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-alt", Namespace: "default"},
	}
	if err := cl.Create(ctx, cfgAlt); err != nil {
		t.Fatalf("Create cfg-alt: %v", err)
	}

	c2.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "cfg-alt"}
	if err := cl.Update(ctx, c2); err != nil {
		t.Fatalf("Update cluster-2: %v", err)
	}

	// Reconcile cfg-shared
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile step 3 (cfg-shared): %v", err)
	}
	if err := cl.Get(ctx, req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get cfg-shared step 3: %v", err)
	}
	if len(updated.Status.UsedBy) != 0 {
		t.Fatalf("step 3: expected empty usedBy for cfg-shared, got %v", updated.Status.UsedBy)
	}

	// Reconcile cfg-alt
	reqAlt := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "cfg-alt"}}
	if _, err := r.Reconcile(ctx, reqAlt); err != nil {
		t.Fatalf("Reconcile step 3 (cfg-alt): %v", err)
	}
	var updatedAlt kividbv1alpha1.KividbConfig
	if err := cl.Get(ctx, reqAlt.NamespacedName, &updatedAlt); err != nil {
		t.Fatalf("Get cfg-alt: %v", err)
	}
	if len(updatedAlt.Status.UsedBy) != 1 || updatedAlt.Status.UsedBy[0] != "cluster-2" {
		t.Fatalf("step 3: expected [cluster-2] for cfg-alt, got %v", updatedAlt.Status.UsedBy)
	}

	// Step 4: Remove configRef entirely (set to nil) -> cfg-alt usedBy becomes empty.
	c2.Spec.ConfigRef = nil
	if err := cl.Update(ctx, c2); err != nil {
		t.Fatalf("Update cluster-2 to nil configRef: %v", err)
	}
	if _, err := r.Reconcile(ctx, reqAlt); err != nil {
		t.Fatalf("Reconcile step 4 (cfg-alt): %v", err)
	}
	if err := cl.Get(ctx, reqAlt.NamespacedName, &updatedAlt); err != nil {
		t.Fatalf("Get cfg-alt step 4: %v", err)
	}
	if len(updatedAlt.Status.UsedBy) != 0 {
		t.Fatalf("step 4: expected empty usedBy for cfg-alt after removing configRef, got %v", updatedAlt.Status.UsedBy)
	}
}

func TestConfigUsageReconciler_AclAndSnapshot(t *testing.T) {
	t.Parallel()

	scheme := newUsageTestScheme(t)

	acl := &kividbv1alpha1.KividbAclConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "acl-1", Namespace: "default"},
	}
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1", Namespace: "default"},
	}
	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-alpha", Namespace: "default"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			AclConfigRef:      &corev1.LocalObjectReference{Name: "acl-1"},
			SnapshotConfigRef: &corev1.LocalObjectReference{Name: "snap-1"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kividbv1alpha1.KividbAclConfig{}, &kividbv1alpha1.KividbSnapshotConfig{}).
		WithIndex(&kividbv1alpha1.KividbCluster{}, ClusterAclConfigRefIndex, func(obj client.Object) []string {
			cluster := obj.(*kividbv1alpha1.KividbCluster)
			if cluster.Spec.AclConfigRef == nil || cluster.Spec.AclConfigRef.Name == "" {
				return nil
			}
			return []string{cluster.Spec.AclConfigRef.Name}
		}).
		WithIndex(&kividbv1alpha1.KividbCluster{}, ClusterSnapshotConfigRefIndex, func(obj client.Object) []string {
			cluster := obj.(*kividbv1alpha1.KividbCluster)
			if cluster.Spec.SnapshotConfigRef == nil || cluster.Spec.SnapshotConfigRef.Name == "" {
				return nil
			}
			return []string{cluster.Spec.SnapshotConfigRef.Name}
		}).
		WithObjects(acl, snap, c).
		Build()

	aclReconciler := &ConfigUsageReconciler[*kividbv1alpha1.KividbAclConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbAclConfig { return &kividbv1alpha1.KividbAclConfig{} },
		IndexField: ClusterAclConfigRefIndex,
		GetUsedBy:  func(a *kividbv1alpha1.KividbAclConfig) []string { return a.Status.UsedBy },
		SetUsedBy:  func(a *kividbv1alpha1.KividbAclConfig, u []string) { a.Status.UsedBy = u },
	}

	snapReconciler := &ConfigUsageReconciler[*kividbv1alpha1.KividbSnapshotConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbSnapshotConfig { return &kividbv1alpha1.KividbSnapshotConfig{} },
		IndexField: ClusterSnapshotConfigRefIndex,
		GetUsedBy:  func(s *kividbv1alpha1.KividbSnapshotConfig) []string { return s.Status.UsedBy },
		SetUsedBy:  func(s *kividbv1alpha1.KividbSnapshotConfig, u []string) { s.Status.UsedBy = u },
	}

	ctx := context.Background()

	if _, err := aclReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "acl-1"}}); err != nil {
		t.Fatalf("Acl Reconcile: %v", err)
	}
	if _, err := snapReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "snap-1"}}); err != nil {
		t.Fatalf("Snap Reconcile: %v", err)
	}

	var updatedAcl kividbv1alpha1.KividbAclConfig
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "acl-1"}, &updatedAcl); err != nil {
		t.Fatalf("Get acl: %v", err)
	}
	if len(updatedAcl.Status.UsedBy) != 1 || updatedAcl.Status.UsedBy[0] != "cluster-alpha" {
		t.Fatalf("expected acl usedBy [cluster-alpha], got %v", updatedAcl.Status.UsedBy)
	}

	var updatedSnap kividbv1alpha1.KividbSnapshotConfig
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "snap-1"}, &updatedSnap); err != nil {
		t.Fatalf("Get snap: %v", err)
	}
	if len(updatedSnap.Status.UsedBy) != 1 || updatedSnap.Status.UsedBy[0] != "cluster-alpha" {
		t.Fatalf("expected snap usedBy [cluster-alpha], got %v", updatedSnap.Status.UsedBy)
	}
}

func TestConfigUsageReconciler_MapClusterEvents(t *testing.T) {
	t.Parallel()

	r := &ConfigUsageReconciler[*kividbv1alpha1.KividbConfig]{
		ExtractClusterRefs: func(c *kividbv1alpha1.KividbCluster) []string {
			if c.Spec.ConfigRef != nil && c.Spec.ConfigRef.Name != "" {
				return []string{c.Spec.ConfigRef.Name}
			}
			return nil
		},
	}

	h := r.mapClusterEvents()
	funcs, ok := h.(*handler.Funcs)
	if !ok {
		t.Fatalf("expected *handler.Funcs, got %T", h)
	}

	ctx := context.Background()

	t.Run("Create enqueues referenced config", func(t *testing.T) {
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()

		c := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: &corev1.LocalObjectReference{Name: "cfg-1"}},
		}
		funcs.Create(ctx, event.CreateEvent{Object: c}, q)

		if q.Len() != 1 {
			t.Fatalf("expected queue length 1, got %d", q.Len())
		}
		item, _ := q.Get()
		if item.Namespace != "prod" || item.Name != "cfg-1" {
			t.Fatalf("unexpected request: %v", item)
		}
	})

	t.Run("Update enqueues both old and new referenced configs", func(t *testing.T) {
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()

		oldC := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: &corev1.LocalObjectReference{Name: "cfg-old"}},
		}
		newC := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: &corev1.LocalObjectReference{Name: "cfg-new"}},
		}
		funcs.Update(ctx, event.UpdateEvent{ObjectOld: oldC, ObjectNew: newC}, q)

		if q.Len() != 2 {
			t.Fatalf("expected queue length 2 (both old and new configs), got %d", q.Len())
		}
		item1, _ := q.Get()
		item2, _ := q.Get()
		names := map[string]bool{item1.Name: true, item2.Name: true}
		if !names["cfg-old"] || !names["cfg-new"] {
			t.Fatalf("expected cfg-old and cfg-new in queue, got %v and %v", item1, item2)
		}
	})

	t.Run("Update enqueues old config when ref is removed (new ref nil)", func(t *testing.T) {
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()

		oldC := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: &corev1.LocalObjectReference{Name: "cfg-old"}},
		}
		newC := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: nil},
		}
		funcs.Update(ctx, event.UpdateEvent{ObjectOld: oldC, ObjectNew: newC}, q)

		if q.Len() != 1 {
			t.Fatalf("expected queue length 1 (old config only), got %d", q.Len())
		}
		item, _ := q.Get()
		if item.Namespace != "prod" || item.Name != "cfg-old" {
			t.Fatalf("unexpected request: %v", item)
		}
	})

	t.Run("Delete enqueues referenced config", func(t *testing.T) {
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()

		c := &kividbv1alpha1.KividbCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "prod"},
			Spec:       kividbv1alpha1.KividbClusterSpec{ConfigRef: &corev1.LocalObjectReference{Name: "cfg-1"}},
		}
		funcs.Delete(ctx, event.DeleteEvent{Object: c}, q)

		if q.Len() != 1 {
			t.Fatalf("expected queue length 1, got %d", q.Len())
		}
		item, _ := q.Get()
		if item.Namespace != "prod" || item.Name != "cfg-1" {
			t.Fatalf("unexpected request: %v", item)
		}
	})
}

func TestConfigUsageReconciler_NamespaceIsolation(t *testing.T) {
	t.Parallel()

	scheme := newUsageTestScheme(t)

	cfgA := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "my-config", Namespace: "ns-a"},
	}
	clusterA := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "ns-a"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "my-config"},
		},
	}

	cfgB := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "my-config", Namespace: "ns-b"},
	}
	clusterB := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-b", Namespace: "ns-b"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "my-config"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kividbv1alpha1.KividbConfig{}).
		WithIndex(&kividbv1alpha1.KividbCluster{}, ClusterConfigRefIndex, func(obj client.Object) []string {
			c, ok := obj.(*kividbv1alpha1.KividbCluster)
			if !ok || c.Spec.ConfigRef == nil || c.Spec.ConfigRef.Name == "" {
				return nil
			}
			return []string{c.Spec.ConfigRef.Name}
		}).
		WithObjects(cfgA, clusterA, cfgB, clusterB).
		Build()

	r := &ConfigUsageReconciler[*kividbv1alpha1.KividbConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbConfig { return &kividbv1alpha1.KividbConfig{} },
		IndexField: ClusterConfigRefIndex,
		GetUsedBy:  func(c *kividbv1alpha1.KividbConfig) []string { return c.Status.UsedBy },
		SetUsedBy:  func(c *kividbv1alpha1.KividbConfig, u []string) { c.Status.UsedBy = u },
	}

	ctx := context.Background()

	// Reconcile my-config in ns-a: must only count cluster-a
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns-a", Name: "my-config"}}); err != nil {
		t.Fatalf("Reconcile ns-a: %v", err)
	}

	var updatedA kividbv1alpha1.KividbConfig
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "ns-a", Name: "my-config"}, &updatedA); err != nil {
		t.Fatalf("Get ns-a config: %v", err)
	}
	if len(updatedA.Status.UsedBy) != 1 || updatedA.Status.UsedBy[0] != "cluster-a" {
		t.Fatalf("expected ns-a usedBy to be [cluster-a], got %v", updatedA.Status.UsedBy)
	}

	// Reconcile my-config in ns-b: must only count cluster-b
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns-b", Name: "my-config"}}); err != nil {
		t.Fatalf("Reconcile ns-b: %v", err)
	}

	var updatedB kividbv1alpha1.KividbConfig
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "ns-b", Name: "my-config"}, &updatedB); err != nil {
		t.Fatalf("Get ns-b config: %v", err)
	}
	if len(updatedB.Status.UsedBy) != 1 || updatedB.Status.UsedBy[0] != "cluster-b" {
		t.Fatalf("expected ns-b usedBy to be [cluster-b], got %v", updatedB.Status.UsedBy)
	}
}
