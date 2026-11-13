package controller

import (
	"context"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func TestRecordUsedBy(t *testing.T) {
	t.Parallel()

	scheme := newTestScheme(t)

	t.Run("appends cluster name to empty usedBy", func(t *testing.T) {
		t.Parallel()
		cfg := &kividbv1alpha1.KividbConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-cfg", Namespace: "default"},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kividbv1alpha1.KividbConfig{}).WithObjects(cfg).Build()
		r := &KividbClusterReconciler{Client: cl, Scheme: scheme}

		err := r.recordUsedBy(context.Background(), cfg, "cluster-1",
			func() []string { return cfg.Status.UsedBy },
			func(v []string) { cfg.Status.UsedBy = v },
		)
		if err != nil {
			t.Fatalf("recordUsedBy: %v", err)
		}

		var updated kividbv1alpha1.KividbConfig
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(updated.Status.UsedBy) != 1 || updated.Status.UsedBy[0] != "cluster-1" {
			t.Fatalf("expected usedBy [cluster-1], got: %v", updated.Status.UsedBy)
		}
	})

	t.Run("is idempotent when cluster name already exists", func(t *testing.T) {
		t.Parallel()
		cfg := &kividbv1alpha1.KividbConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-cfg", Namespace: "default"},
			Status: kividbv1alpha1.KividbConfigStatus{
				UsedBy: []string{"cluster-1"},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kividbv1alpha1.KividbConfig{}).WithObjects(cfg).Build()
		r := &KividbClusterReconciler{Client: cl, Scheme: scheme}

		err := r.recordUsedBy(context.Background(), cfg, "cluster-1",
			func() []string { return cfg.Status.UsedBy },
			func(v []string) { cfg.Status.UsedBy = v },
		)
		if err != nil {
			t.Fatalf("recordUsedBy: %v", err)
		}

		var updated kividbv1alpha1.KividbConfig
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(updated.Status.UsedBy) != 1 || updated.Status.UsedBy[0] != "cluster-1" {
			t.Fatalf("expected usedBy unchanged [cluster-1], got: %v", updated.Status.UsedBy)
		}
	})

	t.Run("appends second cluster without overwriting existing", func(t *testing.T) {
		t.Parallel()
		cfg := &kividbv1alpha1.KividbConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-cfg", Namespace: "default"},
			Status: kividbv1alpha1.KividbConfigStatus{
				UsedBy: []string{"cluster-1"},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kividbv1alpha1.KividbConfig{}).WithObjects(cfg).Build()
		r := &KividbClusterReconciler{Client: cl, Scheme: scheme}

		err := r.recordUsedBy(context.Background(), cfg, "cluster-2",
			func() []string { return cfg.Status.UsedBy },
			func(v []string) { cfg.Status.UsedBy = v },
		)
		if err != nil {
			t.Fatalf("recordUsedBy: %v", err)
		}

		var updated kividbv1alpha1.KividbConfig
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(updated.Status.UsedBy) != 2 || updated.Status.UsedBy[0] != "cluster-1" || updated.Status.UsedBy[1] != "cluster-2" {
			t.Fatalf("expected usedBy [cluster-1, cluster-2], got: %v", updated.Status.UsedBy)
		}
	})

	t.Run("works for KividbAclConfig and KividbSnapshotConfig", func(t *testing.T) {
		t.Parallel()
		acl := &kividbv1alpha1.KividbAclConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-acl", Namespace: "default"},
		}
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-snap", Namespace: "default"},
		}
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&kividbv1alpha1.KividbAclConfig{}, &kividbv1alpha1.KividbSnapshotConfig{}).
			WithObjects(acl, snap).
			Build()
		r := &KividbClusterReconciler{Client: cl, Scheme: scheme}

		if err := r.recordUsedBy(context.Background(), acl, "cluster-1",
			func() []string { return acl.Status.UsedBy },
			func(v []string) { acl.Status.UsedBy = v },
		); err != nil {
			t.Fatalf("recordUsedBy ACL: %v", err)
		}

		if err := r.recordUsedBy(context.Background(), snap, "cluster-1",
			func() []string { return snap.Status.UsedBy },
			func(v []string) { snap.Status.UsedBy = v },
		); err != nil {
			t.Fatalf("recordUsedBy SnapshotConfig: %v", err)
		}

		var updatedAcl kividbv1alpha1.KividbAclConfig
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(acl), &updatedAcl); err != nil {
			t.Fatalf("Get ACL: %v", err)
		}
		if len(updatedAcl.Status.UsedBy) != 1 || updatedAcl.Status.UsedBy[0] != "cluster-1" {
			t.Fatalf("expected acl usedBy [cluster-1], got: %v", updatedAcl.Status.UsedBy)
		}

		var updatedSnap kividbv1alpha1.KividbSnapshotConfig
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(snap), &updatedSnap); err != nil {
			t.Fatalf("Get SnapshotConfig: %v", err)
		}
		if len(updatedSnap.Status.UsedBy) != 1 || updatedSnap.Status.UsedBy[0] != "cluster-1" {
			t.Fatalf("expected snap usedBy [cluster-1], got: %v", updatedSnap.Status.UsedBy)
		}
	})
}
