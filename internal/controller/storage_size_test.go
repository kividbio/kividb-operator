package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// A change of spec.storage.size on a running cluster, which the
// StatefulSet's volume claim template cannot follow: a larger size sets
// StorageResizePending with the manual steps and one Warning Event; a
// smaller one is refused as an invalid spec; set back, or once the
// StatefulSet is recreated with the new size (the last manual step), the
// cluster is as before.
func TestStorageSizeChangeOnARunningCluster(t *testing.T) {
	f := newFailoverCase(t, nil, nil)
	f.r.Recorder = record.NewFakeRecorder(100)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)}
	pass := func() *kividbv1alpha1.KividbCluster {
		t.Helper()
		_, _ = f.r.Reconcile(ctx, req)
		var got kividbv1alpha1.KividbCluster
		if err := f.cl.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return &got
	}
	resize := func(size string) {
		t.Helper()
		var c kividbv1alpha1.KividbCluster
		if err := f.cl.Get(ctx, req.NamespacedName, &c); err != nil {
			t.Fatal(err)
		}
		c.Spec.Storage.Size = size
		if err := f.cl.Update(ctx, &c); err != nil {
			t.Fatal(err)
		}
	}
	pending := func(c *kividbv1alpha1.KividbCluster) *metav1.Condition {
		return meta.FindStatusCondition(c.Status.Conditions, ConditionStorageResizePending)
	}

	// The fake client sets no creationTimestamp, so every pass would take
	// the StatefulSet for a new one and write its volume claim template,
	// which the API server refuses to change: stamp it, as that would.
	made := func() {
		t.Helper()
		var list appsv1.StatefulSetList
		if err := f.cl.List(ctx, &list, client.InNamespace("default")); err != nil {
			t.Fatal(err)
		}
		if len(list.Items) == 0 {
			t.Fatal("no StatefulSet made")
		}
		for i := range list.Items {
			if list.Items[i].CreationTimestamp.IsZero() {
				list.Items[i].CreationTimestamp = metav1.Now()
				if err := f.cl.Update(ctx, &list.Items[i]); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	got := pass() // makes the StatefulSet, with 1Gi
	made()
	if p := pending(got); p != nil {
		t.Fatalf("StorageResizePending on a new cluster: %+v", p)
	}
	_ = events(f.r)

	resize("2Gi")
	got = pass()
	p := pending(got)
	if p == nil || p.Status != metav1.ConditionTrue || p.Reason != ReasonStorageSizeIncreased ||
		!strings.Contains(p.Message, "made with 1Gi") || !strings.Contains(p.Message, "--cascade=orphan") {
		t.Fatalf("StorageResizePending = %+v", p)
	}
	wantWarnings(t, f.r, ReasonStorageSizeIncreased, "spec.storage.size is 2Gi")
	pass()
	wantWarnings(t, f.r, ReasonStorageSizeIncreased) // not again

	resize("512Mi")
	got = pass()
	if p := pending(got); p != nil {
		t.Errorf("StorageResizePending kept on a smaller size: %+v", p)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != "InvalidSpec" || !strings.Contains(ready.Message, "cannot shrink") {
		t.Fatalf("Ready on a smaller size = %+v, want InvalidSpec", ready)
	}
	wantWarnings(t, f.r, "InvalidSpec", "smaller than the 1Gi")

	resize("1Gi")
	got = pass()
	if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready != nil && ready.Reason == "InvalidSpec" {
		t.Fatalf("still refused once set back: %+v", ready)
	}
	if p := pending(got); p != nil {
		t.Fatalf("StorageResizePending once set back: %+v", p)
	}

	// Grown by hand: the PVCs patched, then the StatefulSet deleted with
	// --cascade=orphan; the operator makes it again with the new size.
	resize("2Gi")
	pass()
	var sts appsv1.StatefulSetList
	if err := f.cl.List(ctx, &sts, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	for i := range sts.Items {
		if err := f.cl.Delete(ctx, &sts.Items[i]); err != nil {
			t.Fatal(err)
		}
	}
	pass() // makes it again
	made()
	got = pass()
	if p := pending(got); p != nil {
		t.Fatalf("StorageResizePending once the StatefulSet asks for 2Gi: %+v", p)
	}
}

// Each of a sharded cluster's StatefulSets is looked at: the message names
// the ones still made with the old size, and a StatefulSet the cluster does
// not own is left out.
func TestCheckStorageSizeNamesTheStatefulSetsLeft(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default", UID: "u1"},
		Spec:       kividbv1alpha1.KividbClusterSpec{Storage: kividbv1alpha1.StorageSpec{Size: "2Gi"}},
	}
	stsOf := func(name, size string, owned bool) *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default",
				Labels: map[string]string{kividbv1alpha1.ClusterLabel: "c1"}},
			Spec: appsv1.StatefulSetSpec{VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
				}},
			}}},
		}
		if owned {
			if err := controllerutil.SetControllerReference(c, sts, scheme); err != nil {
				t.Fatal(err)
			}
		}
		return sts
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		stsOf("c1-0", "1Gi", true),
		stsOf("c1-1", "2Gi", true), // recreated already
		stsOf("c1-2", "1Gi", true),
		stsOf("c1-stray", "1Gi", false),
	).Build()
	r := &KividbClusterReconciler{Client: cl, Recorder: record.NewFakeRecorder(10)}

	shrunk, err := r.checkStorageSize(context.Background(), c)
	if shrunk != nil || err != nil {
		t.Fatalf("checkStorageSize = %v, %v", shrunk, err)
	}
	p := meta.FindStatusCondition(c.Status.Conditions, ConditionStorageResizePending)
	if p == nil || !strings.Contains(p.Message, "StatefulSet c1-0, c1-2 were made with 1Gi") || strings.Contains(p.Message, "stray") {
		t.Fatalf("StorageResizePending = %+v", p)
	}

	c.Spec.Storage.Size = "1536Mi"
	if shrunk, _ = r.checkStorageSize(context.Background(), c); shrunk == nil ||
		!strings.Contains(shrunk.Error(), "c1-1") {
		t.Fatalf("shrunk = %v, want c1-1 named", shrunk)
	}
}
