package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// guardTestReconciler is a KividbConfig usage reconciler over a fake client
// holding objs, with a fake event recorder.
func guardTestReconciler(t *testing.T, objs ...client.Object) (*ConfigUsageReconciler[*kividbv1alpha1.KividbConfig], client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := newUsageTestScheme(t)
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
		WithObjects(objs...).
		Build()
	recorder := record.NewFakeRecorder(10)
	r := &ConfigUsageReconciler[*kividbv1alpha1.KividbConfig]{
		Client:     cl,
		Scheme:     scheme,
		NewObject:  func() *kividbv1alpha1.KividbConfig { return &kividbv1alpha1.KividbConfig{} },
		IndexField: ClusterConfigRefIndex,
		GetUsedBy:  func(c *kividbv1alpha1.KividbConfig) []string { return c.Status.UsedBy },
		SetUsedBy:  func(c *kividbv1alpha1.KividbConfig, usedBy []string) { c.Status.UsedBy = usedBy },
		ConditionsOf: func(c *kividbv1alpha1.KividbConfig) *[]metav1.Condition {
			return &c.Status.Conditions
		},
		Recorder: recorder,
	}
	return r, cl, recorder
}

func clusterUsing(name, config string) *kividbv1alpha1.KividbCluster {
	return &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: config},
		},
	}
}

func reconcileConfig(t *testing.T, r *ConfigUsageReconciler[*kividbv1alpha1.KividbConfig], name string) {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile %s: %v", name, err)
	}
}

func getConfig(t *testing.T, cl client.Client, name string) (*kividbv1alpha1.KividbConfig, bool) {
	t.Helper()
	var cfg kividbv1alpha1.KividbConfig
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &cfg)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}
	return &cfg, true
}

// The finalizer is held exactly while some cluster references the config.
func TestConfigDeleteGuard_FinalizerFollowsUsedBy(t *testing.T) {
	t.Parallel()
	cfg := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"}}
	r, cl, _ := guardTestReconciler(t, cfg)
	ctx := context.Background()

	reconcileConfig(t, r, "cfg")
	got, _ := getConfig(t, cl, "cfg")
	if controllerutil.ContainsFinalizer(got, ConfigInUseFinalizer) {
		t.Fatalf("unused config got the finalizer: %v", got.Finalizers)
	}

	user := clusterUsing("c1", "cfg")
	if err := cl.Create(ctx, user); err != nil {
		t.Fatalf("Create c1: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	got, _ = getConfig(t, cl, "cfg")
	if !controllerutil.ContainsFinalizer(got, ConfigInUseFinalizer) {
		t.Fatalf("used config has no finalizer: %v", got.Finalizers)
	}
	if len(got.Status.UsedBy) != 1 || got.Status.UsedBy[0] != "c1" {
		t.Fatalf("usedBy = %v, want [c1]", got.Status.UsedBy)
	}

	user.Spec.ConfigRef = nil
	if err := cl.Update(ctx, user); err != nil {
		t.Fatalf("Update c1: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	got, _ = getConfig(t, cl, "cfg")
	if controllerutil.ContainsFinalizer(got, ConfigInUseFinalizer) {
		t.Fatalf("finalizer kept once nothing uses the config: %v", got.Finalizers)
	}
	if len(got.Status.UsedBy) != 0 {
		t.Fatalf("usedBy = %v, want empty", got.Status.UsedBy)
	}
}

// Deleting a config in use leaves it Terminating with DeletionBlocked and
// one Warning Event naming the clusters; it goes once the last one does.
func TestConfigDeleteGuard_DeleteInUseWaitsForClusters(t *testing.T) {
	t.Parallel()
	cfg := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"}}
	c1, c2 := clusterUsing("c1", "cfg"), clusterUsing("c2", "cfg")
	r, cl, recorder := guardTestReconciler(t, cfg, c1, c2)
	ctx := context.Background()

	reconcileConfig(t, r, "cfg")
	got, _ := getConfig(t, cl, "cfg")
	if err := cl.Delete(ctx, got); err != nil {
		t.Fatalf("Delete cfg: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	got, ok := getConfig(t, cl, "cfg")
	if !ok || got.DeletionTimestamp == nil {
		t.Fatalf("config in use was removed (present=%v)", ok)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionDeletionBlocked)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonInUse ||
		!strings.Contains(cond.Message, "c1, c2") {
		t.Fatalf("DeletionBlocked = %+v", cond)
	}
	select {
	case e := <-recorder.Events:
		if !strings.HasPrefix(e, "Warning InUse") || !strings.Contains(e, "c1, c2") {
			t.Fatalf("event = %q", e)
		}
	default:
		t.Fatal("no Warning Event for the blocked deletion")
	}

	// Nothing changed: no second Event.
	reconcileConfig(t, r, "cfg")
	select {
	case e := <-recorder.Events:
		t.Fatalf("repeated event %q", e)
	default:
	}

	// One cluster goes: the message (and an Event) name the one left.
	if err := cl.Delete(ctx, c1); err != nil {
		t.Fatalf("Delete c1: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	got, _ = getConfig(t, cl, "cfg")
	cond = meta.FindStatusCondition(got.Status.Conditions, ConditionDeletionBlocked)
	if cond == nil || !strings.HasSuffix(cond.Message, ": c2") {
		t.Fatalf("DeletionBlocked after c1 went = %+v", cond)
	}
	if e := <-recorder.Events; !strings.Contains(e, ": c2") {
		t.Fatalf("event after c1 went = %q", e)
	}

	// The last one goes: the finalizer comes off and the config with it.
	if err := cl.Delete(ctx, c2); err != nil {
		t.Fatalf("Delete c2: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	if _, ok := getConfig(t, cl, "cfg"); ok {
		t.Fatal("config still there once nothing references it")
	}
}

// A cluster being deleted no longer holds the config, even while it is
// itself held by a finalizer: deleting a namespace takes its clusters and
// configs together, and must not hang on the config.
func TestConfigDeleteGuard_ClusterBeingDeletedDoesNotBlock(t *testing.T) {
	t.Parallel()
	cfg := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"}}
	user := clusterUsing("c1", "cfg")
	user.Finalizers = []string{"example.com/still-tearing-down"}
	r, cl, _ := guardTestReconciler(t, cfg, user)
	ctx := context.Background()

	reconcileConfig(t, r, "cfg")
	got, _ := getConfig(t, cl, "cfg")
	if err := cl.Delete(ctx, got); err != nil {
		t.Fatalf("Delete cfg: %v", err)
	}
	if err := cl.Delete(ctx, user); err != nil {
		t.Fatalf("Delete c1: %v", err)
	}
	reconcileConfig(t, r, "cfg")
	if _, ok := getConfig(t, cl, "cfg"); ok {
		t.Fatal("config held by a cluster that is being deleted")
	}
}
