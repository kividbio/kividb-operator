package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

// The delete guard against a real API server: a config in use stays
// Terminating with DeletionBlocked and an Event; it goes once its last
// cluster does; and tearing down a namespace's clusters and configs together
// (what deleting the namespace does) leaves nothing stuck, even while a
// cluster is itself still Terminating. envtest runs no namespace controller,
// so the namespace's objects are deleted directly, as it would.
func TestConfigDeleteGuard_Envtest(t *testing.T) {
	binaryDir := findEnvtestBinaryDir()
	if binaryDir == "" {
		t.Skip("skipping envtest: control plane binaries not found (set KUBEBUILDER_ASSETS)")
	}
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: binaryDir,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Skipf("skipping envtest: failed to start control plane: %v", err)
	}
	defer func() { _ = testEnv.Stop() }()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	mgrCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The usage controllers' names are taken by the other envtests of the
	// package, in the same process.
	skipNameValidation := true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SetupConfigIndexes(mgrCtx, mgr.GetFieldIndexer()); err != nil {
		t.Fatal(err)
	}
	if err := SetupConfigUsageControllers(mgr); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mgr.Start(mgrCtx) }()

	ctx := context.Background()
	ns := "test-delete-guard"
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
	create := func(obj client.Object) {
		t.Helper()
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %s: %v", obj.GetName(), err)
		}
	}
	meta0 := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: ns} }
	eventually := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	gone := func(obj client.Object, name string) func() bool {
		return func() bool { return apierrors.IsNotFound(k8s.Get(ctx, key(name), obj)) }
	}
	held := func(obj client.Object, name string) func() bool {
		return func() bool {
			return k8s.Get(ctx, key(name), obj) == nil && controllerutil.ContainsFinalizer(obj, ConfigInUseFinalizer)
		}
	}

	create(&kividbv1alpha1.KividbConfig{ObjectMeta: meta0("cfg")})
	create(&kividbv1alpha1.KividbAclConfig{ObjectMeta: meta0("acl")})
	create(&kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: meta0("snap")})
	create(&kividbv1alpha1.KividbCluster{ObjectMeta: meta0("c1"), Spec: kividbv1alpha1.KividbClusterSpec{
		ConfigRef:         &corev1.LocalObjectReference{Name: "cfg"},
		AclConfigRef:      &corev1.LocalObjectReference{Name: "acl"},
		SnapshotConfigRef: &corev1.LocalObjectReference{Name: "snap"},
		Storage:           kividbv1alpha1.StorageSpec{Size: "1Gi"},
	}})
	// c2 lingers in Terminating once deleted, as a cluster still tearing
	// down would.
	c2 := &kividbv1alpha1.KividbCluster{ObjectMeta: meta0("c2"), Spec: kividbv1alpha1.KividbClusterSpec{
		ConfigRef: &corev1.LocalObjectReference{Name: "cfg"},
		Storage:   kividbv1alpha1.StorageSpec{Size: "1Gi"},
	}}
	c2.Finalizers = []string{"example.com/tearing-down"}
	create(c2)

	var kc kividbv1alpha1.KividbConfig
	var ka kividbv1alpha1.KividbAclConfig
	var ks kividbv1alpha1.KividbSnapshotConfig
	eventually("the KividbConfig's finalizer", held(&kc, "cfg"))
	eventually("the KividbAclConfig's finalizer", held(&ka, "acl"))
	eventually("the KividbSnapshotConfig's finalizer", held(&ks, "snap"))

	// Deleting a config in use: Terminating, DeletionBlocked, an Event.
	if err := k8s.Delete(ctx, &kividbv1alpha1.KividbConfig{ObjectMeta: meta0("cfg")}); err != nil {
		t.Fatal(err)
	}
	eventually("DeletionBlocked naming c1 and c2", func() bool {
		if k8s.Get(ctx, key("cfg"), &kc) != nil || kc.DeletionTimestamp == nil {
			return false
		}
		c := meta.FindStatusCondition(kc.Status.Conditions, ConditionDeletionBlocked)
		return c != nil && c.Status == metav1.ConditionTrue && strings.Contains(c.Message, "c1, c2")
	})
	eventually("the Warning Event", func() bool {
		var events corev1.EventList
		if k8s.List(ctx, &events, client.InNamespace(ns)) != nil {
			return false
		}
		for _, e := range events.Items {
			if e.InvolvedObject.Name == "cfg" && e.Reason == ReasonInUse && e.Type == corev1.EventTypeWarning {
				return true
			}
		}
		return false
	})

	// Tear the namespace's objects down together, the configs first.
	for _, obj := range []client.Object{
		&kividbv1alpha1.KividbAclConfig{ObjectMeta: meta0("acl")},
		&kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: meta0("snap")},
		&kividbv1alpha1.KividbCluster{ObjectMeta: meta0("c1")},
		&kividbv1alpha1.KividbCluster{ObjectMeta: meta0("c2")},
	} {
		if err := k8s.Delete(ctx, obj); err != nil {
			t.Fatalf("delete %s: %v", obj.GetName(), err)
		}
	}
	eventually("the KividbConfig to go", gone(&kc, "cfg"))
	eventually("the KividbAclConfig to go", gone(&ka, "acl"))
	eventually("the KividbSnapshotConfig to go", gone(&ks, "snap"))
	// c2 was still Terminating all along.
	if err := k8s.Get(ctx, key("c2"), c2); err != nil || c2.DeletionTimestamp == nil {
		t.Fatalf("c2 should still be Terminating: %v", err)
	}
	controllerutil.RemoveFinalizer(c2, "example.com/tearing-down")
	if err := k8s.Update(ctx, c2); err != nil {
		t.Fatal(err)
	}
}
