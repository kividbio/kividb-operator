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

// The snapshot config of a bootstrap, against a real API server: a cluster
// made before the snapshot it restores holds the snapshot's config once the
// snapshot is made (the KividbSnapshot watch), though it does not reference
// the config; a delete of the config meanwhile waits with DeletionBlocked
// naming it; and the config goes once the bootstrap is recorded as done.
func TestBootstrapHold_Envtest(t *testing.T) {
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
	ns := "test-bootstrap-hold"
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
	meta0 := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: ns} }
	create := func(obj client.Object) {
		t.Helper()
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %s: %v", obj.GetName(), err)
		}
	}
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

	create(&kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: meta0("nightly")})
	b := &kividbv1alpha1.KividbCluster{ObjectMeta: meta0("b"), Spec: kividbv1alpha1.KividbClusterSpec{
		BootstrapFromSnapshot: &kividbv1alpha1.BootstrapFromSnapshotSpec{
			SnapshotRef: corev1.LocalObjectReference{Name: "a-1"},
		},
		Storage: kividbv1alpha1.StorageSpec{Size: "1Gi"},
	}}
	create(b)

	// No snapshot yet: nothing holds the config.
	var ks kividbv1alpha1.KividbSnapshotConfig
	time.Sleep(time.Second)
	if err := k8s.Get(ctx, key("nightly"), &ks); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&ks, ConfigInUseFinalizer) {
		t.Fatalf("held before the snapshot exists: %v", ks.Finalizers)
	}

	create(&kividbv1alpha1.KividbSnapshot{ObjectMeta: meta0("a-1"), Spec: kividbv1alpha1.KividbSnapshotSpec{
		ClusterRef:        corev1.LocalObjectReference{Name: "a"},
		SnapshotConfigRef: corev1.LocalObjectReference{Name: "nightly"},
	}})
	eventually("the finalizer and usedBy [b]", func() bool {
		return k8s.Get(ctx, key("nightly"), &ks) == nil &&
			controllerutil.ContainsFinalizer(&ks, ConfigInUseFinalizer) &&
			len(ks.Status.UsedBy) == 1 && ks.Status.UsedBy[0] == "b"
	})

	if err := k8s.Delete(ctx, &kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: meta0("nightly")}); err != nil {
		t.Fatal(err)
	}
	eventually("DeletionBlocked naming b", func() bool {
		if k8s.Get(ctx, key("nightly"), &ks) != nil || ks.DeletionTimestamp == nil {
			return false
		}
		c := meta.FindStatusCondition(ks.Status.Conditions, ConditionDeletionBlocked)
		return c != nil && c.Status == metav1.ConditionTrue && strings.HasSuffix(c.Message, ": b")
	})
	// Still there a while later: the bootstrap is not done.
	time.Sleep(time.Second)
	if err := k8s.Get(ctx, key("nightly"), &ks); err != nil {
		t.Fatalf("config of a bootstrap under way went: %v", err)
	}

	// The restore is recorded on the cluster, as reconcileBootstrap does.
	if err := k8s.Get(ctx, key("b"), b); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	b.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{
		Phase: kividbv1alpha1.BootstrapCompleted, Completed: true, SnapshotName: "a-1", CompletionTime: &now,
	}
	if err := k8s.Status().Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	eventually("the config to go", func() bool { return apierrors.IsNotFound(k8s.Get(ctx, key("nightly"), &ks)) })
}
