package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// TestWatchNamespaceIgnoresOtherNamespaces runs the KividbCluster
// controller against a real API server (envtest) with the cache limited to
// team-a, and checks that a KividbCluster in team-b gets nothing. It needs
// the envtest binaries: KUBEBUILDER_ASSETS="$(setup-envtest use -p path)".
// Not parallel: it starts its own API server.
func TestWatchNamespaceIgnoresOtherNamespaces(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; skipping the envtest test")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stopping envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  WatchNamespacesCacheOptions([]string{"team-a"}),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &KividbClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Agent:    NewAgentClient(time.Second, "test-token"),
		Recorder: record.NewFakeRecorder(1000),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	t.Cleanup(func() { cancel(); <-stopped })
	go func() {
		defer close(stopped)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	}()

	// team-b first: if its cluster were watched, it would be reconciled
	// no later than team-a's.
	for _, ns := range []string{"team-b", "team-a"} {
		if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
		kc := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: ns}}
		kc.Spec.Storage.Size = "1Gi"
		if err := c.Create(ctx, kc); err != nil {
			t.Fatalf("creating the KividbCluster in %s: %v", ns, err)
		}
	}

	statefulSets := func(ns string) int {
		var l appsv1.StatefulSetList
		if err := c.List(ctx, &l, client.InNamespace(ns)); err != nil {
			t.Fatal(err)
		}
		return len(l.Items)
	}
	deadline := time.Now().Add(30 * time.Second)
	for statefulSets("team-a") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the KividbCluster in the watched namespace team-a got no StatefulSet in 30s")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// team-b's cluster was created first, so had it been watched it would
	// have been reconciled by now. Keep checking for 2s in case it lags.
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if n := statefulSets("team-b"); n != 0 {
			t.Fatalf("team-b is not watched, but its KividbCluster got %d StatefulSet(s)", n)
		}
	}

	var kc kividbv1alpha1.KividbCluster
	if err := c.Get(ctx, client.ObjectKey{Namespace: "team-b", Name: "kv"}, &kc); err != nil {
		t.Fatal(err)
	}
	if len(kc.Status.Conditions) != 0 || kc.Status.Phase != "" {
		t.Fatalf("team-b is not watched, but its KividbCluster has status %+v", kc.Status)
	}
}
