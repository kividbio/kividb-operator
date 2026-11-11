package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// TestServiceMonitorEnvtest runs the KividbCluster controller against a
// real API server (envtest): first without the ServiceMonitor CRD, then
// with it installed while the operator runs. It needs the envtest binaries:
// KUBEBUILDER_ASSETS="$(setup-envtest use -p path)". Not parallel: it
// starts its own API server.
func TestServiceMonitorEnvtest(t *testing.T) {
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

	kc := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: "default"}}
	kc.Spec.Storage.Size = "1Gi"
	kc.Spec.Monitoring.ServiceMonitor = true
	if err := c.Create(ctx, kc); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(kc)

	eventually := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); !cond(); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("after 30s: %s", what)
			}
		}
	}

	// No CRD: the rest of the cluster is made, and Ready says why there is
	// no ServiceMonitor.
	eventually("the StatefulSet is made without the ServiceMonitor CRD", func() bool {
		return c.Get(ctx, key, &appsv1.StatefulSet{}) == nil
	})
	eventually("the Ready condition names the missing CRD", func() bool {
		var got kividbv1alpha1.KividbCluster
		if err := c.Get(ctx, key, &got); err != nil {
			t.Fatal(err)
		}
		ready := findCondition(got.Status.Conditions, "Ready")
		return ready != nil && ready.Status == metav1.ConditionFalse &&
			strings.Contains(ready.Message, "servicemonitors.monitoring.coreos.com")
	})

	// The CRD installed while the operator runs: the ServiceMonitor follows.
	if _, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths: []string{filepath.Join("testdata", "servicemonitor-crd")},
	}); err != nil {
		t.Fatalf("installing the ServiceMonitor CRD: %v", err)
	}
	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	eventually("the ServiceMonitor is made once the CRD is installed", func() bool {
		return c.Get(ctx, key, sm) == nil
	})
	var got kividbv1alpha1.KividbCluster
	if err := c.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(sm, &got) {
		t.Fatalf("ServiceMonitor owners = %v, want the cluster as controller", sm.GetOwnerReferences())
	}
	eventually("the Ready condition no longer names the CRD", func() bool {
		if err := c.Get(ctx, key, &got); err != nil {
			t.Fatal(err)
		}
		ready := findCondition(got.Status.Conditions, "Ready")
		return ready != nil && !strings.Contains(ready.Message, "monitoring.coreos.com")
	})

	// Turned off: deleted.
	got.Spec.Monitoring.ServiceMonitor = false
	if err := c.Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	eventually("the ServiceMonitor is deleted once serviceMonitor is false", func() bool {
		return apierrors.IsNotFound(c.Get(ctx, key, sm))
	})
}
