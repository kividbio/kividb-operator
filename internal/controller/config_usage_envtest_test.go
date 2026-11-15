package controller

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

func findEnvtestBinaryDir() string {
	if dir := os.Getenv("KUBEBUILDER_ASSETS"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	base := filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) == 0 {
		return ""
	}
	// Return the latest directory entry if available
	return filepath.Join(base, entries[len(entries)-1].Name())
}

func TestConfigUsage_EnvtestLifecycle(t *testing.T) {
	binaryDir := findEnvtestBinaryDir()
	if binaryDir == "" {
		t.Skip("skipping envtest: control plane binaries not found (set KUBEBUILDER_ASSETS)")
	}

	crdPath := filepath.Join("..", "..", "config", "crd", "bases")
	if _, err := os.Stat(filepath.Join(crdPath, "kividb.io_kividbconfigs.yaml")); err != nil {
		t.Fatalf("CRD path %s not found: %v", crdPath, err)
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdPath},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: binaryDir,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Skipf("skipping envtest: failed to start control plane: %v", err)
	}
	defer func() {
		_ = testEnv.Stop()
	}()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add client-go scheme: %v", err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add kividb scheme: %v", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	mgrCtx, cancelMgr := context.WithCancel(context.Background())
	defer cancelMgr()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if err := SetupConfigIndexes(mgrCtx, mgr.GetFieldIndexer()); err != nil {
		t.Fatalf("failed to setup config indexes: %v", err)
	}
	if err := SetupConfigUsageControllers(mgr); err != nil {
		t.Fatalf("failed to setup config usage controllers: %v", err)
	}

	mgrStopped := make(chan struct{})
	go func() {
		defer close(mgrStopped)
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()

	ctx := context.Background()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-usedby-envtest",
		},
	}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create cfg-a and cfg-b
	cfgA := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cfg-a",
			Namespace: ns.Name,
		},
	}
	if err := k8sClient.Create(ctx, cfgA); err != nil {
		t.Fatalf("failed to create cfg-a: %v", err)
	}

	cfgB := &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cfg-b",
			Namespace: ns.Name,
		},
	}
	if err := k8sClient.Create(ctx, cfgB); err != nil {
		t.Fatalf("failed to create cfg-b: %v", err)
	}

	// 1. Two clusters sharing cfg-a
	cluster1 := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: ns.Name,
		},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "cfg-a"},
			Storage:   kividbv1alpha1.StorageSpec{Size: "1Gi"},
		},
	}
	if err := k8sClient.Create(ctx, cluster1); err != nil {
		t.Fatalf("failed to create cluster-1: %v", err)
	}

	cluster2 := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
			Namespace: ns.Name,
		},
		Spec: kividbv1alpha1.KividbClusterSpec{
			ConfigRef: &corev1.LocalObjectReference{Name: "cfg-a"},
			Storage:   kividbv1alpha1.StorageSpec{Size: "1Gi"},
		},
	}
	if err := k8sClient.Create(ctx, cluster2); err != nil {
		t.Fatalf("failed to create cluster-2: %v", err)
	}

	pollUsedBy := func(configName string, expected []string) bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var curr kividbv1alpha1.KividbConfig
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: configName}, &curr); err == nil {
				actual := make([]string, len(curr.Status.UsedBy))
				copy(actual, curr.Status.UsedBy)
				sort.Strings(actual)
				expSorted := make([]string, len(expected))
				copy(expSorted, expected)
				sort.Strings(expSorted)

				if len(actual) == len(expSorted) {
					match := true
					for i := range actual {
						if actual[i] != expSorted[i] {
							match = false
							break
						}
					}
					if match {
						return true
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	// Assert step 1: both cluster-1 and cluster-2 are in cfg-a.status.usedBy
	if !pollUsedBy("cfg-a", []string{"cluster-1", "cluster-2"}) {
		var curr kividbv1alpha1.KividbConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cfg-a"}, &curr)
		t.Fatalf("step 1 failed: expected cfg-a to be used by [cluster-1, cluster-2], got: %v", curr.Status.UsedBy)
	}
	if !pollUsedBy("cfg-b", nil) {
		var curr kividbv1alpha1.KividbConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cfg-b"}, &curr)
		t.Fatalf("step 1 failed: expected cfg-b to be unused, got: %v", curr.Status.UsedBy)
	}

	// Step 2: Delete one cluster (cluster-1)
	if err := k8sClient.Delete(ctx, cluster1); err != nil {
		t.Fatalf("failed to delete cluster-1: %v", err)
	}

	// Assert step 2: only cluster-2 remains in cfg-a.status.usedBy
	if !pollUsedBy("cfg-a", []string{"cluster-2"}) {
		var curr kividbv1alpha1.KividbConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cfg-a"}, &curr)
		t.Fatalf("step 2 failed: expected cfg-a to be used by [cluster-2], got: %v", curr.Status.UsedBy)
	}

	// Step 3: Switch cluster-2 to cfg-b
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cluster-2"}, cluster2); err != nil {
		t.Fatalf("failed to get cluster-2: %v", err)
	}
	cluster2.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "cfg-b"}
	if err := k8sClient.Update(ctx, cluster2); err != nil {
		t.Fatalf("failed to update cluster-2: %v", err)
	}

	// Assert step 3: cfg-a is empty, cfg-b has [cluster-2]
	if !pollUsedBy("cfg-a", nil) {
		var curr kividbv1alpha1.KividbConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cfg-a"}, &curr)
		t.Fatalf("step 3 failed: expected cfg-a to be empty, got: %v", curr.Status.UsedBy)
	}
	if !pollUsedBy("cfg-b", []string{"cluster-2"}) {
		var curr kividbv1alpha1.KividbConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cfg-b"}, &curr)
		t.Fatalf("step 3 failed: expected cfg-b to be used by [cluster-2], got: %v", curr.Status.UsedBy)
	}

	// Step 4: Verify AclConfig and SnapshotConfig tracking
	aclCfg := &kividbv1alpha1.KividbAclConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acl-a",
			Namespace: ns.Name,
		},
	}
	if err := k8sClient.Create(ctx, aclCfg); err != nil {
		t.Fatalf("failed to create acl-a: %v", err)
	}

	snapCfg := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-a",
			Namespace: ns.Name,
		},
	}
	if err := k8sClient.Create(ctx, snapCfg); err != nil {
		t.Fatalf("failed to create snap-a: %v", err)
	}

	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "cluster-2"}, cluster2); err != nil {
		t.Fatalf("failed to get cluster-2: %v", err)
	}
	cluster2.Spec.AclConfigRef = &corev1.LocalObjectReference{Name: "acl-a"}
	cluster2.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap-a"}
	if err := k8sClient.Update(ctx, cluster2); err != nil {
		t.Fatalf("failed to update cluster-2 with acl and snapshot refs: %v", err)
	}

	pollAclUsedBy := func(expected []string) bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var curr kividbv1alpha1.KividbAclConfig
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "acl-a"}, &curr); err == nil {
				if len(curr.Status.UsedBy) == len(expected) {
					match := true
					for i := range curr.Status.UsedBy {
						if curr.Status.UsedBy[i] != expected[i] {
							match = false
							break
						}
					}
					if match {
						return true
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	pollSnapshotUsedBy := func(expected []string) bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var curr kividbv1alpha1.KividbSnapshotConfig
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "snap-a"}, &curr); err == nil {
				if len(curr.Status.UsedBy) == len(expected) {
					match := true
					for i := range curr.Status.UsedBy {
						if curr.Status.UsedBy[i] != expected[i] {
							match = false
							break
						}
					}
					if match {
						return true
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	if !pollAclUsedBy([]string{"cluster-2"}) {
		var curr kividbv1alpha1.KividbAclConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "acl-a"}, &curr)
		t.Fatalf("expected acl-a to be used by [cluster-2], got: %v", curr.Status.UsedBy)
	}
	if !pollSnapshotUsedBy([]string{"cluster-2"}) {
		var curr kividbv1alpha1.KividbSnapshotConfig
		_ = k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "snap-a"}, &curr)
		t.Fatalf("expected snap-a to be used by [cluster-2], got: %v", curr.Status.UsedBy)
	}
}
