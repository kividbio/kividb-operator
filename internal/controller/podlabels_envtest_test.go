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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// TestPodLabelsEnvtest runs the KividbCluster controller against a real
// API server, which refuses a StatefulSet whose pod template does not
// match its selector (#122). A podLabels key that would override one of
// the operator's labels no longer keeps a new cluster's StatefulSet, or a
// shard's, from being made, nor a running cluster's from being updated.
// It needs KUBEBUILDER_ASSETS="$(setup-envtest use -p path)".
func TestPodLabelsEnvtest(t *testing.T) {
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
	skipNameValidation := true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := record.NewFakeRecorder(1000)
	r := &KividbClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Agent:    NewAgentClient(time.Second, "test-token"),
		Recorder: events,
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

	eventually := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); !cond(); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("after 30s: %s", what)
			}
		}
	}
	stsLabels := func(name string) map[string]string {
		var sts appsv1.StatefulSet
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, &sts); err != nil {
			return nil
		}
		return sts.Spec.Template.Labels
	}
	conflicting := map[string]string{
		"team":                      "a",
		kividbv1alpha1.ClusterLabel: "other",
		kividbv1alpha1.RoleLabel:    string(kividbv1alpha1.RoleMaster),
	}
	wantLabels := func(what string, got map[string]string, cluster, team string) {
		t.Helper()
		if got[kividbv1alpha1.ClusterLabel] != cluster || got["team"] != team {
			t.Fatalf("%s: template labels %v, want %s=%s and team=%s", what, got, kividbv1alpha1.ClusterLabel, cluster, team)
		}
		if _, ok := got[kividbv1alpha1.RoleLabel]; ok {
			t.Fatalf("%s: template labels %v carry %s", what, got, kividbv1alpha1.RoleLabel)
		}
	}

	// A new replicated cluster: its StatefulSet is made, with the
	// operator's labels, and an Event names the keys left out.
	rep := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "rep", Namespace: "default"}}
	rep.Spec.Storage.Size = "1Gi"
	rep.Spec.Replicas = 1
	rep.Spec.PodLabels = conflicting
	if err := c.Create(ctx, rep); err != nil {
		t.Fatal(err)
	}
	eventually("the replicated cluster's StatefulSet is made", func() bool { return stsLabels("rep") != nil })
	wantLabels("replicated", stsLabels("rep"), "rep", "a")
	eventually("a PodLabelsIgnored Event naming kividb.io/cluster and kividb.io/role", func() bool {
		select {
		case ev := <-events.Events:
			return strings.Contains(ev, "PodLabelsIgnored") && strings.Contains(ev, "kividb.io/cluster, kividb.io/role")
		default:
			return false
		}
	})

	// A new sharded cluster: each shard's StatefulSet is made.
	shards := int32(3)
	shd := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "shd", Namespace: "default"}}
	shd.Spec.Storage.Size = "1Gi"
	shd.Spec.Shards = &shards
	shd.Spec.PodLabels = conflicting
	if err := c.Create(ctx, shd); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < shards; i++ {
		name := shardStatefulSetName(shd, i)
		eventually("shard StatefulSet "+name+" is made", func() bool { return stsLabels(name) != nil })
		wantLabels(name, stsLabels(name), "shd", "a")
	}

	// A running cluster that gets a selector key later: updates of its
	// StatefulSet still go through (here, the new team label), and its
	// selector labels stay what they were.
	run := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"}}
	run.Spec.Storage.Size = "1Gi"
	run.Spec.PodLabels = map[string]string{"team": "a"}
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	eventually("the running cluster's StatefulSet is made", func() bool { return stsLabels("run") != nil })
	// The controller writes the cluster too (its finalizer, its status), so
	// the update is made on the latest copy, and again on a conflict.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
			return err
		}
		run.Spec.PodLabels = map[string]string{"team": "b", "app.kubernetes.io/instance": "other"}
		return c.Update(ctx, run)
	}); err != nil {
		t.Fatal(err)
	}
	eventually("the StatefulSet's template takes team=b", func() bool { return stsLabels("run")["team"] == "b" })
	if got := stsLabels("run"); got["app.kubernetes.io/instance"] != "run" {
		t.Fatalf("template labels %v, want app.kubernetes.io/instance=run", got)
	}
}
