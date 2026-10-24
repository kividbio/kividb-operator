package main

import (
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func int32p(n int32) *int32 { return &n }

func TestBuildClusterDefaults(t *testing.T) {
	c, err := buildCluster(createRequest{Namespace: "default", Name: "cache"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec.Replicas != 2 {
		t.Errorf("replicas %d, want the CRD's default 2", c.Spec.Replicas)
	}
	if c.Spec.Variant != kividbv1alpha1.VariantStandard || c.Spec.Image != defaultKividbImage {
		t.Errorf("variant %q image %q, want standard and %s", c.Spec.Variant, c.Spec.Image, defaultKividbImage)
	}
	if c.Spec.Storage.Size != "1Gi" {
		t.Errorf("storage %q, want 1Gi", c.Spec.Storage.Size)
	}
	if c.Spec.Resources.Requests != nil || c.Spec.SnapshotConfigRef != nil || c.Spec.Services.Master.Type != "" {
		t.Errorf("unexpected optional fields set: %+v", c.Spec)
	}
}

// The operator never derives an image from spec.variant, so the form must
// set the image that matches it.
func TestBuildClusterVariantPicksItsImage(t *testing.T) {
	for _, v := range []string{"lua", "tls", "full"} {
		c, err := buildCluster(createRequest{Namespace: "ns", Name: "q", Variant: v})
		if err != nil {
			t.Fatal(err)
		}
		if want := defaultKividbImage + "-" + v; c.Spec.Image != want {
			t.Errorf("variant %s: image %q, want %q", v, c.Spec.Image, want)
		}
	}
	c, err := buildCluster(createRequest{Namespace: "ns", Name: "q", Variant: "lua", Image: "registry.example/kividb:v9-lua"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec.Image != "registry.example/kividb:v9-lua" {
		t.Errorf("an explicit image is kept, got %q", c.Spec.Image)
	}
}

func TestBuildClusterOptions(t *testing.T) {
	c, err := buildCluster(createRequest{
		Namespace: "apps", Name: "sessions", Replicas: int32p(0), StorageSize: "20Gi", StorageClassName: "fast",
		Memory: "2Gi", CPU: "500m", SnapshotConfig: "s3", MasterServiceType: "LoadBalancer", Monitoring: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec.Replicas != 0 {
		t.Errorf("replicas %d, want 0", c.Spec.Replicas)
	}
	if *c.Spec.Storage.StorageClassName != "fast" || c.Spec.Storage.Size != "20Gi" {
		t.Errorf("storage %+v", c.Spec.Storage)
	}
	if got := c.Spec.Resources.Limits[corev1.ResourceMemory]; got.String() != "2Gi" {
		t.Errorf("memory limit %s", got.String())
	}
	if got := c.Spec.Resources.Requests[corev1.ResourceCPU]; got.String() != "500m" {
		t.Errorf("cpu request %s", got.String())
	}
	if c.Spec.SnapshotConfigRef.Name != "s3" || c.Spec.Services.Master.Type != corev1.ServiceTypeLoadBalancer || !c.Spec.Monitoring.Enabled {
		t.Errorf("snapshot/service/monitoring not set: %+v", c.Spec)
	}
}

func TestBuildClusterRefuses(t *testing.T) {
	cases := map[string]createRequest{
		"capital name":      {Namespace: "default", Name: "Cache"},
		"empty name":        {Namespace: "default", Name: ""},
		"long name":         {Namespace: "default", Name: strings.Repeat("a", maxClusterNameLen+1)},
		"bad namespace":     {Namespace: "my_ns", Name: "cache"},
		"too many replicas": {Namespace: "default", Name: "cache", Replicas: int32p(maxCreateReplicas + 1)},
		"negative replicas": {Namespace: "default", Name: "cache", Replicas: int32p(-1)},
		"unknown variant":   {Namespace: "default", Name: "cache", Variant: "turbo"},
		"bad storage":       {Namespace: "default", Name: "cache", StorageSize: "lots"},
		"bad memory":        {Namespace: "default", Name: "cache", Memory: "2 GB"},
		"bad cpu":           {Namespace: "default", Name: "cache", CPU: "fast"},
		"bad service type":  {Namespace: "default", Name: "cache", MasterServiceType: "ExternalName"},
	}
	for name, req := range cases {
		if _, err := buildCluster(req); err == nil {
			t.Errorf("%s: accepted %+v", name, req)
		}
	}
}

func TestClusterYAMLLeavesOutEmptySections(t *testing.T) {
	c, err := buildCluster(createRequest{Namespace: "default", Name: "cache"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := clusterYAML(c)
	if err != nil {
		t.Fatal(err)
	}
	y := string(b)
	for _, want := range []string{"kind: KividbCluster", "apiVersion: kividb.io/v1alpha1", "name: cache", "namespace: default", "replicas: 2", "size: 1Gi"} {
		if !strings.Contains(y, want) {
			t.Errorf("missing %q in\n%s", want, y)
		}
	}
	for _, unwanted := range []string{"resources:", "services:", "monitoring:", "failover:", "{}", "null"} {
		if strings.Contains(y, unwanted) {
			t.Errorf("empty section %q left in\n%s", unwanted, y)
		}
	}
}

func TestImageTag(t *testing.T) {
	cases := map[string]string{
		"docker.io/kividb/kividb:v1.1.2":            "v1.1.2",
		"docker.io/kividb/kividb:v1.1.2-lua":        "v1.1.2-lua",
		"localhost:5000/kividb":                     "",
		"localhost:5000/kividb:dev":                 "dev",
		"kividb/kividb:v1.1.2@sha256:0123456789abc": "v1.1.2",
		"kividb/kividb":                             "",
	}
	for image, want := range cases {
		if got := imageTag(image); got != want {
			t.Errorf("imageTag(%q) = %q, want %q", image, got, want)
		}
	}
}

func TestClusterStats(t *testing.T) {
	ms := newMetricsStore("")
	now := time.Now()
	ms0 := now.Add(-2 * time.Minute).UnixMilli()
	// Two pods, each scraped three times 15 s apart, the last just now.
	for i, dt := range []time.Duration{-30 * time.Second, -15 * time.Second, 0} {
		ts := now.Add(dt).UnixMilli()
		ms.record("ns", "c", "c-0", ts, 100, 4, 0, float64(1000+i*1500), true, true, true, true) // 100 cmds/s
		ms.record("ns", "c", "c-1", ts, 50, 1, 0, float64(10+i*150), true, true, true, true)     // 10 cmds/s
	}
	ms.record("ns", "other", "other-0", ms0, 999, 9, 0, 0, true, true, true, true)

	s := ms.clusterStats("ns", "c", now)
	if s == nil {
		t.Fatal("no stats")
	}
	if s.MemoryBytes != 150 || s.Clients != 5 {
		t.Errorf("memory %v clients %v, want 150 and 5", s.MemoryBytes, s.Clients)
	}
	if s.OpsPerSec < 109.9 || s.OpsPerSec > 110.1 {
		t.Errorf("ops %v, want 110", s.OpsPerSec)
	}
	if len(s.Memory) == 0 || len(s.Ops) == 0 {
		t.Errorf("no sparkline points: %+v", s)
	}
	if ms.clusterStats("ns", "missing", now) != nil {
		t.Error("stats for a cluster never scraped")
	}
}

// A counter that goes down (the pod restarted) is no rate, not a
// negative one.
func TestClusterStatsCounterReset(t *testing.T) {
	ms := newMetricsStore("")
	now := time.Now()
	ms.record("ns", "c", "c-0", now.Add(-15*time.Second).UnixMilli(), 1, 0, 0, 5000, true, true, true, true)
	ms.record("ns", "c", "c-0", now.UnixMilli(), 1, 0, 0, 20, true, true, true, true)
	s := ms.clusterStats("ns", "c", now)
	if s.OpsPerSec != 0 {
		t.Errorf("ops %v after a reset, want 0", s.OpsPerSec)
	}
}
