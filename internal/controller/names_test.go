package controller

import (
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNamingHelpers(t *testing.T) {
	t.Parallel()

	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "ns"},
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"statefulSetName", statefulSetName(c), "mycluster"},
		{"headlessServiceName", headlessServiceName(c), "mycluster-headless"},
		{"masterServiceName", masterServiceName(c), "mycluster-master"},
		{"replicaServiceName", replicaServiceName(c), "mycluster-replicas"},
		{"configMapName", configMapName(c), "mycluster-config"},
		{"secretName", secretName(c), "mycluster-auth"},
		{"backupCronJobName", backupCronJobName(c), "mycluster-backup"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Fatalf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestSnapshotSourceServiceName(t *testing.T) {
	t.Parallel()

	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
	}

	tests := []struct {
		name   string
		source kividbv1alpha1.SnapshotSource
		want   string
	}{
		{"master source", kividbv1alpha1.SnapshotSourceMaster, "demo-master"},
		{"replica source", kividbv1alpha1.SnapshotSourceReplica, "demo-replicas"},
		{"empty defaults to master", "", "demo-master"},
		{"unrecognized defaults to master", kividbv1alpha1.SnapshotSource("other"), "demo-master"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := snapshotSourceServiceName(c, tt.source)
			if got != tt.want {
				t.Fatalf("snapshotSourceServiceName(%q) = %q, want %q", tt.source, got, tt.want)
			}
		})
	}
}

func TestDefaultImageConstants(t *testing.T) {
	t.Parallel()

	if DefaultAgentImage == "" {
		t.Fatal("DefaultAgentImage must be non-empty")
	}
	if DefaultKividbImage == "" {
		t.Fatal("DefaultKividbImage must be non-empty")
	}
	if !strings.HasPrefix(DefaultAgentImage, "quay.io/") {
		t.Fatalf("DefaultAgentImage unexpected: %q", DefaultAgentImage)
	}
	if !strings.HasPrefix(DefaultKividbImage, "quay.io/") {
		t.Fatalf("DefaultKividbImage unexpected: %q", DefaultKividbImage)
	}
	if !strings.HasSuffix(DefaultKividbImage, ":v1.0.3") {
		t.Fatalf("DefaultKividbImage must pin v1.0.3, got %q", DefaultKividbImage)
	}
}
