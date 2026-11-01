package controller

import (
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
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
		{"configSecretName", configSecretName(c), "mycluster-config"},
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

func TestSnapshotSourceRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source kividbv1alpha1.SnapshotSource
		want   agentapi.Role
	}{
		{"master source", kividbv1alpha1.SnapshotSourceMaster, agentapi.RoleMaster},
		{"replica source", kividbv1alpha1.SnapshotSourceReplica, agentapi.RoleReplica},
		{"empty defaults to master", "", agentapi.RoleMaster},
		{"unrecognized defaults to master", kividbv1alpha1.SnapshotSource("other"), agentapi.RoleMaster},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := snapshotSourceRole(tt.source); got != tt.want {
				t.Fatalf("snapshotSourceRole(%q) = %q, want %q", tt.source, got, tt.want)
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
	if !strings.HasPrefix(DefaultAgentImage, "docker.io/kividb/") {
		t.Fatalf("DefaultAgentImage unexpected: %q", DefaultAgentImage)
	}
	if !strings.HasPrefix(DefaultKividbImage, "docker.io/kividb/") {
		t.Fatalf("DefaultKividbImage unexpected: %q", DefaultKividbImage)
	}
	if !strings.HasSuffix(DefaultKividbImage, ":v1.1.3") {
		t.Fatalf("DefaultKividbImage must pin v1.1.3, got %q", DefaultKividbImage)
	}
}

// The chart passes agent.image to the manager as --default-agent-image; a
// cluster without spec.agentImage must get that, not the built-in default.
func TestDefaultAgentImageCanBeOverridden(t *testing.T) {
	defer SetDefaultAgentImage("")

	c := &kividbv1alpha1.KividbCluster{}
	if got := agentImage(c); got != DefaultAgentImage {
		t.Fatalf("without an override: got %q, want %q", got, DefaultAgentImage)
	}

	SetDefaultAgentImage("registry.example/mirror/kividb-operator-agent:0.5.0")
	if got := agentImage(c); got != "registry.example/mirror/kividb-operator-agent:0.5.0" {
		t.Fatalf("with an override: got %q", got)
	}

	c.Spec.AgentImage = "registry.example/own/agent:1"
	if got := agentImage(c); got != "registry.example/own/agent:1" {
		t.Fatalf("spec.agentImage must still win: got %q", got)
	}

	SetDefaultAgentImage("")
	c.Spec.AgentImage = ""
	if got := agentImage(c); got != DefaultAgentImage {
		t.Fatalf("an empty override restores the default: got %q", got)
	}
}
