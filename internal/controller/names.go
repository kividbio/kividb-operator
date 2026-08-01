package controller

import (
	"fmt"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

// Well-known ports and paths shared between the controller, the generated
// pod specs, and the agent sidecar binary (cmd/agent). Keep this file as
// the single source of truth for anything the two binaries must agree on.
const (
	// AgentPort is the sidecar HTTP API port.
	AgentPort = 8081

	// AgentPortName is the container port name for AgentPort.
	AgentPortName = "agent"

	// KividbPortName is the container port name for the RESP port.
	KividbPortName = "kividb"

	// DataDir is the shared volume mount path for both containers. kividb
	// has no `dir` config directive; it writes dump.kdb/appendonly.aof
	// relative to its working directory, so the kividb container's
	// workingDir is set to DataDir and the PVC is mounted here.
	DataDir = "/data"

	// ConfigDir is where the rendered kividb.conf ConfigMap is mounted.
	ConfigDir = "/etc/kividb"

	// ConfigFileName is the rendered config file's name within ConfigDir.
	ConfigFileName = "kividb.conf"

	// AclDir is where the rendered ACL Secret is mounted.
	AclDir = "/etc/kividb/acl"

	// AclFileName is the rendered ACL file's name within AclDir.
	AclFileName = "users.acl"

	// TLSDir is where the TLS cert/key (and optional CA cert) Secret is
	// mounted, when a referenced KividbConfig has spec.tls.enabled: true.
	TLSDir = "/etc/kividb/tls"

	// DataVolumeFSGroup is applied as the pod-level securityContext.fsGroup
	// so the data PVC is writable by kividb's non-root container user (an
	// arbitrary UID picked by `useradd` in kividb's own image, not
	// something this operator can pin) and readable by the agent sidecar
	// (a different, distroless-assigned UID) -- see statefulset.go.
	DataVolumeFSGroup = 1000

	// DefaultAgentImage is used when KividbClusterSpec.AgentImage is unset.
	// Bumped by hand alongside VERSION/Chart.yaml on every release (see the
	// pre-release checklist in docs/RELEASING.md) -- there is currently no
	// build-time (-ldflags) mechanism that does this automatically, despite
	// what an earlier version of this comment claimed.
	DefaultAgentImage = "quay.io/kividbio/kividb-operator-agent:0.3.0"

	// DefaultKividbImage is used when KividbClusterSpec.Image is unset.
	// Pinned to the kividb engine line this operator release was validated
	// against (see CHANGELOG / hack/e2e). Override with spec.image for a
	// different tag or variant (e.g. ...:v1.0.3-tls). Deliberately not
	// derived from spec.variant: the operator never guesses an image tag
	// from spec.variant.
	DefaultKividbImage = "quay.io/kividbio/kividb:v1.0.3"

	// ExporterPort is the redis_exporter sidecar's standard listen port
	// (its own documented default -- not something this project invented).
	ExporterPort = 9121

	// ExporterPortName is the container/Service port name for ExporterPort.
	ExporterPortName = "metrics"

	// DefaultExporterImage is used when KividbClusterSpec.ExporterImage is
	// unset. oliver006/redis_exporter is the de facto standard Prometheus
	// exporter for Redis-protocol stores -- kividb speaks enough of the
	// INFO/CONFIG surface for its core metric set to work unmodified.
	DefaultExporterImage = "oliver006/redis_exporter:v1.66.0"

	// managedByValue is the standard app.kubernetes.io/managed-by value.
	managedByValue = "kividb-operator"
	appName        = "kividb"
)

func statefulSetName(c *kividbv1alpha1.KividbCluster) string     { return c.Name }
func headlessServiceName(c *kividbv1alpha1.KividbCluster) string { return c.Name + "-headless" }
func masterServiceName(c *kividbv1alpha1.KividbCluster) string   { return c.Name + "-master" }
func replicaServiceName(c *kividbv1alpha1.KividbCluster) string  { return c.Name + "-replicas" }
func configMapName(c *kividbv1alpha1.KividbCluster) string       { return c.Name + "-config" }
func secretName(c *kividbv1alpha1.KividbCluster) string          { return c.Name + "-auth" }
func backupCronJobName(c *kividbv1alpha1.KividbCluster) string   { return c.Name + "-backup" }

// selectorLabels returns the immutable identity labels used both as the
// StatefulSet/Service selector and as a base label set on every pod. Do not
// add mutable data here (e.g. role) -- see roleLabels.
func selectorLabels(c *kividbv1alpha1.KividbCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     appName,
		"app.kubernetes.io/instance": c.Name,
		kividbv1alpha1.ClusterLabel:  c.Name,
	}
}

func commonLabels(c *kividbv1alpha1.KividbCluster) map[string]string {
	l := selectorLabels(c)
	l["app.kubernetes.io/managed-by"] = managedByValue
	return l
}

// backupLabels labels the backup CronJob and the Jobs/Pods it spawns.
// Deliberately NOT commonLabels/selectorLabels: those are also the
// StatefulSet's pod selector, and the controller lists pods by that same
// selector to compute replication roles (see failover.go) -- reusing it
// here would make every backup Job's pod look like a cluster member (a
// real bug found live: it showed up in status.pods as a bogus
// role=unknown entry). reconcileBackupStatus (backup.go) lists Jobs by
// this same label set.
func backupLabels(c *kividbv1alpha1.KividbCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "kividb-backup",
		"app.kubernetes.io/instance":   c.Name,
		"app.kubernetes.io/managed-by": managedByValue,
		kividbv1alpha1.ClusterLabel:    c.Name,
	}
}

func agentAddr(podIP string) string {
	return fmt.Sprintf("%s:%d", podIP, AgentPort)
}
