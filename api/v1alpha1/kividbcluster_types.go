package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPhase describes the coarse-grained lifecycle state of a KividbCluster.
type ClusterPhase string

const (
	PhasePending      ClusterPhase = "Pending"
	PhaseProvisioning ClusterPhase = "Provisioning"
	PhaseRunning      ClusterPhase = "Running"
	PhaseDegraded     ClusterPhase = "Degraded"
	PhaseFailingOver  ClusterPhase = "FailingOver"
	PhaseError        ClusterPhase = "Error"
)

// NodeRole is the runtime replication role of a single kividb pod, as tracked
// by the operator. It is independent of the pod's StatefulSet ordinal.
type NodeRole string

const (
	RoleMaster  NodeRole = "master"
	RoleReplica NodeRole = "replica"
	RoleUnknown NodeRole = "unknown"
)

// RoleLabel is the pod label the operator maintains to reflect NodeRole.
// Both the master and replica Services select on this label, so a failover
// is completed purely by relabeling pods -- no Service spec changes needed.
const RoleLabel = "kividb.io/role"

// ClusterLabel identifies which KividbCluster a pod/service/etc. belongs to.
const ClusterLabel = "kividb.io/cluster"

// KividbVariant selects which build of the kividb image to run. Each
// variant beyond "standard" corresponds to a real, separately-published
// image tag suffix (e.g. quay.io/kividbio/kividb:v1.0.2-tls) -- the
// features aren't runtime-togglable, they're compiled in.
type KividbVariant string

const (
	// VariantStandard is the base build: no TLS, no Lua scripting.
	VariantStandard KividbVariant = "standard"

	// VariantTLS adds TLS listener support (--tls-port and friends).
	// Pair with spec.configRef's KividbConfig.spec.tls.
	VariantTLS KividbVariant = "tls"

	// VariantLua adds Lua scripting support (EVAL/EVALSHA/FUNCTION).
	VariantLua KividbVariant = "lua"

	// VariantFull includes both TLS and Lua support.
	VariantFull KividbVariant = "full"
)

// SecretKeyRef points at a single key within a Secret in the same namespace
// as the object referencing it.
type SecretKeyRef struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Key within the Secret's data.
	Key string `json:"key"`
}

// KividbUser configures a single Redis-ACL-style user that the operator
// will render into the ACL file kividb loads via --aclfile. Used by
// KividbAclConfig.
type KividbUser struct {
	// Name of the ACL user. "default" configures the built-in default user.
	Name string `json:"name"`

	// Enabled toggles the ACL "on"/"off" flag. Defaults to true.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// NoPass grants the user passwordless auth (ACL "nopass"). Mutually
	// exclusive with PasswordSecretRef.
	// +optional
	NoPass bool `json:"noPass,omitempty"`

	// PasswordSecretRef points at a Secret key holding the user's plaintext
	// password. The operator hashes it into the ACL file exactly as kividb's
	// own ACL SETUSER would (SHA-256), so the plaintext never touches disk
	// outside the Secret.
	// +optional
	PasswordSecretRef *SecretKeyRef `json:"passwordSecretRef,omitempty"`

	// KeyPatterns are ACL key patterns, e.g. ["~*"] for all keys or
	// ["~cache:*", "~session:*"]. Defaults to ["~*"] if empty.
	// +optional
	KeyPatterns []string `json:"keyPatterns,omitempty"`

	// ChannelPatterns are ACL pub/sub channel patterns, e.g. ["&*"].
	// Defaults to ["&*"] if empty.
	// +optional
	ChannelPatterns []string `json:"channelPatterns,omitempty"`

	// CommandRules are ACL command rules, e.g. ["+@all"] or
	// ["+@all", "-flushall", "-flushdb", "-config"]. Defaults to
	// ["+@all"] if empty.
	// +optional
	CommandRules []string `json:"commandRules,omitempty"`
}

// StorageSpec configures the PersistentVolumeClaim template used for each
// pod's data directory (kividb has no `dir` config directive -- the data
// directory *is* the container's working directory, so this volume is
// mounted at /data and the kividb container's workingDir is set to /data).
type StorageSpec struct {
	// Size is the requested capacity of the PVC, e.g. "10Gi".
	Size string `json:"size"`

	// StorageClassName, if set, is used for the PVC. Leave empty to use the
	// cluster default StorageClass.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// AccessModes defaults to ["ReadWriteOnce"].
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// ServiceSpec configures one of the two Services the operator manages for a
// cluster (master and replicas). Both select pods purely by RoleLabel, so
// failover never requires touching the Service object itself.
type ServiceSpec struct {
	// Type is the Kubernetes Service type. Defaults to ClusterIP.
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	Type corev1.ServiceType `json:"type,omitempty"`

	// LoadBalancerIP requests a specific IP when Type is LoadBalancer.
	// Support depends on the cloud provider's LoadBalancer controller.
	// +optional
	LoadBalancerIP string `json:"loadBalancerIP,omitempty"`

	// LoadBalancerSourceRanges restricts LoadBalancer ingress by CIDR.
	// +optional
	LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty"`

	// Annotations are merged onto the generated Service, commonly used for
	// cloud-provider LoadBalancer tuning (e.g.
	// service.beta.kubernetes.io/aws-load-balancer-type).
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ServicesSpec groups the master and replica Service configurations.
type ServicesSpec struct {
	// Master is the Service that always selects the current master pod.
	// +optional
	Master ServiceSpec `json:"master,omitempty"`

	// Replicas is the Service that load-balances across all replica pods,
	// useful for scaling out reads.
	// +optional
	Replicas ServiceSpec `json:"replicas,omitempty"`
}

// MonitoringSpec toggles metrics exposure via the per-pod agent sidecar and
// the optional redis_exporter sidecar.
type MonitoringSpec struct {
	// Enabled adds a redis_exporter sidecar (see spec.exporterImage) to
	// every pod. The agent sidecar's own lightweight /metrics is always
	// available regardless of this setting.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ServiceMonitor creates a Prometheus Operator ServiceMonitor object
	// when true. Requires the monitoring.coreos.com CRDs to be installed.
	// +optional
	ServiceMonitor bool `json:"serviceMonitor,omitempty"`
}

// FailoverSpec tunes automatic master-failover behavior.
type FailoverSpec struct {
	// Enabled turns on automatic detection and promotion. Defaults to true.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// UnhealthyThresholdSeconds is how long the master must be unready
	// before the operator promotes a replica. Defaults to 30.
	// +optional
	UnhealthyThresholdSeconds *int32 `json:"unhealthyThresholdSeconds,omitempty"`
}

// KividbClusterSpec defines the desired state of a KividbCluster.
type KividbClusterSpec struct {
	// Replicas is the number of replica pods in addition to the single
	// master. Total pods managed by the StatefulSet is Replicas+1.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas,omitempty"`

	// Image is the kividb container image, e.g.
	// "quay.io/kividbio/kividb:v1.0.2" or, for a non-standard build,
	// "quay.io/kividbio/kividb:v1.0.2-tls". Defaults to a floating,
	// unpinned tag if unset -- set this explicitly to pin a specific
	// version. The operator uses this value verbatim; it never derives or
	// modifies an image reference from Variant below.
	// +optional
	Image string `json:"image,omitempty"`

	// Variant declares which build of kividb Image actually is:
	// "standard" (default), "tls", "lua", or "full" (TLS+Lua). This is
	// informational, not instructional -- it does not change which image
	// gets pulled (see Image above). It tells the operator whether to wire
	// up variant-specific configuration (currently: TLS cert mounting and
	// CLI flags, gated on a referenced KividbConfig's spec.tls). Setting
	// Variant to "tls"/"lua"/"full" without Image actually being that kind
	// of build (or vice versa) is not validated by the API server -- the
	// operator emits a guidance Event (visible via `kubectl describe
	// kividbcluster`) on a likely mismatch, but does not block reconciling.
	// +optional
	// +kubebuilder:default=standard
	// +kubebuilder:validation:Enum=standard;tls;lua;full
	Variant KividbVariant `json:"variant,omitempty"`

	// ImagePullPolicy for the kividb container. Defaults to IfNotPresent.
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets referenced by the pod spec.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// AgentImage is the kividb-operator sidecar agent image. Defaults to
	// the operator's bundled default (same tag as the operator release).
	// +optional
	AgentImage string `json:"agentImage,omitempty"`

	// ExporterImage is the Prometheus redis_exporter sidecar image, added
	// to every pod only when spec.monitoring.enabled is true. Defaults to
	// the operator's bundled oliver006/redis_exporter version.
	// +optional
	ExporterImage string `json:"exporterImage,omitempty"`

	// Port is the RESP protocol port kividb listens on. Defaults to 6380.
	// +optional
	// +kubebuilder:default=6380
	Port int32 `json:"port,omitempty"`

	// ConfigRef names a KividbConfig in the same namespace, providing
	// kividb.conf directives and TLS settings. Optional: omit for kividb's
	// own built-in defaults plus whatever the operator itself pins (port,
	// aclfile).
	// +optional
	ConfigRef *corev1.LocalObjectReference `json:"configRef,omitempty"`

	// AclConfigRef names a KividbAclConfig in the same namespace, providing
	// ACL users and/or requirepass. Optional: omit for an open,
	// passwordless default user (fine for local/dev, not for anything
	// reachable outside the cluster).
	// +optional
	AclConfigRef *corev1.LocalObjectReference `json:"aclConfigRef,omitempty"`

	// SnapshotConfigRef names a KividbSnapshotConfig in the same namespace,
	// providing the backup schedule and S3 destination. Optional: omit to
	// disable scheduled backups entirely.
	// +optional
	SnapshotConfigRef *corev1.LocalObjectReference `json:"snapshotConfigRef,omitempty"`

	// Storage configures the per-pod PersistentVolumeClaim template.
	Storage StorageSpec `json:"storage"`

	// Resources for the kividb container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// AgentResources for the sidecar agent container.
	// +optional
	AgentResources corev1.ResourceRequirements `json:"agentResources,omitempty"`

	// ExporterResources for the redis_exporter sidecar container. Only
	// relevant when spec.monitoring.enabled is true.
	// +optional
	ExporterResources corev1.ResourceRequirements `json:"exporterResources,omitempty"`

	// Tolerations applied to every pod in the StatefulSet.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector applied to every pod in the StatefulSet.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity applied to every pod. If left nil, the operator injects a
	// preferred pod anti-affinity rule (by ClusterLabel) so replicas prefer
	// distinct nodes from each other and from the master.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PodAnnotations merged onto every pod.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// PodLabels merged onto every pod (in addition to the operator's own
	// app.kubernetes.io/*, ClusterLabel and RoleLabel labels).
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// TerminationGracePeriodSeconds overrides the pod's grace period so
	// kividb has time to complete its SIGTERM snapshot-on-shutdown path.
	// Defaults to 60.
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// Services configures the master/replica Service objects.
	// +optional
	Services ServicesSpec `json:"services,omitempty"`

	// Monitoring configures metrics exposure.
	// +optional
	Monitoring MonitoringSpec `json:"monitoring,omitempty"`

	// Failover tunes automatic master-promotion behavior.
	// +optional
	Failover FailoverSpec `json:"failover,omitempty"`
}

// KividbPodStatus reports the operator's last-observed state for one pod.
type KividbPodStatus struct {
	// Name of the Pod.
	Name string `json:"name"`

	// Role is the last-observed replication role.
	Role NodeRole `json:"role"`

	// Ready mirrors the pod's Ready condition.
	Ready bool `json:"ready"`

	// ReplicationOffset is the last-observed kividb replication offset
	// (master_repl_offset from INFO replication), used to rank replicas
	// during failover.
	// +optional
	ReplicationOffset int64 `json:"replicationOffset,omitempty"`
}

// KividbClusterStatus defines the observed state of a KividbCluster.
type KividbClusterStatus struct {
	// Phase is the coarse-grained lifecycle state.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// MasterPod is the name of the pod currently holding the master role.
	// +optional
	MasterPod string `json:"masterPod,omitempty"`

	// Pods reports per-pod role/readiness/offset.
	// +optional
	Pods []KividbPodStatus `json:"pods,omitempty"`

	// LastFailoverTime records when the operator last promoted a replica.
	// +optional
	LastFailoverTime *metav1.Time `json:"lastFailoverTime,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions follow the standard Kubernetes conditions convention.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kdb
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.masterPod`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbCluster is the Schema for the kividbclusters API. It describes a
// single master, N-replica KiviDB cluster: storage, scheduling
// constraints, exposed Services, monitoring, and failover behavior.
// Configuration, ACL users, and scheduled backups are set via separate
// KividbConfig / KividbAclConfig / KividbSnapshotConfig resources,
// referenced by name.
type KividbCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KividbClusterSpec   `json:"spec"`
	Status KividbClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KividbClusterList contains a list of KividbCluster.
type KividbClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbCluster{}, &KividbClusterList{})
}
