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
	// PhaseRebalancing: a sharded cluster is moving hash slots between
	// shards, after spec.shards changed.
	PhaseRebalancing ClusterPhase = "Rebalancing"
	PhaseError       ClusterPhase = "Error"
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
// image tag suffix (e.g. docker.io/kividb/kividb:v1.1.2-tls) -- the
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

// ClusterModeSpec tunes kividb's cluster mode, for a KividbCluster with
// spec.shards. The operator owns the cluster-* directives (except the
// cluster-slot-migration-* ones, which a KividbConfig may set): these are
// the ones a user may choose.
type ClusterModeSpec struct {
	// NodeTimeoutMs is cluster-node-timeout: how long a node may go
	// unanswered before the others suspect it, and so roughly how long a
	// failover takes. Defaults to 5000.
	// +optional
	// +kubebuilder:validation:Minimum=1000
	NodeTimeoutMs *int32 `json:"nodeTimeoutMs,omitempty"`

	// RequireFullCoverage is cluster-require-full-coverage: when true (the
	// default), every node refuses commands while any slot has no working
	// master.
	// +optional
	RequireFullCoverage *bool `json:"requireFullCoverage,omitempty"`

	// MinReplicasToWrite is min-replicas-to-write: a master refuses writes
	// unless at least this many replicas are connected and no more than
	// MinReplicasMaxLagSeconds behind. 0 (the default) turns it off.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReplicasToWrite *int32 `json:"minReplicasToWrite,omitempty"`

	// MinReplicasMaxLagSeconds is min-replicas-max-lag. Defaults to 10.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicasMaxLagSeconds *int32 `json:"minReplicasMaxLagSeconds,omitempty"`

	// ReplicaValidityFactor is cluster-replica-validity-factor: a replica
	// whose link to its master has been down for longer than
	// NodeTimeoutMs × ReplicaValidityFactor (plus the replication ping
	// period) does not stand for election when the master fails, as its
	// data may be too old. 0 lets every replica stand, however stale.
	// Unset leaves the engine's default, 10: with the default node timeout
	// of 5 s, a replica cut off for more than about a minute does not take
	// over. Needs engine 1.2 or later: older engines do not know the
	// directive and do not start with it.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ReplicaValidityFactor *int32 `json:"replicaValidityFactor,omitempty"`

	// BusSecretRef names the cluster-bus-secret: the nodes prove they know
	// it before the cluster bus accepts them. When unset the operator
	// generates one into the Secret <cluster>-bus.
	// +optional
	BusSecretRef *SecretKeyRef `json:"busSecretRef,omitempty"`
}

// KividbClusterSpec defines the desired state of a KividbCluster.
// +kubebuilder:validation:XValidation:rule="has(self.shards) == has(oldSelf.shards)",message="spec.shards cannot be added or removed after creation: a cluster is sharded, or not, for good"
type KividbClusterSpec struct {
	// Shards turns the cluster into a sharded one: that many shards, each
	// a master and Replicas replicas, in kividb's cluster mode, with the
	// 16384 hash slots spread across the shards. Unset: one master and its
	// replicas, as before. Set or unset only at creation; the number can
	// change later.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Shards *int32 `json:"shards,omitempty"`

	// Cluster tunes cluster mode. Only with Shards.
	// +optional
	Cluster *ClusterModeSpec `json:"cluster,omitempty"`

	// Replicas is the number of replica pods for each master: beside the
	// single master, or in each shard when Shards is set.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas,omitempty"`

	// Image is the kividb container image, e.g.
	// "docker.io/kividb/kividb:v1.1.2" or, for a non-standard build,
	// "docker.io/kividb/kividb:v1.1.2-tls". Defaults to
	// docker.io/kividb/kividb:v1.1.2 if unset -- set this explicitly to pin a specific
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

	// BootstrapFromSnapshot, when set on a new cluster, seeds pod-0's data
	// volume from a successful KividbSnapshot before kividb starts. Only
	// honored while status.bootstrap.completed is unset; after a successful
	// bootstrap the field is ignored (treat as immutable for that cluster).
	// +optional
	BootstrapFromSnapshot *BootstrapFromSnapshotSpec `json:"bootstrapFromSnapshot,omitempty"`

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

// BootstrapFromSnapshotSpec points at a KividbSnapshot used to seed a
// brand-new cluster's data volume.
type BootstrapFromSnapshotSpec struct {
	// SnapshotRef names a KividbSnapshot in the same namespace. The
	// snapshot's status.phase must be Succeeded and status.objectKey must
	// be set.
	SnapshotRef corev1.LocalObjectReference `json:"snapshotRef"`
}

// BootstrapPhase is the coarse state of snapshot bootstrap.
type BootstrapPhase string

// AllowEmptyFailoverAnnotation, set to "true" on a KividbCluster, lets
// failover promote a replica that holds no keys even though the master
// was last seen holding data. Without it the operator waits for the
// master to come back instead; see reconcileRoles.
const AllowEmptyFailoverAnnotation = "kividb.io/allow-empty-failover"

const (
	BootstrapPending    BootstrapPhase = "Pending"
	BootstrapInProgress BootstrapPhase = "InProgress"
	BootstrapCompleted  BootstrapPhase = "Completed"
	BootstrapFailed     BootstrapPhase = "Failed"
)

// BootstrapStatus reports progress of seeding from a snapshot.
type BootstrapStatus struct {
	// Phase is the current bootstrap lifecycle state.
	// +optional
	Phase BootstrapPhase `json:"phase,omitempty"`

	// SnapshotName echoes the snapshot that was (or is being) used.
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`

	// Completed is true once the PVC has been seeded successfully. When
	// true, spec.bootstrapFromSnapshot is ignored on subsequent reconciles.
	// +optional
	Completed bool `json:"completed,omitempty"`

	// Message is a human-readable summary.
	// +optional
	Message string `json:"message,omitempty"`

	// Error holds the failure reason when Phase is Failed.
	// +optional
	Error string `json:"error,omitempty"`

	// CompletionTime is when bootstrap finished successfully.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
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

	// Synced is true for the master, and for a replica that has finished
	// its initial full sync from the current master and is keeping up
	// with it. Rolling updates and restarts only move on to the next pod
	// while every pod is synced, and failover prefers synced replicas.
	// +optional
	Synced bool `json:"synced,omitempty"`

	// Keys is the last-observed number of keys held by the pod. Failover
	// uses the master's value to recognise replicas that have lost their
	// dataset.
	// +optional
	Keys int64 `json:"keys,omitempty"`

	// ReplID is, on the master's entry, the replication stream
	// (master_replid) its ReplicationOffset is a position in, recorded
	// only while the pod reports being that stream's master. Empty on a
	// replica's entry, and cleared when the master role moves.
	// +optional
	ReplID string `json:"replID,omitempty"`

	// RunID is the engine's run_id when the pod was last asked. It changes
	// whenever the engine restarts: failover treats a master's lower
	// offset as lost writes only after such a restart.
	// +optional
	RunID string `json:"runID,omitempty"`

	// Incarnation is the pod's UID and its kividb container's restart
	// count when the pod was last asked: the restart evidence used when the
	// agent does not report a run_id.
	// +optional
	Incarnation string `json:"incarnation,omitempty"`

	// Shard is the shard the pod belongs to, in a sharded cluster.
	// +optional
	Shard *int32 `json:"shard,omitempty"`

	// NodeID is the pod's cluster node ID, in a sharded cluster. The
	// operator keeps it to recognise a pod that rejoined with a new ID
	// (its volume was lost), whose old ID must then be forgotten.
	// +optional
	NodeID string `json:"nodeID,omitempty"`
}

// ShardStatus reports one shard of a sharded cluster.
type ShardStatus struct {
	// Index of the shard: its pods are <cluster>-<index>-<n>.
	Index int32 `json:"index"`

	// MasterPod is the shard's current master: after a failover, not
	// necessarily <cluster>-<index>-0.
	// +optional
	MasterPod string `json:"masterPod,omitempty"`

	// MasterNodeID is the master's cluster node ID.
	// +optional
	MasterNodeID string `json:"masterNodeID,omitempty"`

	// Slots are the hash-slot ranges the shard's master owns, e.g. "0-5460".
	// +optional
	Slots []string `json:"slots,omitempty"`

	// Pods are the shard's pods.
	// +optional
	Pods []string `json:"pods,omitempty"`

	// ReadyPods counts the shard's Ready pods.
	// +optional
	ReadyPods int32 `json:"readyPods,omitempty"`
}

// ClusterModeStatus is CLUSTER INFO as the operator last saw it.
type ClusterModeStatus struct {
	// State is cluster_state: ok, or fail.
	// +optional
	State string `json:"state,omitempty"`

	// SlotsAssigned counts the slots that have a master.
	// +optional
	SlotsAssigned int32 `json:"slotsAssigned,omitempty"`

	// SlotsFail counts the slots whose master is failed.
	// +optional
	SlotsFail int32 `json:"slotsFail,omitempty"`

	// KnownNodes counts the nodes in the cluster, this one included.
	// +optional
	KnownNodes int32 `json:"knownNodes,omitempty"`

	// CurrentEpoch is the cluster's current configuration epoch.
	// +optional
	CurrentEpoch int64 `json:"currentEpoch,omitempty"`

	// StaleNodeIDs are the old node IDs of pods that rejoined with a new
	// one (their volume was lost). The operator forgets them on every node
	// that still knows them, until none does: gossip would otherwise bring
	// them back.
	// +optional
	StaleNodeIDs []string `json:"staleNodeIDs,omitempty"`

	// Shards is how many shards the slots are spread over. When
	// spec.shards differs, the operator moves slots until it does not.
	// +optional
	Shards int32 `json:"shards,omitempty"`

	// Rebalance is a change of the shard count under way.
	// +optional
	Rebalance *RebalanceStatus `json:"rebalance,omitempty"`
}

// RebalancePhase is where a change of the shard count is.
type RebalancePhase string

const (
	// RebalanceAddingShards: waiting for every pod, new shards' too, to be
	// up and in the cluster.
	RebalanceAddingShards RebalancePhase = "AddingShards"
	// RebalanceMovingSlots: moving slots, one move at a time.
	RebalanceMovingSlots RebalancePhase = "MovingSlots"
)

// RebalanceStatus is a change of the shard count under way: from From
// shards to To, moving slots with CLUSTER MIGRATION IMPORT.
type RebalanceStatus struct {
	// From is the number of shards that may hold slots: the shard count
	// before, or more if the change was itself changed half way.
	From int32 `json:"from"`
	// To is spec.shards.
	To int32 `json:"to"`
	// +optional
	Phase RebalancePhase `json:"phase,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// Current is the move under way.
	// +optional
	Current *SlotMove `json:"current,omitempty"`
	// Pending are the moves still to make after Current, as planned now.
	// +optional
	Pending []SlotMove `json:"pending,omitempty"`
	// MovesDone counts the moves completed.
	// +optional
	MovesDone int32 `json:"movesDone,omitempty"`
	// LastError is the last move's failure, retried after a while.
	// +optional
	LastError string `json:"lastError,omitempty"`
	// +optional
	LastErrorTime *metav1.Time `json:"lastErrorTime,omitempty"`
}

// SlotMove is slots moving from one shard's master to another's.
type SlotMove struct {
	FromShard int32 `json:"fromShard"`
	ToShard   int32 `json:"toShard"`
	// Slots are ranges, "0-1364".
	Slots []string `json:"slots"`
	// SlotCount is how many slots Slots covers.
	SlotCount int32 `json:"slotCount"`
	// DestPod and TaskID: the destination master the import runs on, and
	// the engine's task ID for it.
	// +optional
	DestPod string `json:"destPod,omitempty"`
	// +optional
	TaskID string `json:"taskID,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
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

	// Shards reports each shard, in a sharded cluster.
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`

	// Cluster reports the state of cluster mode, in a sharded cluster.
	// +optional
	Cluster *ClusterModeStatus `json:"cluster,omitempty"`

	// LastFailoverTime records when the operator last promoted a replica.
	// +optional
	LastFailoverTime *metav1.Time `json:"lastFailoverTime,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Bootstrap reports snapshot-bootstrap progress when
	// spec.bootstrapFromSnapshot was set on this cluster.
	// +optional
	Bootstrap *BootstrapStatus `json:"bootstrap,omitempty"`

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
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shards`
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
