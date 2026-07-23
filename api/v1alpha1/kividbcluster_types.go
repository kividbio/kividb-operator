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

// SecretKeyRef points at a single key within a Secret in the same namespace
// as the KividbCluster.
type SecretKeyRef struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Key within the Secret's data.
	Key string `json:"key"`
}

// KividbUser configures a single Redis-ACL-style user that the operator
// will render into the ACL file kividb loads via --aclfile.
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

// AuthSpec configures the default/legacy requirepass authentication in
// addition to (or instead of) the ACL user list.
type AuthSpec struct {
	// RequirePassSecretRef points at a Secret key holding the requirepass
	// value for the built-in default user. If unset, no requirepass is
	// configured and unauthenticated access is allowed unless the "default"
	// user is defined explicitly in Users.
	// +optional
	RequirePassSecretRef *SecretKeyRef `json:"requirePassSecretRef,omitempty"`

	// Users is the list of ACL users to render into the ACL file.
	// +optional
	Users []KividbUser `json:"users,omitempty"`
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

// S3StorageSpec configures the S3-compatible object storage destination
// backups are uploaded to. Any S3-API-compatible endpoint works: AWS S3,
// MinIO, Ceph RGW, Backblaze B2, R2, GCS (via its S3 interop endpoint), etc.
type S3StorageSpec struct {
	// Endpoint is the S3-compatible HTTP(S) endpoint, e.g.
	// "https://s3.us-east-1.amazonaws.com" or "https://minio.example.com:9000".
	Endpoint string `json:"endpoint"`

	// Bucket is the destination bucket name. Must already exist.
	Bucket string `json:"bucket"`

	// Region is passed to the S3 client. Use any placeholder (e.g. "us-east-1")
	// for providers that ignore it, such as most MinIO deployments.
	// +optional
	Region string `json:"region,omitempty"`

	// PathPrefix is prepended to every object key, e.g. "backups/prod".
	// +optional
	PathPrefix string `json:"pathPrefix,omitempty"`

	// ForcePathStyle enables path-style addressing (bucket.endpoint/key vs
	// endpoint/bucket/key). Required by most self-hosted MinIO setups.
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// InsecureSkipTLSVerify disables TLS certificate verification against
	// Endpoint. Only intended for self-signed test/dev MinIO instances.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecureSkipTLSVerify,omitempty"`

	// CredentialsSecretRef points at a Secret containing the access key ID
	// and secret access key.
	CredentialsSecretRef S3CredentialsSecretRef `json:"credentialsSecretRef"`
}

// S3CredentialsSecretRef names the Secret and keys holding S3 credentials.
type S3CredentialsSecretRef struct {
	// Name of the Secret.
	Name string `json:"name"`

	// AccessKeyIDKey is the key within the Secret holding the access key ID.
	// Defaults to "accessKeyId".
	// +optional
	AccessKeyIDKey string `json:"accessKeyIdKey,omitempty"`

	// SecretAccessKeyKey is the key within the Secret holding the secret
	// access key. Defaults to "secretAccessKey".
	// +optional
	SecretAccessKeyKey string `json:"secretAccessKeyKey,omitempty"`
}

// BackupSpec configures scheduled snapshotting to S3-compatible storage.
type BackupSpec struct {
	// Enabled turns on the scheduled backup CronJob.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is a standard cron expression, e.g. "0 * * * *" for hourly.
	// Evaluated in the timezone of the operator's kube-controller (UTC on
	// most clusters). Required when Enabled is true.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// Retention is the number of most-recent snapshots to keep in S3; older
	// ones are pruned after each successful backup. 0 means keep all.
	// +optional
	// +kubebuilder:default=7
	Retention int32 `json:"retention,omitempty"`

	// TimeoutSeconds bounds how long a single backup run (BGSAVE + upload)
	// may take before the Job is considered failed. Defaults to 900 (15m).
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`

	// S3 is the destination object storage configuration. Required when
	// Enabled is true.
	// +optional
	S3 *S3StorageSpec `json:"s3,omitempty"`

	// JobResources overrides the resource requirements of the backup Job's
	// container. Defaults to modest fixed requests/limits.
	// +optional
	JobResources corev1.ResourceRequirements `json:"jobResources,omitempty"`
}

// MonitoringSpec toggles metrics exposure via the per-pod agent sidecar.
type MonitoringSpec struct {
	// Enabled exposes a Prometheus-format /metrics endpoint on the agent
	// sidecar's port, derived from kividb's INFO output.
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

	// Image is the kividb container image, e.g. "quay.io/kividbio/kividb:v1.0.2".
	Image string `json:"image"`

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

	// KividbConfig holds free-form kividb.conf directives (the same
	// lower-kebab-case keys accepted by kividb's --configfile, e.g.
	// "maxmemory", "threads", "aof", "loglevel", "cluster-enabled",
	// "notify-keyspace-events", "slowlog-log-slower-than", "tls-port", ...).
	// The operator renders these verbatim into the generated ConfigMap; it
	// does not validate directive names beyond what the CRD schema requires
	// elsewhere (e.g. Port, Auth). Do not set "replicaof" here -- replication
	// topology is managed dynamically by the operator at runtime.
	// +optional
	KividbConfig map[string]string `json:"kividbConfig,omitempty"`

	// Auth configures requirepass and/or ACL users.
	// +optional
	Auth AuthSpec `json:"auth,omitempty"`

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

	// Backup configures scheduled S3 snapshotting.
	// +optional
	Backup BackupSpec `json:"backup,omitempty"`

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

// BackupStatus reports the outcome of the most recent backup run.
type BackupStatus struct {
	// LastRunTime is when the most recent backup Job started.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`

	// LastSuccessTime is when the most recent backup Job succeeded.
	// +optional
	LastSuccessTime *metav1.Time `json:"lastSuccessTime,omitempty"`

	// LastObjectKey is the S3 object key of the most recent successful
	// backup.
	// +optional
	LastObjectKey string `json:"lastObjectKey,omitempty"`

	// LastError holds the error message of the most recent failed backup,
	// cleared on the next success.
	// +optional
	LastError string `json:"lastError,omitempty"`
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

	// Backup reports the status of scheduled backups.
	// +optional
	Backup BackupStatus `json:"backup,omitempty"`

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
// +kubebuilder:resource:shortName=kdb;kdbc
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.masterPod`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbCluster is the Schema for the kividbclusters API. It describes a
// single master, N-replica KiviDB cluster, including its configuration,
// ACL users, storage, scheduling constraints, exposed Services, scheduled
// S3 backups, monitoring, and failover behavior.
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
