package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// SnapshotSource selects which role's pod a scheduled snapshot is taken
// from.
type SnapshotSource string

const (
	// SnapshotSourceMaster takes the snapshot from the current master.
	// Recommended: the master always has the most complete, up-to-date
	// data, and kividb's own replication has no guaranteed consistency
	// lag bound today, so a replica-sourced snapshot can be measurably
	// stale.
	SnapshotSourceMaster SnapshotSource = "master"

	// SnapshotSourceReplica takes the snapshot from a replica instead,
	// trading a small, variable staleness window for zero BGSAVE I/O
	// impact on the pod serving writes.
	SnapshotSourceReplica SnapshotSource = "replica"
)

// KividbSnapshotConfigSpec defines a reusable backup destination,
// schedule, and retention policy, referenced by name from one or more
// KividbClusters via spec.snapshotConfigRef -- analogous to StackGres's
// SGObjectStorage plus its cluster-level backup schedule, combined into
// one resource here. Each run creates a KividbSnapshot record.
type KividbSnapshotConfigSpec struct {
	// Schedule is a standard cron expression, e.g. "0 * * * *" for hourly.
	Schedule string `json:"schedule"`

	// Retention is the number of most-recent snapshots to keep in S3; 0
	// means keep all.
	// +optional
	// +kubebuilder:default=7
	Retention int32 `json:"retention,omitempty"`

	// Source selects which role's pod the snapshot is taken from.
	// Defaults to, and is recommended to stay, "master".
	// +optional
	// +kubebuilder:default=master
	// +kubebuilder:validation:Enum=master;replica
	Source SnapshotSource `json:"source,omitempty"`

	// TimeoutSeconds bounds how long a single backup run (BGSAVE + upload)
	// may take before it's considered failed. Defaults to 900 (15m).
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`

	// S3 is the destination object storage configuration.
	S3 S3StorageSpec `json:"s3"`

	// JobResources overrides the resource requirements of the backup Job's
	// container. Defaults to modest fixed requests/limits.
	// +optional
	JobResources corev1.ResourceRequirements `json:"jobResources,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=kdbsc
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbSnapshotConfig is a reusable, standalone backup destination and
// schedule. Create one and reference it by name from any number of
// KividbClusters' spec.snapshotConfigRef.
type KividbSnapshotConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec KividbSnapshotConfigSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// KividbSnapshotConfigList contains a list of KividbSnapshotConfig.
type KividbSnapshotConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbSnapshotConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbSnapshotConfig{}, &KividbSnapshotConfigList{})
}
