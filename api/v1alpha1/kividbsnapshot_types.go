package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotPhase is the lifecycle state of a single KividbSnapshot.
type SnapshotPhase string

const (
	SnapshotPending    SnapshotPhase = "Pending"
	SnapshotInProgress SnapshotPhase = "InProgress"
	SnapshotSucceeded  SnapshotPhase = "Succeeded"
	SnapshotFailed     SnapshotPhase = "Failed"
)

// KividbSnapshotSpec identifies which cluster and snapshot configuration
// this snapshot belongs to. KividbSnapshot objects are created and
// managed by the operator, one per backup Job run -- you don't normally
// create these yourself (see docs/BACKUP_RESTORE.md for the one
// documented exception: recording a manually-triggered backup).
type KividbSnapshotSpec struct {
	// ClusterRef names the KividbCluster this snapshot was taken from.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// SnapshotConfigRef names the KividbSnapshotConfig that produced this
	// run (its schedule, retention, and S3 destination).
	SnapshotConfigRef corev1.LocalObjectReference `json:"snapshotConfigRef"`
}

// KividbSnapshotStatus reports the outcome of one backup run.
type KividbSnapshotStatus struct {
	// Phase is the current lifecycle state.
	// +optional
	Phase SnapshotPhase `json:"phase,omitempty"`

	// StartTime is when the backup Job started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the backup Job finished (success or failure).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// SourcePod is the pod the snapshot was actually taken from.
	// +optional
	SourcePod string `json:"sourcePod,omitempty"`

	// SourceRole is the role (master/replica) SourcePod held at the time,
	// matching the owning KividbSnapshotConfig's spec.source.
	// +optional
	SourceRole NodeRole `json:"sourceRole,omitempty"`

	// ObjectKey is the S3 object key the snapshot was uploaded to.
	// +optional
	ObjectKey string `json:"objectKey,omitempty"`

	// SizeBytes is the uploaded object's size.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// DurationMs is how long the BGSAVE+upload took.
	// +optional
	DurationMs int64 `json:"durationMs,omitempty"`

	// Error holds the failure reason when Phase is Failed.
	// +optional
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kdbs
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Object Key",type=string,JSONPath=`.status.objectKey`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbSnapshot records the outcome of a single backup run: which pod it
// was taken from, whether it succeeded, and the exact S3 object it
// produced. One is created per run of the owning KividbSnapshotConfig's
// schedule.
type KividbSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KividbSnapshotSpec   `json:"spec"`
	Status KividbSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KividbSnapshotList contains a list of KividbSnapshot.
type KividbSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbSnapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbSnapshot{}, &KividbSnapshotList{})
}
