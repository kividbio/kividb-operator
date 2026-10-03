package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DbOpsPhase is the lifecycle state of a KividbDbOps.
type DbOpsPhase string

const (
	DbOpsPending   DbOpsPhase = "Pending"
	DbOpsRunning   DbOpsPhase = "Running"
	DbOpsCompleted DbOpsPhase = "Completed"
	DbOpsFailed    DbOpsPhase = "Failed"
)

// DbOpsType is the operation to perform.
type DbOpsType string

const (
	// DbOpsRestart performs a controlled rolling restart of cluster pods.
	DbOpsRestart DbOpsType = "restart"
)

// RestartMethod controls how a restart is executed.
type RestartMethod string

const (
	// RestartInPlace deletes/recreates existing pods one at a time (no
	// temporary extra replica). Extra methods (e.g. ReducedImpact) may be
	// added in a later release.
	RestartInPlace RestartMethod = "InPlace"
)

// KividbDbOpsSpec defines a declarative database operation against a
// KividbCluster (StackGres SGDbOps-style).
type KividbDbOpsSpec struct {
	// ClusterRef names the KividbCluster this operation targets.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Op is the operation kind. Currently only "restart" is supported.
	// +kubebuilder:validation:Enum=restart
	Op DbOpsType `json:"op"`

	// Restart configures a rolling restart when Op is "restart".
	// +optional
	Restart *DbOpsRestartSpec `json:"restart,omitempty"`

	// MaxRetries is how many times a failed step may be retried. 0 means
	// no retries.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	MaxRetries int32 `json:"maxRetries,omitempty"`
}

// DbOpsRestartSpec configures the restart operation.
type DbOpsRestartSpec struct {
	// Method is how pods are restarted. Defaults to InPlace.
	// +optional
	// +kubebuilder:default=InPlace
	// +kubebuilder:validation:Enum=InPlace
	Method RestartMethod `json:"method,omitempty"`

	// OnlyPendingRestart, when true, restarts only pods the operator has
	// marked as needing a restart. When false (default), every pod is
	// restarted once.
	// +optional
	OnlyPendingRestart bool `json:"onlyPendingRestart,omitempty"`
}

// DbOpsRestartStatus tracks restart progress.
type DbOpsRestartStatus struct {
	// CompletedPods lists pods that have been successfully restarted.
	// +optional
	CompletedPods []string `json:"completedPods,omitempty"`

	// PendingPods lists pods still waiting to be restarted.
	// +optional
	PendingPods []string `json:"pendingPods,omitempty"`

	// CurrentPod is the pod currently being restarted, if any.
	// +optional
	CurrentPod string `json:"currentPod,omitempty"`

	// CurrentPodUID is the UID of the pod that was deleted for CurrentPod.
	// The restart of that pod is only complete once a pod with a different
	// UID has taken its place.
	// +optional
	CurrentPodUID string `json:"currentPodUID,omitempty"`
}

// KividbDbOpsStatus reports operation progress.
type KividbDbOpsStatus struct {
	// Phase is the current lifecycle state.
	// +optional
	Phase DbOpsPhase `json:"phase,omitempty"`

	// StartTime is when the controller began executing the op.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the op finished (success or failure).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message is a human-readable summary of the current/last step.
	// +optional
	Message string `json:"message,omitempty"`

	// Error holds the failure reason when Phase is Failed.
	// +optional
	Error string `json:"error,omitempty"`

	// Restart holds restart-specific progress when Op is restart.
	// +optional
	Restart *DbOpsRestartStatus `json:"restart,omitempty"`

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
// +kubebuilder:resource:shortName=kdbops
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Op",type=string,JSONPath=`.spec.op`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbDbOps is a declarative database operation (e.g. rolling restart)
// against a KividbCluster.
type KividbDbOps struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KividbDbOpsSpec   `json:"spec"`
	Status KividbDbOpsStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KividbDbOpsList contains a list of KividbDbOps.
type KividbDbOpsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbDbOps `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbDbOps{}, &KividbDbOpsList{})
}
