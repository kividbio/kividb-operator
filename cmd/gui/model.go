package main

import "time"

// ClusterSummary is the row shown on the dashboard (GET /api/clusters) for
// a single KividbCluster. Every field here comes straight off the
// KividbCluster object itself (spec/status) -- no other API calls are
// needed to render the dashboard.
type ClusterSummary struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Phase     string `json:"phase"`
	MasterPod string `json:"masterPod"`

	// DesiredPods is spec.replicas+1 (the master plus its replicas).
	DesiredPods int32 `json:"desiredPods"`
	// ReadyPods/TotalPods are derived from status.pods[], which the
	// controller keeps in sync with live Pod readiness on every reconcile.
	ReadyPods int `json:"readyPods"`
	TotalPods int `json:"totalPods"`

	BackupEnabled     bool       `json:"backupEnabled"`
	BackupLastSuccess *time.Time `json:"backupLastSuccess,omitempty"`
	BackupLastError   string     `json:"backupLastError,omitempty"`

	CreationTimestamp time.Time `json:"creationTimestamp"`
	Age               string    `json:"age"`
}

// PodView describes one pod for the cluster detail page. Name/Role/Ready/
// ReplicationOffset come from KividbCluster.status.pods[] (the operator's
// own last-observed view); Phase/PodIP/NodeName/RestartCount are a
// best-effort live enrichment from the Pod object itself and are left zero
// if that lookup fails or the pod no longer exists.
type PodView struct {
	Name              string `json:"name"`
	Role              string `json:"role"`
	Ready             bool   `json:"ready"`
	ReplicationOffset int64  `json:"replicationOffset"`

	Phase        string `json:"phase,omitempty"`
	PodIP        string `json:"podIP,omitempty"`
	NodeName     string `json:"nodeName,omitempty"`
	RestartCount int32  `json:"restartCount,omitempty"`
}

// ServiceView reports the live state of one of the cluster's Services
// (master or replicas). Type mirrors spec, but ClusterIP/ExternalIP/Ports
// are only known once the Service object actually exists.
type ServiceView struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	ClusterIP  string  `json:"clusterIP,omitempty"`
	ExternalIP string  `json:"externalIP,omitempty"`
	Ports      []int32 `json:"ports,omitempty"`
}

// StatefulSetView is a small cross-check of the StatefulSet's own reported
// replica counts, alongside the operator's status.pods[]-derived counts.
type StatefulSetView struct {
	DesiredReplicas int32 `json:"desiredReplicas"`
	ReadyReplicas   int32 `json:"readyReplicas"`
	CurrentReplicas int32 `json:"currentReplicas"`
	UpdatedReplicas int32 `json:"updatedReplicas"`
}

// CronJobView reports the backup CronJob's own schedule bookkeeping,
// distinct from (and a cross-check on) KividbClusterStatus.Backup.
type CronJobView struct {
	Name               string     `json:"name"`
	Schedule           string     `json:"schedule"`
	Suspended          bool       `json:"suspended"`
	LastScheduleTime   *time.Time `json:"lastScheduleTime,omitempty"`
	LastSuccessfulTime *time.Time `json:"lastSuccessfulTime,omitempty"`
}

// ConditionView mirrors metav1.Condition for JSON display.
type ConditionView struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// EventView is a trimmed-down corev1.Event for display.
type EventView struct {
	Type           string    `json:"type"`
	Reason         string    `json:"reason"`
	Message        string    `json:"message"`
	Count          int32     `json:"count"`
	Source         string    `json:"source,omitempty"`
	FirstTimestamp time.Time `json:"firstTimestamp,omitempty"`
	LastTimestamp  time.Time `json:"lastTimestamp,omitempty"`
}

// ClusterDetail is the full payload for GET /api/clusters/{namespace}/{name}.
type ClusterDetail struct {
	ClusterSummary

	Image            string `json:"image"`
	AgentImage       string `json:"agentImage,omitempty"`
	Port             int32  `json:"port"`
	StorageSize      string `json:"storageSize"`
	StorageClassName string `json:"storageClassName,omitempty"`

	BackupSchedule  string `json:"backupSchedule,omitempty"`
	BackupRetention int32  `json:"backupRetention,omitempty"`

	MasterServiceType  string `json:"masterServiceType,omitempty"`
	ReplicaServiceType string `json:"replicaServiceType,omitempty"`

	ObservedGeneration int64      `json:"observedGeneration"`
	LastFailoverTime   *time.Time `json:"lastFailoverTime,omitempty"`

	Pods        []PodView        `json:"pods"`
	Services    []ServiceView    `json:"services,omitempty"`
	StatefulSet *StatefulSetView `json:"statefulSet,omitempty"`
	CronJob     *CronJobView     `json:"cronJob,omitempty"`
	Conditions  []ConditionView  `json:"conditions,omitempty"`
	Events      []EventView      `json:"events,omitempty"`
}
