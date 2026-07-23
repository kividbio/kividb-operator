// Package agentapi defines the JSON wire contract between the
// kividb-operator controller (client) and the per-pod agent sidecar
// (server, cmd/agent). Both sides import this package so the request and
// response shapes can never drift out of sync with each other.
//
// Endpoints:
//
//	GET  /healthz      -> 200 if the agent process itself is up (liveness).
//	GET  /readyz        -> 200 if a RESP PING against local kividb succeeds,
//	                        503 otherwise (readiness; gates Service membership).
//	GET  /status        -> 200 StatusResponse
//	POST /promote        -> 200 OKResponse | 500 ErrorResponse (REPLICAOF NO ONE)
//	POST /replicaof      -> body ReplicaOfRequest -> 200 OKResponse | 500 ErrorResponse
//	POST /acl/reload    -> 200 OKResponse | 500 ErrorResponse (ACL LOAD)
//	POST /backup         -> 200 BackupResponse | 500 ErrorResponse
//	GET  /metrics        -> Prometheus text exposition format
package agentapi

// Role mirrors kividbv1alpha1.NodeRole without importing the API package,
// keeping cmd/agent's dependency graph free of controller-runtime.
type Role string

const (
	RoleMaster  Role = "master"
	RoleReplica Role = "replica"
	RoleUnknown Role = "unknown"
)

// StatusResponse is returned by GET /status.
type StatusResponse struct {
	Role              Role   `json:"role"`
	Connected         bool   `json:"connected"`
	MasterHost        string `json:"masterHost,omitempty"`
	MasterPort        int32  `json:"masterPort,omitempty"`
	ReplicationOffset int64  `json:"replicationOffset"`
	LastSaveUnix      int64  `json:"lastSaveUnix"`
	AofEnabled        bool   `json:"aofEnabled"`
}

// ReplicaOfRequest is the body of POST /replicaof.
type ReplicaOfRequest struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

// BackupResponse is returned by a successful POST /backup.
type BackupResponse struct {
	ObjectKey   string `json:"objectKey"`
	SizeBytes   int64  `json:"sizeBytes"`
	DurationMs  int64  `json:"durationMs"`
	PrunedCount int    `json:"prunedCount"`
}

// OKResponse is returned by simple actions that have no extra payload.
type OKResponse struct {
	OK bool `json:"ok"`
}

// ErrorResponse is returned (with a non-2xx status code) whenever an agent
// action fails.
type ErrorResponse struct {
	Error string `json:"error"`
}
