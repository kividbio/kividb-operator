// Package agentapi defines the JSON wire contract between the
// kividb-operator controller (client) and the per-pod agent sidecar
// (server, cmd/agent). Both sides import this package so the request and
// response shapes can never drift out of sync with each other.
//
// Endpoints. Those marked * need the agent token as a bearer token
// (Authorization: Bearer <token>, see SetToken); without it they answer
// 401, and an agent with no token configured answers them 503.
//
//	GET  /healthz       -> 200 if the agent process itself is up (the agent
//	                       container's liveness).
//	GET  /readyz        -> 200 if a RESP PING against local kividb succeeds,
//	                       503 otherwise (readiness; gates Service membership).
//	GET  /livez         -> 200 if local kividb answers anything at all within a
//	                       few seconds (the kividb container's liveness: a hung
//	                       engine accepts TCP connections and answers nothing).
//	GET  /prestop       -> blocks while local kividb is a master with replicas
//	                       attached, so the operator can hand the role over
//	                       before the engine is stopped (both containers'
//	                       preStop hook; see PreStopTimeoutEnv).
//	GET  /metrics       -> Prometheus text exposition format
//	GET  /status      * -> 200 StatusResponse
//	POST /promote     * -> 200 OKResponse | 500 ErrorResponse (REPLICAOF NO ONE)
//	POST /replicaof   * -> body ReplicaOfRequest -> 200 OKResponse | 500 ErrorResponse
//	POST /acl/reload  * -> 200 OKResponse | 500 ErrorResponse (ACL LOAD)
//	POST /backup      * -> 200 BackupResponse | 500 ErrorResponse (see backup.go for a sharded cluster's)
//	POST /exec        * -> body ExecRequest -> 200 ExecResponse (any command)
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
	Role      Role `json:"role"`
	Connected bool `json:"connected"`
	// MasterLinkUp: on a replica, whether its link to the master is up
	// (ROLE's state is "connected"). Absent on a master, and from agents
	// before 0.7.0.
	MasterLinkUp *bool `json:"masterLinkUp,omitempty"`
	// MasterLastIOSeconds: on a replica, seconds since anything arrived
	// from its master (INFO replication master_last_io_seconds_ago; -1
	// while the link is down). Absent on a master and from agents before
	// 0.7.0.
	MasterLastIOSeconds *int64 `json:"masterLastIOSeconds,omitempty"`
	// ConnectedReplicas: on a master, how many replicas are attached
	// (connected_slaves). Absent from agents before 0.7.0.
	ConnectedReplicas *int64 `json:"connectedReplicas,omitempty"`
	MasterHost        string `json:"masterHost,omitempty"`
	MasterPort        int32  `json:"masterPort,omitempty"`
	ReplicationOffset int64  `json:"replicationOffset"`
	LastSaveUnix      int64  `json:"lastSaveUnix"`
	AofEnabled        bool   `json:"aofEnabled"`
	// KeyCount is the number of keys across all databases (INFO keyspace).
	// KeyCountKnown tells a real 0 apart from an agent too old to report
	// the field at all.
	KeyCount      int64 `json:"keyCount"`
	KeyCountKnown bool  `json:"keyCountKnown,omitempty"`
}

// PreStopTimeoutEnv names the agent's environment variable holding how
// many seconds GET /prestop waits, at most, for the master role to move
// away before it lets the engine stop.
const PreStopTimeoutEnv = "PRESTOP_TIMEOUT_SECONDS"

// AclReloadRequest is the optional body of POST /acl/reload.
type AclReloadRequest struct {
	// IfFileHash, when set, is the Fingerprint (with
	// AclFileFingerprintSalt) of the ACL file the caller expects kividb to
	// load. The agent answers 409 without reloading if
	// the file mounted in the pod does not match: an updated Secret takes
	// a while to reach a running pod's volume, and reloading before then
	// would just re-read the old content.
	IfFileHash string `json:"ifFileHash,omitempty"`
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
	// SourcePod/SourceRole identify which pod actually performed the
	// backup (the agent's own POD_NAME and its role at the time) -- the
	// backup-trigger client that called this endpoint only knows a
	// Service name, not which pod handled the request.
	SourcePod  string `json:"sourcePod,omitempty"`
	SourceRole Role   `json:"sourceRole,omitempty"`
}

// BackupResult is written by `agent backup-trigger` as JSON to its own
// container's termination message (see cmd/agent/backup_trigger.go and
// the default terminationMessagePath, /dev/termination-log) so the
// controller can populate a KividbSnapshot's status from
// pod.status.containerStatuses[].state.terminated.message without
// scraping logs. Mirrors BackupResponse on success; carries Error instead
// on failure.
type BackupResult struct {
	Success bool `json:"success"`
	BackupResponse
	Error string `json:"error,omitempty"`
	// Shards is a sharded cluster's backup, one entry per shard;
	// ObjectKey is then the run's manifest.
	Shards []BackupShardResult `json:"shards,omitempty"`
}

// ExecRequest is the body of POST /exec: a RESP command as string args.
type ExecRequest struct {
	Args []string `json:"args"`
}

// ExecResponse is returned by POST /exec.
type ExecResponse struct {
	Type  string         `json:"type"` // status|error|integer|bulk|array|nil
	Str   string         `json:"str,omitempty"`
	Int   int64          `json:"int,omitempty"`
	Array []ExecResponse `json:"array,omitempty"`
	IsNil bool           `json:"isNil,omitempty"`
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
