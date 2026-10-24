package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	"github.com/kividbio/kividb-operator/internal/respclient"
)

const respTimeout = 5 * time.Second

type server struct {
	cfg agentConfig
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	fs.Parse(args) //nolint:errcheck // ExitOnError already handles failures

	cfg := loadConfig()
	s := &server{cfg: cfg}

	addr := fmt.Sprintf(":%d", cfg.AgentPort)
	log.Printf("kividb-operator agent listening on %s (kividb at %s)", addr, cfg.KividbAddr)
	if cfg.AgentToken == "" {
		log.Printf("no %s set: every endpoint but the probes and /metrics will answer 503", agentapi.TokenEnv)
	}
	return http.ListenAndServe(addr, s.routes())
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	// Open: the kubelet's probes, and Prometheus. /metrics carries counters
	// from INFO, nothing that changes or reveals the data.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	// Token required: these change the server, or (/exec) run any command.
	mux.HandleFunc("GET /status", s.withToken(s.handleStatus))
	mux.HandleFunc("POST /promote", s.withToken(s.handlePromote))
	mux.HandleFunc("POST /replicaof", s.withToken(s.handleReplicaOf))
	mux.HandleFunc("POST /acl/reload", s.withToken(s.handleAclReload))
	mux.HandleFunc("POST /backup", s.withToken(s.handleBackup))
	mux.HandleFunc("POST /backup/manifest", s.withToken(s.handleBackupManifest))
	mux.HandleFunc("POST /exec", s.withToken(s.handleExec))
	// Cluster mode (sharded KividbClusters).
	mux.HandleFunc("GET /cluster/status", s.withToken(s.handleClusterStatus))
	mux.HandleFunc("POST /cluster/meet", s.withToken(s.handleClusterMeet))
	mux.HandleFunc("POST /cluster/addslots", s.withToken(s.handleClusterAddSlots))
	mux.HandleFunc("POST /cluster/replicate", s.withToken(s.handleClusterReplicate))
	mux.HandleFunc("POST /cluster/forget", s.withToken(s.handleClusterForget))
	mux.HandleFunc("POST /cluster/announce", s.withToken(s.handleClusterAnnounce))
	mux.HandleFunc("POST /cluster/failover", s.withToken(s.handleClusterFailover))
	return mux
}

// withToken lets a request through only with the agent token. Anyone who
// can reach the pod could otherwise promote it, repoint its replication or
// run any command through /exec.
func (s *server) withToken(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AgentToken == "" {
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("this agent has no %s: it refuses everything but the probes and /metrics", agentapi.TokenEnv))
			return
		}
		if !agentapi.TokenMatches(r, s.cfg.AgentToken) {
			writeError(w, http.StatusUnauthorized, fmt.Errorf("missing or wrong agent token"))
			return
		}
		h(w, r)
	}
}

func (s *server) dial() (*respclient.Client, error) {
	c, err := respclient.Dial(s.cfg.KividbAddr, respTimeout)
	if err != nil {
		return nil, err
	}
	if err := c.Auth(s.cfg.AuthUsername, s.cfg.AuthPassword); err != nil {
		c.Close()
		return nil, fmt.Errorf("auth: %w", err)
	}
	return c, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, agentapi.ErrorResponse{Error: err.Error()})
}

// handleHealthz is a pure liveness check of the agent process itself; it
// never talks to kividb, so a kividb-side outage cannot get the agent
// container OOMKilled/restarted by liveness failures.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleReadyz performs a real RESP PING against local kividb. This is the
// probe wired to the *kividb* container's readinessProbe (see
// statefulset.go), since kividb ships no HTTP endpoint of its own -- a
// failing PING here removes the pod from both the master and replica
// Services via their Ready-gated Endpoints.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	c, err := s.dial()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.queryStatus()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) queryStatus() (*agentapi.StatusResponse, error) {
	c, err := s.dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	roleReply, err := c.Role()
	if err != nil {
		return nil, fmt.Errorf("ROLE: %w", err)
	}

	out := &agentapi.StatusResponse{Role: agentapi.RoleUnknown, Connected: true}
	if len(roleReply.Array) == 0 {
		return out, nil
	}
	switch roleReply.Array[0].Str {
	case "master":
		out.Role = agentapi.RoleMaster
		if len(roleReply.Array) > 1 {
			out.ReplicationOffset = roleReply.Array[1].Int
		}
	case "slave":
		out.Role = agentapi.RoleReplica
		if len(roleReply.Array) > 4 {
			out.MasterHost = roleReply.Array[1].Str
			// The port is a RESP integer (as in Redis); older engines sent
			// a string. Anything outside the TCP port range is ignored and
			// reported as 0, i.e. unknown.
			port := roleReply.Array[2].Int
			if port == 0 {
				if p, err := strconv.ParseInt(roleReply.Array[2].Str, 10, 32); err == nil {
					port = p
				}
			}
			if port > 0 && port <= 65535 {
				out.MasterPort = int32(port)
			}
			out.ReplicationOffset = roleReply.Array[4].Int
		}
	}

	if lastSave, err := c.LastSave(); err == nil {
		out.LastSaveUnix = lastSave
	}
	if info, err := c.Info("persistence"); err == nil {
		fields := respclient.ParseInfo(info)
		out.AofEnabled = fields["aof_enabled"] == "1"
	}
	if info, err := c.Info("keyspace"); err == nil {
		out.KeyCount = keyspaceKeyCount(info)
		out.KeyCountKnown = true
	}
	return out, nil
}

// keyspaceKeyCount sums keys=N over the "dbN:keys=N,expires=M,..." lines
// of INFO keyspace.
func keyspaceKeyCount(info string) int64 {
	var total int64
	for name, value := range respclient.ParseInfo(info) {
		if !strings.HasPrefix(name, "db") {
			continue
		}
		for _, field := range strings.Split(value, ",") {
			if n, ok := strings.CutPrefix(field, "keys="); ok {
				if v, err := strconv.ParseInt(n, 10, 64); err == nil {
					total += v
				}
			}
		}
	}
	return total
}

// fileHash returns the ACL-file fingerprint of path's contents (see
// agentapi.Fingerprint), or "" if path is unset or unreadable.
func fileHash(path string) string {
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return agentapi.Fingerprint(string(b), agentapi.AclFileFingerprintSalt)
}

func (s *server) handlePromote(w http.ResponseWriter, r *http.Request) {
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer c.Close()
	if err := c.ReplicaOfNoOne(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}

func (s *server) handleReplicaOf(w http.ResponseWriter, r *http.Request) {
	var req agentapi.ReplicaOfRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Host == "" || req.Port == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("host and port are required"))
		return
	}
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer c.Close()
	s.setMasterAuth(c)
	if err := c.ReplicaOf(req.Host, req.Port); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}

// setMasterAuth gives kividb the password to present to its master. From
// kividb v1.0.5 a master whose default user has a password refuses an
// unauthenticated replication handshake, so without this no replica of an
// ACL-protected cluster ever syncs. Every pod of a cluster shares the
// default user's password, so the one this agent authenticates with
// locally is also the one the master expects.
//
// It is set at runtime rather than as a --masterauth flag because the
// operator does not know which engine version an image is: older engines
// reject the flag at startup, whereas CONFIG SET of a parameter they do not
// have is just an error reply, ignored here.
func (s *server) setMasterAuth(c *respclient.Client) {
	if s.cfg.AuthPassword == "" {
		return
	}
	_, _ = c.Do("CONFIG", "SET", "masterauth", s.cfg.AuthPassword)
}

func (s *server) handleAclReload(w http.ResponseWriter, r *http.Request) {
	var req agentapi.AclReloadRequest
	_ = json.NewDecoder(r.Body).Decode(&req) // the body is optional
	if req.IfFileHash != "" && fileHash(s.cfg.AclFile) != req.IfFileHash {
		writeError(w, http.StatusConflict, fmt.Errorf("the ACL file mounted in this pod does not have the expected content yet"))
		return
	}
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer c.Close()
	if err := c.AclLoad(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.dropUsersNotInAclFile(c); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}

// dropUsersNotInAclFile deletes every kividb user the ACL file no longer
// defines. kividb's ACL LOAD only adds and updates the users it finds in
// the file; one that was removed from it stays, password and all, until
// the process restarts.
func (s *server) dropUsersNotInAclFile(c *respclient.Client) error {
	if s.cfg.AclFile == "" {
		return nil
	}
	content, err := os.ReadFile(s.cfg.AclFile)
	if err != nil {
		return fmt.Errorf("reading ACL file: %w", err)
	}
	defined := aclFileUsers(string(content))

	stale := func() ([]string, error) {
		reply, err := c.Do("ACL", "USERS")
		if err != nil {
			return nil, fmt.Errorf("ACL USERS: %w", err)
		}
		var names []string
		for _, u := range reply.Array {
			if !defined[u.Str] && u.Str != "default" {
				names = append(names, u.Str)
			}
		}
		return names, nil
	}

	names, err := stale()
	if err != nil || len(names) == 0 {
		return err
	}
	// The reply to DELUSER is not what decides success: with the ACL file
	// on a read-only Secret mount kividb deletes the user and then reports
	// that it could not rewrite the file. Look at the user list again
	// instead.
	_, _ = c.Do(append([]string{"ACL", "DELUSER"}, names...)...)
	left, err := stale()
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return fmt.Errorf("could not delete users removed from the ACL file: %s", strings.Join(left, ", "))
	}
	return nil
}

// aclFileUsers returns the names defined by "user <name> ..." lines.
func aclFileUsers(content string) map[string]bool {
	names := map[string]bool{}
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "user" {
			names[fields[1]] = true
		}
	}
	return names
}

func (s *server) handleBackup(w http.ResponseWriter, r *http.Request) {
	var req agentapi.BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Run != "" {
		if err := agentapi.CheckRun(req.Run); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if req.Shard < 0 || req.Shard > 1023 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("bad shard %d", req.Shard))
			return
		}
	}
	resp, err := s.runBackup(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleExec(w http.ResponseWriter, r *http.Request) {
	var req agentapi.ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Args) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("args must be a non-empty command"))
		return
	}
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer c.Close()
	reply, err := c.Do(req.Args...)
	if err != nil && reply == nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := replyToExec(reply)
	// RESP error replies are still HTTP 200 with type=error so the GUI can
	// display the engine message without treating transport as failed.
	writeJSON(w, http.StatusOK, out)
}

func replyToExec(r *respclient.Reply) agentapi.ExecResponse {
	if r == nil {
		return agentapi.ExecResponse{Type: "nil", IsNil: true}
	}
	switch r.Type {
	case '+':
		return agentapi.ExecResponse{Type: "status", Str: r.Str}
	case '-':
		return agentapi.ExecResponse{Type: "error", Str: r.Str}
	case ':':
		return agentapi.ExecResponse{Type: "integer", Int: r.Int}
	case '$':
		if r.IsNil {
			return agentapi.ExecResponse{Type: "nil", IsNil: true}
		}
		return agentapi.ExecResponse{Type: "bulk", Str: r.Str}
	case '*':
		if r.IsNil {
			return agentapi.ExecResponse{Type: "nil", IsNil: true}
		}
		arr := make([]agentapi.ExecResponse, len(r.Array))
		for i, item := range r.Array {
			arr[i] = replyToExec(item)
		}
		return agentapi.ExecResponse{Type: "array", Array: arr}
	default:
		return agentapi.ExecResponse{Type: "bulk", Str: r.Str}
	}
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := s.renderMetrics()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(body))
}
