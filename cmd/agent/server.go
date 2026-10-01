package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("POST /promote", s.handlePromote)
	mux.HandleFunc("POST /replicaof", s.handleReplicaOf)
	mux.HandleFunc("POST /acl/reload", s.handleAclReload)
	mux.HandleFunc("POST /backup", s.handleBackup)
	mux.HandleFunc("POST /exec", s.handleExec)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	addr := fmt.Sprintf(":%d", cfg.AgentPort)
	log.Printf("kividb-operator agent listening on %s (kividb at %s)", addr, cfg.KividbAddr)
	return http.ListenAndServe(addr, mux)
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
			// The port is a RESP integer (as in Redis), not a string.
			out.MasterPort = int32(roleReply.Array[2].Int)
			if p, err := strconv.Atoi(roleReply.Array[2].Str); err == nil && out.MasterPort == 0 {
				out.MasterPort = int32(p)
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

// fileHash returns the hex SHA-256 of path's contents, or "" if path is
// unset or unreadable.
func fileHash(path string) string {
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
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
	if err := c.ReplicaOf(req.Host, req.Port); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
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
	resp, err := s.runBackup(r.Context())
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
