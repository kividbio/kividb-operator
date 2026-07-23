package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
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
			if p, err := strconv.Atoi(roleReply.Array[2].Str); err == nil {
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
	return out, nil
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
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}

func (s *server) handleBackup(w http.ResponseWriter, r *http.Request) {
	resp, err := s.runBackup(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
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
