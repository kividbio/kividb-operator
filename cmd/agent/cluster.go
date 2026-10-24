package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	"github.com/kividbio/kividb-operator/internal/respclient"
)

// The cluster-mode endpoints: the operator forms and looks after a sharded
// cluster through these. Each is one or two RESP commands; the decisions
// are the operator's.

// handleClusterStatus reports what this node knows of itself and of the
// cluster: CLUSTER MYID, INFO, NODES, and its announced hostname.
func (s *server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	defer c.Close()

	out := agentapi.ClusterStatusResponse{}
	info, err := c.Do("CLUSTER", "INFO")
	if err != nil {
		// "ERR This instance has cluster support disabled", or an old engine.
		writeJSON(w, http.StatusOK, out)
		return
	}
	out.Enabled = true
	fields := respclient.ParseInfo(info.Str)
	out.State = fields["cluster_state"]
	out.SlotsAssigned, _ = strconv.Atoi(fields["cluster_slots_assigned"])
	out.SlotsFail, _ = strconv.Atoi(fields["cluster_slots_fail"])
	out.KnownNodes, _ = strconv.Atoi(fields["cluster_known_nodes"])
	out.CurrentEpoch, _ = strconv.ParseInt(fields["cluster_current_epoch"], 10, 64)

	if id, err := c.Do("CLUSTER", "MYID"); err == nil {
		out.MyID = id.Str
	}
	nodes, err := c.Do("CLUSTER", "NODES")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("CLUSTER NODES: %w", err))
		return
	}
	out.Nodes = agentapi.ParseClusterNodes(nodes.Str)

	if v, err := c.Do("CONFIG", "GET", "cluster-announce-hostname"); err == nil && len(v.Array) == 2 {
		out.Hostname = v.Array[1].Str
	}
	if info, err := c.Info("replication"); err == nil {
		repl := respclient.ParseInfo(info)
		key := "master_repl_offset"
		if repl["role"] == "slave" {
			if _, ok := repl["slave_repl_offset"]; ok {
				key = "slave_repl_offset"
			}
		}
		out.ReplicationOffset, _ = strconv.ParseInt(repl[key], 10, 64)
		out.MasterLinkUp = repl["master_link_status"] == "up"
	}
	writeJSON(w, http.StatusOK, out)
}

// clusterCommand decodes the request into req, runs the command build
// returns, and answers OK.
func clusterCommand[T any](s *server, w http.ResponseWriter, r *http.Request, build func(T) ([]string, error)) {
	var req T
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	args, err := build(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	defer c.Close()
	if _, err := c.Do(args...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("%s: %w", strings.Join(args[:min(2, len(args))], " "), err))
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}

func (s *server) handleClusterMeet(w http.ResponseWriter, r *http.Request) {
	clusterCommand(s, w, r, func(req agentapi.ClusterMeetRequest) ([]string, error) {
		if req.IP == "" || req.Port <= 0 {
			return nil, fmt.Errorf("ip and port are required")
		}
		return []string{"CLUSTER", "MEET", req.IP, strconv.Itoa(req.Port)}, nil
	})
}

func (s *server) handleClusterAddSlots(w http.ResponseWriter, r *http.Request) {
	clusterCommand(s, w, r, func(req agentapi.ClusterSlotsRequest) ([]string, error) {
		if len(req.Ranges) == 0 {
			return nil, fmt.Errorf("no ranges")
		}
		args := []string{"CLUSTER", "ADDSLOTSRANGE"}
		for _, rg := range req.Ranges {
			if rg[0] < 0 || rg[1] < rg[0] || rg[1] >= agentapi.TotalSlots {
				return nil, fmt.Errorf("bad range %d-%d", rg[0], rg[1])
			}
			args = append(args, strconv.Itoa(rg[0]), strconv.Itoa(rg[1]))
		}
		return args, nil
	})
}

func nodeIDArg(id string) error {
	if len(id) != 40 || strings.Trim(id, "0123456789abcdef") != "" {
		return fmt.Errorf("%q is not a node ID", id)
	}
	return nil
}

func (s *server) handleClusterReplicate(w http.ResponseWriter, r *http.Request) {
	clusterCommand(s, w, r, func(req agentapi.ClusterNodeRequest) ([]string, error) {
		if err := nodeIDArg(req.NodeID); err != nil {
			return nil, err
		}
		return []string{"CLUSTER", "REPLICATE", req.NodeID}, nil
	})
}

func (s *server) handleClusterForget(w http.ResponseWriter, r *http.Request) {
	clusterCommand(s, w, r, func(req agentapi.ClusterNodeRequest) ([]string, error) {
		if err := nodeIDArg(req.NodeID); err != nil {
			return nil, err
		}
		return []string{"CLUSTER", "FORGET", req.NodeID}, nil
	})
}

func (s *server) handleClusterAnnounce(w http.ResponseWriter, r *http.Request) {
	clusterCommand(s, w, r, func(req agentapi.ClusterAnnounceRequest) ([]string, error) {
		if req.Hostname == "" || strings.Trim(strings.ToLower(req.Hostname), "abcdefghijklmnopqrstuvwxyz0123456789-.") != "" {
			return nil, fmt.Errorf("%q is not a hostname", req.Hostname)
		}
		return []string{"CONFIG", "SET", "cluster-announce-hostname", req.Hostname}, nil
	})
}

// handleClusterFailover asks this node, a replica, to take over from its
// master: CLUSTER FAILOVER, which the two run together without losing a
// write. The engine refuses it on a master.
func (s *server) handleClusterFailover(w http.ResponseWriter, r *http.Request) {
	c, err := s.dial()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	defer c.Close()
	if _, err := c.Do("CLUSTER", "FAILOVER"); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("CLUSTER FAILOVER: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, agentapi.OKResponse{OK: true})
}
