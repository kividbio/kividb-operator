package agentapi

import (
	"strconv"
	"strings"
)

// Cluster-mode endpoints of the agent (sharded KividbClusters). All need
// the agent token.
//
//	GET  /cluster/status    -> 200 ClusterStatusResponse
//	POST /cluster/meet      -> body ClusterMeetRequest      (CLUSTER MEET ip port)
//	POST /cluster/addslots  -> body ClusterSlotsRequest     (CLUSTER ADDSLOTSRANGE)
//	POST /cluster/replicate -> body ClusterNodeRequest      (CLUSTER REPLICATE id)
//	POST /cluster/forget    -> body ClusterNodeRequest      (CLUSTER FORGET id)
//	POST /cluster/announce  -> body ClusterAnnounceRequest  (CONFIG SET cluster-announce-hostname)
//	POST /cluster/failover  -> on a replica: CLUSTER FAILOVER, its master hands over

// TotalSlots is the number of hash slots in a cluster.
const TotalSlots = 16384

// ClusterNode is one line of CLUSTER NODES.
type ClusterNode struct {
	ID       string   `json:"id"`
	IP       string   `json:"ip,omitempty"`
	Port     int      `json:"port,omitempty"`
	BusPort  int      `json:"busPort,omitempty"`
	Hostname string   `json:"hostname,omitempty"`
	Flags    []string `json:"flags,omitempty"`
	// MasterID is the master this node replicates, "" for a master.
	MasterID  string `json:"masterID,omitempty"`
	LinkState string `json:"linkState,omitempty"`
	// Slots are the ranges the node owns, "0-5460" or "42".
	Slots []string `json:"slots,omitempty"`
}

// Has reports whether the node carries flag (myself, master, slave, fail,
// fail?, handshake, noaddr).
func (n ClusterNode) Has(flag string) bool {
	for _, f := range n.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

// IsMaster: the node is a master (it may own no slots).
func (n ClusterNode) IsMaster() bool { return n.Has("master") }

// Failed: the cluster has marked the node failed, or this node suspects it.
func (n ClusterNode) Failed() bool { return n.Has("fail") || n.Has("fail?") }

// SlotCount is how many slots the node owns.
func (n ClusterNode) SlotCount() int {
	total := 0
	for _, r := range n.Slots {
		lo, hi, ok := ParseSlotRange(r)
		if ok {
			total += hi - lo + 1
		}
	}
	return total
}

// ParseSlotRange parses "0-5460" or "42".
func ParseSlotRange(s string) (lo, hi int, ok bool) {
	a, b, isRange := strings.Cut(s, "-")
	lo, err := strconv.Atoi(a)
	if err != nil {
		return 0, 0, false
	}
	hi = lo
	if isRange {
		if hi, err = strconv.Atoi(b); err != nil {
			return 0, 0, false
		}
	}
	return lo, hi, lo >= 0 && hi >= lo && hi < TotalSlots
}

// ParseClusterNodes parses the reply of CLUSTER NODES:
//
//	<id> <ip:port@busport[,hostname]> <flags> <master|-> <ping> <pong> <epoch> <link> <slot>...
//
// Slot entries in brackets (migrating/importing markers) are left out.
func ParseClusterNodes(text string) []ClusterNode {
	var nodes []ClusterNode
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) < 8 {
			continue
		}
		n := ClusterNode{ID: f[0], Flags: strings.Split(f[2], ","), LinkState: f[7]}
		if f[3] != "-" {
			n.MasterID = f[3]
		}
		addr, host, _ := strings.Cut(f[1], ",")
		n.Hostname = host
		hostPort, bus, _ := strings.Cut(addr, "@")
		if i := strings.LastIndexByte(hostPort, ':'); i >= 0 {
			n.IP = hostPort[:i]
			n.Port, _ = strconv.Atoi(hostPort[i+1:])
		}
		n.BusPort, _ = strconv.Atoi(bus)
		for _, s := range f[8:] {
			if !strings.HasPrefix(s, "[") {
				n.Slots = append(n.Slots, s)
			}
		}
		nodes = append(nodes, n)
	}
	return nodes
}

// ClusterStatusResponse is GET /cluster/status: what the node knows of
// itself and of the cluster.
type ClusterStatusResponse struct {
	// Enabled is false when kividb is not in cluster mode.
	Enabled bool   `json:"enabled"`
	MyID    string `json:"myID,omitempty"`
	// State is cluster_state from CLUSTER INFO: ok or fail.
	State         string `json:"state,omitempty"`
	SlotsAssigned int    `json:"slotsAssigned,omitempty"`
	SlotsFail     int    `json:"slotsFail,omitempty"`
	KnownNodes    int    `json:"knownNodes,omitempty"`
	CurrentEpoch  int64  `json:"currentEpoch,omitempty"`
	// Hostname is the node's cluster-announce-hostname.
	Hostname string `json:"hostname,omitempty"`
	// ReplicationOffset is the node's own offset (master_repl_offset on a
	// master, the applied offset on a replica).
	ReplicationOffset int64 `json:"replicationOffset,omitempty"`
	// MasterLinkUp: a replica's link to its master is up
	// (master_link_status).
	MasterLinkUp bool `json:"masterLinkUp,omitempty"`
	// MasterLastIOSeconds: on a replica, seconds since anything arrived
	// from its master (master_last_io_seconds_ago; -1 while the link is
	// down). Reported, not used to decide anything.
	MasterLastIOSeconds *int64        `json:"masterLastIOSeconds,omitempty"`
	Nodes               []ClusterNode `json:"nodes,omitempty"`
}

// Myself is the node's own line of CLUSTER NODES.
func (s *ClusterStatusResponse) Myself() (ClusterNode, bool) {
	for _, n := range s.Nodes {
		if n.Has("myself") {
			return n, true
		}
	}
	return ClusterNode{}, false
}

// Node returns the node with id as this node sees it.
func (s *ClusterStatusResponse) Node(id string) (ClusterNode, bool) {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return ClusterNode{}, false
}

// ClusterMeetRequest is POST /cluster/meet.
type ClusterMeetRequest struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// ClusterSlotsRequest is POST /cluster/addslots: inclusive ranges.
type ClusterSlotsRequest struct {
	Ranges [][2]int `json:"ranges"`
}

// ClusterNodeRequest names a node, for /cluster/replicate and /forget.
type ClusterNodeRequest struct {
	NodeID string `json:"nodeID"`
}

// ClusterAnnounceRequest is POST /cluster/announce.
type ClusterAnnounceRequest struct {
	Hostname string `json:"hostname"`
}

// Slot migration (CLUSTER MIGRATION, atomic: clients see no ASK).
//
//	POST /cluster/migration/import  -> body ClusterSlotsRequest -> 200 ClusterImportResponse
//	                                   (on the destination master; every range from one source)
//	GET  /cluster/migration/status?id=<task> -> 200 ClusterMigrationTask | 404 (no such task)

// ClusterImportResponse is the engine's task ID for an import.
type ClusterImportResponse struct {
	TaskID string `json:"taskID"`
}

// Migration task states.
const (
	MigrationInProgress = "in_progress"
	MigrationCompleted  = "completed"
	MigrationFailed     = "failed"
	MigrationCanceled   = "canceled"
)

// ClusterMigrationTask is one task of CLUSTER MIGRATION STATUS.
type ClusterMigrationTask struct {
	ID        string `json:"id"`
	Slots     string `json:"slots,omitempty"`
	State     string `json:"state"`
	LastError string `json:"lastError,omitempty"`
	Retries   int64  `json:"retries,omitempty"`
}
