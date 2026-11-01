package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// The cluster status carries what the operator needs to see a shard master
// that restarted with less than its replica holds: the engine's run ID and
// uptime, its key count, and whether it is still loading.
func TestClusterStatusReportsRestartAndKeys(t *testing.T) {
	const id = "514b2c79aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rr := newRespRecorder(t, func(cmd []string) string {
		switch strings.ToUpper(strings.Join(cmd, " ")) {
		case "CLUSTER INFO":
			return bulk("cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:2\r\n")
		case "CLUSTER MYID":
			return bulk(id)
		case "CLUSTER NODES":
			return bulk(id + " 10.0.0.1:6380@16380 myself,master - 0 0 1 connected 0-16383\n")
		case "CONFIG GET CLUSTER-ANNOUNCE-HOSTNAME":
			return "*2\r\n" + bulk("cluster-announce-hostname") + bulk("s-0-0.s-0-headless.default.svc")
		case "INFO REPLICATION":
			return bulk("role:master\r\nmaster_repl_offset:38516\r\n")
		case "INFO SERVER":
			return bulk("run_id:edf3a91aa4a65b42e1c934a5f2e6cab04e3d000c\r\nuptime_in_seconds:1\r\n")
		case "INFO KEYSPACE":
			return bulk("db0:keys=987,expires=0,avg_ttl=0\r\n")
		case "INFO PERSISTENCE":
			return bulk("loading:0\r\naof_enabled:0\r\n")
		}
		return "-ERR unknown\r\n"
	})
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), AgentToken: "tok"}}
	req := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	agentapi.SetToken(req, "tok")
	rec := httptest.NewRecorder()
	s.routes().ServeHTTP(rec, req)
	var st agentapi.ClusterStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("%d %s: %v", rec.Code, rec.Body, err)
	}
	if st.MyID != id || st.RunID != "edf3a91aa4a65b42e1c934a5f2e6cab04e3d000c" || st.UptimeSeconds == nil || *st.UptimeSeconds != 1 ||
		!st.KeyCountKnown || st.KeyCount != 987 || st.AofEnabled || st.Loading || st.ReplicationOffset != 38516 {
		t.Fatalf("%+v", st)
	}
}

func TestClusterFailoverArgs(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{"", "CLUSTER FAILOVER"}, // the operator before 0.7.0 and the GUI send no body
		{"{}", "CLUSTER FAILOVER"},
		{`{"mode":"force"}`, "CLUSTER FAILOVER FORCE"},
		{`{"mode":"takeover"}`, "CLUSTER FAILOVER TAKEOVER"},
	}
	for _, tc := range cases {
		args, err := clusterFailoverArgs(strings.NewReader(tc.body))
		if err != nil || strings.Join(args, " ") != tc.want {
			t.Errorf("%q: %v %v, want %s", tc.body, args, err, tc.want)
		}
	}
	for _, bad := range []string{`{"mode":"FORCE ; FLUSHALL"}`, `{"mode":"reset"}`, `not json`} {
		if args, err := clusterFailoverArgs(strings.NewReader(bad)); err == nil {
			t.Errorf("%q accepted: %v", bad, args)
		}
	}
	if args, err := clusterFailoverArgs(nil); err != nil || len(args) != 2 {
		t.Errorf("nil body: %v %v", args, err)
	}
}
