package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

func TestRunsToPrune(t *testing.T) {
	p := "backups/cache/"
	run := func(ts string, files ...string) []string {
		var out []string
		for _, f := range files {
			out = append(out, p+ts+"/"+f)
		}
		return out
	}
	var keys []string
	keys = append(keys, run("20261001T000000Z", "shard-0-a.tar.gz", "shard-1-b.tar.gz", "manifest.json")...)
	keys = append(keys, run("20261002T000000Z", "shard-0-a.tar.gz")...) // incomplete, old
	keys = append(keys, run("20261003T000000Z", "shard-0-a.tar.gz", "shard-1-b.tar.gz", "manifest.json")...)
	keys = append(keys, run("20261004T000000Z", "shard-0-a.tar.gz", "shard-1-b.tar.gz", "manifest.json")...)
	keys = append(keys, run("20261005T000000Z", "shard-0-a.tar.gz")...) // under way
	keys = append(keys, p+"cache-0-20260901T000000Z.tar.gz")            // not a run
	keys = append(keys, p+"notarun/shard-0-a.tar.gz")

	doomed, runs := runsToPrune(p, keys, 2)
	if runs != 2 {
		t.Fatalf("pruned %d runs: %v", runs, doomed)
	}
	want := append(run("20261001T000000Z", "manifest.json", "shard-0-a.tar.gz", "shard-1-b.tar.gz"), run("20261002T000000Z", "shard-0-a.tar.gz")...)
	if fmt.Sprint(doomed) != fmt.Sprint(want) {
		t.Fatalf("doomed\n%v\nwant\n%v", doomed, want)
	}
	if d, n := runsToPrune(p, keys, 1); n != 3 || len(d) != 7 {
		t.Fatalf("keep 1: %d runs %v", n, d)
	}
	if d, _ := runsToPrune(p, keys, 3); d != nil {
		t.Fatalf("keep 3: %v", d)
	}
}

// fakeShardAgent is one pod's agent for runShardedBackup.
type fakeShardAgent struct {
	pod, id, master string
	role            agentapi.Role
	slots           []string
	fail            string

	mu       sync.Mutex
	backups  []agentapi.BackupRequest
	manifest *agentapi.BackupManifest
}

func (a *fakeShardAgent) serve(t *testing.T, nodes func() []agentapi.ClusterNode) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !agentapi.TokenMatches(r, "tok") {
			writeError(w, http.StatusUnauthorized, fmt.Errorf("token"))
			return
		}
		switch r.URL.Path {
		case "/status":
			writeJSON(w, http.StatusOK, agentapi.StatusResponse{Role: a.role})
		case "/cluster/status":
			ns := nodes()
			for i := range ns {
				if ns[i].ID == a.id {
					ns[i].Flags = append([]string{"myself"}, ns[i].Flags...)
				}
			}
			writeJSON(w, http.StatusOK, agentapi.ClusterStatusResponse{Enabled: true, MyID: a.id, Nodes: ns})
		case "/backup":
			var req agentapi.BackupRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if a.fail != "" {
				writeError(w, http.StatusInternalServerError, fmt.Errorf("%s", a.fail))
				return
			}
			a.mu.Lock()
			a.backups = append(a.backups, req)
			a.mu.Unlock()
			writeJSON(w, http.StatusOK, agentapi.BackupResponse{
				ObjectKey:  fmt.Sprintf("cache/%s/shard-%d-%s.tar.gz", req.Run, req.Shard, a.pod),
				SizeBytes:  100 + int64(req.Shard),
				SourcePod:  a.pod,
				SourceRole: a.role,
			})
		case "/backup/manifest":
			var m agentapi.BackupManifest
			_ = json.NewDecoder(r.Body).Decode(&m)
			a.mu.Lock()
			a.manifest = &m
			a.mu.Unlock()
			writeJSON(w, http.StatusOK, agentapi.ManifestResponse{ObjectKey: "cache/" + m.Run + "/manifest.json", PrunedRuns: 1})
		default:
			http.NotFound(w, r)
		}
	}))
}

func shardedFixture(t *testing.T) ([]*fakeShardAgent, [][]string) {
	// Shard 0: cache-0-0 master. Shard 1: cache-1-1 is master after a
	// failover, cache-1-0 its replica.
	agents := []*fakeShardAgent{
		{pod: "cache-0-0", id: strings.Repeat("a", 40), role: agentapi.RoleMaster, slots: []string{"0-8191"}},
		{pod: "cache-0-1", id: strings.Repeat("b", 40), role: agentapi.RoleReplica, master: strings.Repeat("a", 40)},
		{pod: "cache-1-0", id: strings.Repeat("c", 40), role: agentapi.RoleReplica, master: strings.Repeat("d", 40)},
		{pod: "cache-1-1", id: strings.Repeat("d", 40), role: agentapi.RoleMaster, slots: []string{"8192-16383"}},
	}
	nodes := func() []agentapi.ClusterNode {
		var out []agentapi.ClusterNode
		for _, a := range agents {
			flag := "master"
			if a.master != "" {
				flag = "slave"
			}
			out = append(out, agentapi.ClusterNode{ID: a.id, Flags: []string{flag}, MasterID: a.master, Slots: a.slots})
		}
		return out
	}
	addrs := make([]string, len(agents))
	for i, a := range agents {
		srv := a.serve(t, nodes)
		t.Cleanup(srv.Close)
		addrs[i] = strings.TrimPrefix(srv.URL, "http://")
	}
	return agents, [][]string{{addrs[0], addrs[1]}, {addrs[2], addrs[3]}}
}

func readResult(t *testing.T, path string) agentapi.BackupResult {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r agentapi.BackupResult
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestShardedBackupFromReplicas(t *testing.T) {
	term := filepath.Join(t.TempDir(), "termination-log")
	t.Setenv("TERMINATION_MESSAGE_PATH", term)
	agents, shards := shardedFixture(t)

	if err := runShardedBackup(shards, agentapi.RoleReplica, "cache", "tok", time.Minute); err != nil {
		t.Fatal(err)
	}
	// One backup per shard, from its replica, all under one run.
	b1, b2 := agents[1].backups, agents[2].backups
	if len(b1) != 1 || len(b2) != 1 || b1[0].Shard != 0 || b2[0].Shard != 1 || b1[0].Run != b2[0].Run || agentapi.CheckRun(b1[0].Run) != nil {
		t.Fatalf("backups: %+v %+v", b1, b2)
	}
	if len(agents[0].backups)+len(agents[3].backups) != 0 {
		t.Fatal("a master was backed up")
	}
	m := agents[1].manifest
	if m == nil || m.Cluster != "cache" || len(m.Shards) != 2 {
		t.Fatalf("manifest %+v", m)
	}
	if s := m.Shards[1]; s.SourcePod != "cache-1-0" || s.MasterNodeID != strings.Repeat("d", 40) || fmt.Sprint(s.Slots) != "[8192-16383]" || s.SourceRole != agentapi.RoleReplica {
		t.Fatalf("shard 1 in the manifest: %+v", s)
	}
	if s := m.Shards[0]; fmt.Sprint(s.Slots) != "[0-8191]" || !strings.HasSuffix(s.ObjectKey, "/shard-0-cache-0-1.tar.gz") {
		t.Fatalf("shard 0 in the manifest: %+v", s)
	}
	res := readResult(t, term)
	if !res.Success || res.ObjectKey != "cache/"+b1[0].Run+"/manifest.json" || res.SizeBytes != 201 || len(res.Shards) != 2 || res.Shards[1].SourcePod != "cache-1-0" {
		t.Fatalf("result %+v", res)
	}
}

func TestShardedBackupIncompleteWritesNoManifest(t *testing.T) {
	term := filepath.Join(t.TempDir(), "termination-log")
	t.Setenv("TERMINATION_MESSAGE_PATH", term)
	agents, shards := shardedFixture(t)
	agents[3].fail = "BGSAVE: disk full"

	if err := runShardedBackup(shards, agentapi.RoleMaster, "cache", "tok", time.Minute); err == nil {
		t.Fatal("no error with a shard failing")
	}
	if agents[0].manifest != nil {
		t.Fatal("a manifest for an incomplete run")
	}
	res := readResult(t, term)
	if res.Success || !strings.Contains(res.Error, "shard 1") || !strings.Contains(res.Shards[1].Error, "disk full") || res.Shards[0].SourcePod != "cache-0-0" {
		t.Fatalf("result %+v", res)
	}
}

func TestFitResult(t *testing.T) {
	r := agentapi.BackupResult{Error: strings.Repeat("x", 1024)}
	for i := 0; i < 24; i++ {
		r.Shards = append(r.Shards, agentapi.BackupShardResult{
			Index: int32(i), SourcePod: fmt.Sprintf("a-forty-character-cluster-name-goes-here-%d-2", i),
			SizeBytes: 1 << 40, Error: strings.Repeat("e", 300),
		})
	}
	got := fitResult(r)
	b, _ := json.Marshal(got)
	if len(b) > terminationMessageLimit || len(got.Shards) != 24 {
		t.Fatalf("%d bytes, %d shards", len(b), len(got.Shards))
	}
}

func TestBackupRequestChecks(t *testing.T) {
	h := (&server{cfg: agentConfig{AgentToken: "tok", KividbAddr: "127.0.0.1:1", ClusterName: "cache"}}).routes()
	do := func(path, body string) int {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		agentapi.SetToken(req, "tok")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, body := range []string{`{"run":"../../etc"}`, `{"run":"20261025T101500Z","shard":-1}`, `{"run":5}`} {
		if code := do("/backup", body); code != http.StatusBadRequest {
			t.Errorf("/backup %s: %d", body, code)
		}
	}
	good := `{"cluster":"cache","run":"20261025T101500Z","shards":[{"index":0,"objectKey":"cache/20261025T101500Z/shard-0-cache-0-0.tar.gz"}]}`
	for _, body := range []string{
		`{"cluster":"cache","run":"x","shards":[]}`,
		`{"cluster":"other","run":"20261025T101500Z","shards":[{"index":0,"objectKey":"other/20261025T101500Z/a"}]}`,
		`{"cluster":"cache","run":"20261025T101500Z","shards":[{"index":0,"objectKey":"cache/20261024T000000Z/shard-0.tar.gz"}]}`,
		`{"cluster":"cache","run":"20261025T101500Z","shards":[{"index":1,"objectKey":"cache/20261025T101500Z/shard-1.tar.gz"}]}`,
		`{"cluster":"cache","run":"20261025T101500Z","shards":[{"index":0,"objectKey":"cache/20261025T101500Z/x/../../y"}]}`,
	} {
		if code := do("/backup/manifest", body); code != http.StatusBadRequest {
			t.Errorf("/backup/manifest %s: %d", body, code)
		}
	}
	// A valid manifest gets past the checks, to the missing S3 config.
	if code := do("/backup/manifest", good); code != http.StatusInternalServerError {
		t.Errorf("valid manifest: %d", code)
	}
}
