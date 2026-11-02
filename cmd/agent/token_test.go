package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

func TestAgentEndpointsNeedTheToken(t *testing.T) {
	cases := []struct {
		method, path string
		protected    bool
	}{
		{http.MethodGet, "/healthz", false},
		{http.MethodGet, "/status", true},
		{http.MethodPost, "/promote", true},
		{http.MethodPost, "/replicaof", true},
		{http.MethodPost, "/acl/reload", true},
		{http.MethodPost, "/backup", true},
		{http.MethodPost, "/backup/manifest", true},
		{http.MethodPost, "/cluster/failover", true},
		{http.MethodPost, "/cluster/migration/import", true},
		{http.MethodGet, "/cluster/migration/status", true},
		{http.MethodPost, "/exec", true},
		{http.MethodGet, "/cluster/status", true},
		{http.MethodPost, "/cluster/meet", true},
		{http.MethodPost, "/cluster/addslots", true},
		{http.MethodPost, "/cluster/replicate", true},
		{http.MethodPost, "/cluster/forget", true},
		{http.MethodPost, "/cluster/announce", true},
	}
	withToken := (&server{cfg: agentConfig{AgentToken: "s3cret", KividbAddr: "127.0.0.1:1"}}).routes()
	noToken := (&server{cfg: agentConfig{KividbAddr: "127.0.0.1:1"}}).routes()

	for _, tc := range cases {
		do := func(h http.Handler, token string) int {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
			agentapi.SetToken(req, token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			return rec.Code
		}
		if !tc.protected {
			if code := do(withToken, ""); code != http.StatusOK {
				t.Errorf("%s %s without a token: %d, want 200", tc.method, tc.path, code)
			}
			continue
		}
		if code := do(withToken, ""); code != http.StatusUnauthorized {
			t.Errorf("%s %s without a token: %d, want 401", tc.method, tc.path, code)
		}
		if code := do(withToken, "wrong"); code != http.StatusUnauthorized {
			t.Errorf("%s %s with a wrong token: %d, want 401", tc.method, tc.path, code)
		}
		// With the token the handler runs (and fails to reach kividb: not 401).
		if code := do(withToken, "s3cret"); code == http.StatusUnauthorized || code == http.StatusServiceUnavailable && tc.path == "/healthz" {
			t.Errorf("%s %s with the token: %d", tc.method, tc.path, code)
		}
		// An agent with no token refuses rather than serving everyone.
		if code := do(noToken, "anything"); code != http.StatusServiceUnavailable {
			t.Errorf("%s %s on an agent with no token: %d, want 503", tc.method, tc.path, code)
		}
	}
}

// The agent container mounts the cluster's auth Secret next to the ACL
// file, and the kubelet updates that file when the operator's token
// changes; AGENT_TOKEN keeps the value the container started with.
func TestAgentTakesTheMountedTokenWhenItChanges(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, agentapi.TokenSecretKey)
	if err := os.WriteFile(tokenFile, []byte("old-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := (&server{cfg: agentConfig{
		AgentToken: "old-token",
		AclFile:    filepath.Join(dir, "users.acl"),
		KividbAddr: "127.0.0.1:1",
	}}).routes()
	status := func(token string) int {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		agentapi.SetToken(req, token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := status("old-token"); code == http.StatusUnauthorized {
		t.Fatalf("the current token: %d", code)
	}

	if err := os.WriteFile(tokenFile, []byte("new-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := status("new-token"); code == http.StatusUnauthorized {
		t.Errorf("the new token in the mounted Secret: %d, want it accepted", code)
	}
	if code := status("old-token"); code != http.StatusUnauthorized {
		t.Errorf("the old token after the mounted Secret changed: %d, want 401", code)
	}
}

func TestAgentFallsBackToTheTokenEnv(t *testing.T) {
	cases := []struct {
		name     string
		file     string // "-": no file
		env      string
		token    string
		wantCode int // 0: anything but 401 and 503
	}{
		{name: "no file, env token", file: "-", env: "env-token", token: "env-token"},
		{name: "no file, wrong token", file: "-", env: "env-token", token: "other", wantCode: http.StatusUnauthorized},
		{name: "empty file, env token", file: " \n", env: "env-token", token: "env-token"},
		{name: "empty file, no env", file: "", token: "", wantCode: http.StatusServiceUnavailable},
		{name: "no file, no env", file: "-", token: "", wantCode: http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.file != "-" {
				if err := os.WriteFile(filepath.Join(dir, agentapi.TokenSecretKey), []byte(tc.file), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			h := (&server{cfg: agentConfig{
				AgentToken: tc.env,
				AclFile:    filepath.Join(dir, "users.acl"),
				KividbAddr: "127.0.0.1:1",
			}}).routes()
			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			agentapi.SetToken(req, tc.token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			switch {
			case tc.wantCode != 0 && rec.Code != tc.wantCode:
				t.Errorf("%d, want %d", rec.Code, tc.wantCode)
			case tc.wantCode == 0 && (rec.Code == http.StatusUnauthorized || rec.Code == http.StatusServiceUnavailable):
				t.Errorf("%d, want the handler to run", rec.Code)
			}
		})
	}
}

func fakeAgent(t *testing.T, role agentapi.Role, token string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !agentapi.TokenMatches(r, token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(agentapi.StatusResponse{Role: role})
	}))
}

func TestFindAgentPicksTheRole(t *testing.T) {
	replica := fakeAgent(t, agentapi.RoleReplica, "tok")
	defer replica.Close()
	master := fakeAgent(t, agentapi.RoleMaster, "tok")
	defer master.Close()
	host := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	targets := []string{"127.0.0.1:1", host(replica), host(master)} // the first is down

	got, err := findAgent(context.Background(), targets, agentapi.RoleMaster, "tok")
	if err != nil || got != host(master) {
		t.Fatalf("master: %q, %v", got, err)
	}
	got, err = findAgent(context.Background(), targets, agentapi.RoleReplica, "tok")
	if err != nil || got != host(replica) {
		t.Fatalf("replica: %q, %v", got, err)
	}
	if _, err := findAgent(context.Background(), targets, agentapi.RoleMaster, "wrong"); err == nil {
		t.Fatal("found an agent with the wrong token")
	}
}
