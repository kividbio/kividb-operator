package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		{http.MethodPost, "/exec", true},
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
