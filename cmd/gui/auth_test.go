package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthMiddleware(t *testing.T) {
	t.Parallel()

	withAuth := authConfig{Username: "admin", Password: "s3cret"}
	noAuth := authConfig{}

	tests := []struct {
		name   string
		auth   authConfig
		method string
		path   string
		creds  bool
		origin string
		want   int
	}{
		{"no auth configured: reads are open", noAuth, http.MethodGet, "/api/clusters", false, "", http.StatusOK},
		{"no auth configured: exec is refused", noAuth, http.MethodPost, "/api/clusters/ns/c1/exec", false, "", http.StatusForbidden},
		{"no auth configured: delete is refused", noAuth, http.MethodDelete, "/api/clusters/ns/c1", false, "", http.StatusForbidden},
		{"healthz never needs auth", withAuth, http.MethodGet, "/healthz", false, "", http.StatusOK},
		{"read without credentials", withAuth, http.MethodGet, "/api/clusters", false, "", http.StatusUnauthorized},
		{"read with credentials", withAuth, http.MethodGet, "/api/clusters", true, "", http.StatusOK},
		{"write with credentials, no Origin (curl)", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "", http.StatusOK},
		{"write with credentials from the GUI's own page", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "http://gui.example:8090", http.StatusOK},
		{"write with credentials from another site", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "https://evil.example", http.StatusForbidden},
		{"delete with credentials from another site", withAuth, http.MethodDelete, "/api/clusters/ns/c1", true, "https://evil.example", http.StatusForbidden},
		{"read from another site is not a write", withAuth, http.MethodGet, "/api/clusters", true, "https://evil.example", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := tt.auth.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(tt.method, "http://gui.example:8090"+tt.path, strings.NewReader("{}"))
			if tt.creds {
				req.SetBasicAuth("admin", "s3cret")
			}
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
