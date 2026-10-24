package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

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
		{"no auth configured: create is refused", noAuth, http.MethodPost, "/api/clusters", false, "", http.StatusForbidden},
		{"healthz never needs auth", withAuth, http.MethodGet, "/healthz", false, "", http.StatusOK},
		{"the login page needs no auth", withAuth, http.MethodGet, "/login", false, "", http.StatusOK},
		{"static assets need no auth", withAuth, http.MethodGet, "/static/style.css", false, "", http.StatusOK},
		{"API read signed out", withAuth, http.MethodGet, "/api/clusters", false, "", http.StatusUnauthorized},
		{"page signed out goes to the login page", withAuth, http.MethodGet, "/clusters/ns/c1", false, "", http.StatusSeeOther},
		{"write signed out", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", false, "", http.StatusUnauthorized},
		{"read with Basic auth", withAuth, http.MethodGet, "/api/clusters", true, "", http.StatusOK},
		{"write with Basic auth, no Origin (curl)", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "", http.StatusOK},
		{"write from the GUI's own page", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "http://gui.example:8090", http.StatusOK},
		{"write from another site", withAuth, http.MethodPost, "/api/clusters/ns/c1/exec", true, "https://evil.example", http.StatusForbidden},
		{"delete from another site", withAuth, http.MethodDelete, "/api/clusters/ns/c1", true, "https://evil.example", http.StatusForbidden},
		{"login posted from another site", withAuth, http.MethodPost, "/login", false, "https://evil.example", http.StatusForbidden},
		{"read from another site is not a write", withAuth, http.MethodGet, "/api/clusters", true, "https://evil.example", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := tt.auth.middleware(okHandler())
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
			// The GUI has its own login page: never ask the browser for
			// its built-in login box.
			if rec.Header().Get("WWW-Authenticate") != "" {
				t.Fatalf("WWW-Authenticate set: %q", rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestSignedOutPageRedirectKeepsWhereYouWere(t *testing.T) {
	h := authConfig{Username: "admin", Password: "s3cret"}.middleware(okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://gui.example/clusters/ns/c1?x=1", nil))
	want := "/login?next=" + url.QueryEscape("/clusters/ns/c1?x=1")
	if got := rec.Header().Get("Location"); got != want {
		t.Fatalf("Location %q, want %q", got, want)
	}
}

// postLogin signs in through the real handler and returns the response.
func postLogin(a authConfig, user, pass, next, remote string) *httptest.ResponseRecorder {
	form := url.Values{"username": {user}, "password": {pass}, "next": {next}}
	req := httptest.NewRequest(http.MethodPost, "http://gui.example/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	a.handleLogin(rec, req)
	return rec
}

func withCookie(c *http.Cookie) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://gui.example/", nil)
	r.AddCookie(c)
	return r
}

func TestLoginSessionCookie(t *testing.T) {
	a := newAuthConfig("admin", "s3cret")

	rec := postLogin(a, "admin", "s3cret", "/clusters/ns/c1", "10.0.0.1:5000")
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/clusters/ns/c1" {
		t.Fatalf("login: %d to %q", rec.Code, rec.Header().Get("Location"))
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != sessionCookie || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatalf("cookie: %+v", cookies)
	}
	session := cookies[0]

	h := a.middleware(okHandler())
	read := func(c *http.Cookie) int {
		req := httptest.NewRequest(http.MethodGet, "http://gui.example/api/clusters", nil)
		if c != nil {
			req.AddCookie(c)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := read(session); code != http.StatusOK {
		t.Fatalf("with the session: %d", code)
	}

	tampered := *session
	tampered.Value = strings.Replace(session.Value, ".", ".9", 1)
	if code := read(&tampered); code != http.StatusUnauthorized {
		t.Errorf("tampered cookie: %d", code)
	}
	// A new password means a restarted GUI, with a new key.
	if newAuthConfig("admin", "changed").validSession(withCookie(session)) {
		t.Error("a session outlived a password change")
	}
	if newAuthConfig("admin", "s3cret").validSession(withCookie(session)) {
		t.Error("a session outlived a restart (the key is per process)")
	}
	if strings.Contains(session.Value, "s3cret") {
		t.Error("the password is in the cookie")
	}
	expired := &http.Cookie{Name: sessionCookie, Value: a.newSession(time.Now().Add(-time.Minute))}
	if code := read(expired); code != http.StatusUnauthorized {
		t.Errorf("expired cookie: %d", code)
	}
}

func TestLoginRefusesWrongPasswordAndLimitsAttempts(t *testing.T) {
	a := newAuthConfig("admin", "s3cret")

	rec := postLogin(a, "admin", "wrong", "/", "10.0.0.2:1")
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "error=invalid") || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("wrong password: %q, cookies %v", loc, rec.Result().Cookies())
	}
	for i := 1; i < loginMaxFailures; i++ {
		a.limiter.fail("10.0.0.2")
	}
	// Locked now, even with the right password.
	rec = postLogin(a, "admin", "s3cret", "/", "10.0.0.2:1")
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "error=locked") || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("locked: %q", loc)
	}
	// Another address is not.
	rec = postLogin(a, "admin", "s3cret", "/", "10.0.0.3:1")
	if len(rec.Result().Cookies()) != 1 {
		t.Fatal("another address was locked out too")
	}
}

func TestSafeNext(t *testing.T) {
	cases := map[string]string{
		"":                       "/",
		"/clusters/ns/c1":        "/clusters/ns/c1",
		"/new?x=1#y":             "/new?x=1#y",
		"//evil.example":         "/",
		"/\\evil.example":        "/",
		"https://evil.example/":  "/",
		"javascript:alert(1)":    "/",
		"/login?next=/somewhere": "/",
		"/\t/evil.example":       "/",
		"/\n/evil.example":       "/",
		"/clusters/a\\b":         "/",
	}
	for in, want := range cases {
		if got := safeNext(in); got != want {
			t.Errorf("safeNext(%q) = %q, want %q", in, got, want)
		}
	}
}
