package main

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

// authConfig is loaded once at startup from env. When Username is empty,
// auth is disabled and the GUI is a read-only dashboard: every request
// that is not a GET/HEAD -- the RESP explorer, restarts, scaling, promote,
// snapshot, delete -- is refused. When set, every request except /healthz
// must pass Basic auth.
type authConfig struct {
	Username string
	Password string
}

func loadAuthConfig() authConfig {
	return authConfig{
		Username: os.Getenv("GUI_AUTH_USERNAME"),
		Password: os.Getenv("GUI_AUTH_PASSWORD"),
	}
}

func (a authConfig) enabled() bool {
	return a.Username != "" && a.Password != ""
}

func isReadOnly(r *http.Request) bool {
	return r.Method == http.MethodGet || r.Method == http.MethodHead
}

// sameOrigin reports whether a state-changing request was made by this
// GUI's own pages. Basic-auth credentials are attached by the browser to
// any request for this host, including one a different site makes the
// browser send, and the JSON handlers do not care what Content-Type the
// body claims -- so without this check any page the operator visits while
// logged in could restart, scale or delete clusters. Browsers always send
// Origin on cross-origin writes; a request with none (curl, scripts) is
// not something a third-party page can produce.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

func (a authConfig) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		if !isReadOnly(r) && !sameOrigin(r) {
			writeJSONError(w, http.StatusForbidden, fmt.Errorf("cross-origin request refused"))
			return
		}
		if !a.enabled() {
			if !isReadOnly(r) {
				writeJSONError(w, http.StatusForbidden, fmt.Errorf("the GUI is read-only until authentication is configured (set GUI_AUTH_USERNAME/GUI_AUTH_PASSWORD, or gui.auth.existingSecret in the Helm chart)"))
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(user), []byte(a.Username)) != 1 ||
			subtle.ConstantTimeCompare([]byte(pass), []byte(a.Password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="kividb-operator-gui"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
