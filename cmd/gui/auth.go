package main

import (
	"crypto/subtle"
	"net/http"
	"os"
)

// authConfig is loaded once at startup from env. When Username is empty,
// auth is disabled (local `go run` convenience). When set, every request
// except /healthz must pass Basic auth. Exec and mutating routes refuse
// to register usefully without auth in production charts (values require
// a Secret).
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

func (a authConfig) middleware(next http.Handler) http.Handler {
	if !a.enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
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
