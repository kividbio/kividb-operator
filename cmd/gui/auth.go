package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// authConfig is loaded once at startup from env. When Username is empty,
// auth is disabled and the GUI is a read-only dashboard: every request
// that is not a GET/HEAD -- the console, creating, restarting, scaling,
// promoting, snapshots, deleting -- is refused. When set, every request
// except /healthz, the login page and the static assets needs either a
// session (from signing in on /login) or Basic auth (for scripts).
type authConfig struct {
	Username string
	Password string
	// key signs session cookies: random, made at startup. Nothing derived
	// from the password goes into a cookie, so a stolen cookie gives no way
	// to guess the password offline. Changing the password restarts the
	// GUI, which makes a new key and so ends every session; so does any
	// other restart.
	key     []byte
	limiter *loginLimiter // nil: no limit (tests)
}

func newAuthConfig(username, password string) authConfig {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("session key: %v", err))
	}
	return authConfig{Username: username, Password: password, key: key, limiter: newLoginLimiter()}
}

func loadAuthConfig() authConfig {
	return newAuthConfig(os.Getenv("GUI_AUTH_USERNAME"), os.Getenv("GUI_AUTH_PASSWORD"))
}

func (a authConfig) enabled() bool {
	return a.Username != "" && a.Password != ""
}

func isReadOnly(r *http.Request) bool {
	return r.Method == http.MethodGet || r.Method == http.MethodHead
}

// sameOrigin reports whether a state-changing request was made by this
// GUI's own pages. The session cookie (SameSite=Lax) and Basic-auth
// credentials are attached by the browser to requests for this host,
// including some a different site makes the browser send, and the JSON
// handlers do not care what Content-Type the body claims -- so without
// this check a page the operator visits while signed in could restart,
// scale or delete clusters. Browsers always send Origin on cross-origin
// writes; a request with none (curl, scripts) is not something a
// third-party page can produce.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

// ---------- sessions ----------

const (
	sessionCookie = "kividb_gui_session"
	sessionTTL    = 12 * time.Hour
)

// now is the clock sessions are checked against; tests replace it.
var now = time.Now

func (a authConfig) sign(payload string) string {
	m := hmac.New(sha256.New, a.key)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// newSession is a cookie value: user, expiry (unix seconds), signature.
func (a authConfig) newSession(expires time.Time) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(a.Username)) + "." + strconv.FormatInt(expires.Unix(), 10)
	return payload + "." + a.sign(payload)
}

func (a authConfig) validSession(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil || len(a.key) == 0 {
		return false
	}
	i := strings.LastIndexByte(c.Value, '.')
	if i < 0 {
		return false
	}
	payload, sig := c.Value[:i], c.Value[i+1:]
	if !hmac.Equal([]byte(sig), []byte(a.sign(payload))) {
		return false
	}
	user, exp, ok := strings.Cut(payload, ".")
	if !ok {
		return false
	}
	name, err := base64.RawURLEncoding.DecodeString(user)
	if err != nil || string(name) != a.Username {
		return false
	}
	expires, err := strconv.ParseInt(exp, 10, 64)
	return err == nil && now().Unix() < expires
}

func (a authConfig) validBasic(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	return ok && a.credentialsMatch(user, pass)
}

func (a authConfig) credentialsMatch(user, pass string) bool {
	u := subtle.ConstantTimeCompare([]byte(user), []byte(a.Username))
	p := subtle.ConstantTimeCompare([]byte(pass), []byte(a.Password))
	return u&p == 1
}

// isSecure: whether the browser reached the GUI over HTTPS, directly or
// through a TLS-terminating proxy or ingress.
func isSecure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   isSecure(r),
		SameSite: http.SameSiteLaxMode,
	})
}

// ---------- login attempts ----------

const (
	loginMaxFailures = 10
	loginWindow      = 5 * time.Minute
	loginFailDelay   = 400 * time.Millisecond
)

// loginLimiter refuses sign-ins from an address after loginMaxFailures
// failures in loginWindow, until the window has passed.
type loginLimiter struct {
	mu   sync.Mutex
	seen map[string]*loginFailures
}

type loginFailures struct {
	count int
	first time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{seen: map[string]*loginFailures{}}
}

func (l *loginLimiter) blocked(addr string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.seen[addr]
	if f == nil {
		return false
	}
	if now().Sub(f.first) > loginWindow {
		delete(l.seen, addr)
		return false
	}
	return f.count >= loginMaxFailures
}

func (l *loginLimiter) fail(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.seen[addr]
	if f == nil || now().Sub(f.first) > loginWindow {
		f = &loginFailures{first: now()}
		l.seen[addr] = f
	}
	f.count++
}

func (l *loginLimiter) succeed(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.seen, addr)
}

func clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// safeNext keeps the after-login redirect on this GUI: a path, never
// another site ("//evil.example", "https://…", "/\\evil.example").
func safeNext(next string) string {
	if next == "" || next[0] != '/' || (len(next) > 1 && (next[1] == '/' || next[1] == '\\')) {
		return "/"
	}
	if strings.ContainsRune(next, '\\') {
		return "/"
	}
	// Browsers drop tabs and newlines from a URL: "/<tab>/evil.example"
	// would become "//evil.example".
	for _, c := range next {
		if c < 0x20 || c == 0x7f {
			return "/"
		}
	}
	if strings.HasPrefix(next, "/login") {
		return "/"
	}
	return next
}

// ---------- handlers ----------

func (a authConfig) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	// Nothing to sign in to, or already signed in. (The page's form carries
	// ?next= through to the sign-in itself; nothing sends a signed-in user
	// here.)
	if !a.enabled() || a.validSession(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	serveEmbeddedHTML(w, "static/login.html")
}

func (a authConfig) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !a.enabled() {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	next := safeNext(r.PostForm.Get("next"))
	back := func(code string) {
		http.Redirect(w, r, "/login?error="+code+"&next="+url.QueryEscape(next), http.StatusSeeOther)
	}
	addr := clientAddr(r)
	if a.limiter.blocked(addr) {
		back("locked")
		return
	}
	if !a.credentialsMatch(r.PostForm.Get("username"), r.PostForm.Get("password")) {
		a.limiter.fail(addr)
		time.Sleep(loginFailDelay)
		back("invalid")
		return
	}
	a.limiter.succeed(addr)
	setSessionCookie(w, r, a.newSession(now().Add(sessionTTL)), int(sessionTTL.Seconds()))
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (a authConfig) handleLogout(w http.ResponseWriter, r *http.Request) {
	setSessionCookie(w, r, "", -1)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// public: reachable without signing in. The static assets are the same
// for everyone and hold nothing about the clusters.
func public(path string) bool {
	return path == "/healthz" || path == "/login" || path == "/logout" || strings.HasPrefix(path, "/static/")
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
			if !isReadOnly(r) && r.URL.Path != "/login" {
				writeJSONError(w, http.StatusForbidden, fmt.Errorf("the GUI is read-only until authentication is configured (set GUI_AUTH_USERNAME/GUI_AUTH_PASSWORD, or gui.auth.existingSecret in the Helm chart)"))
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if public(r.URL.Path) || a.validSession(r) || a.validBasic(r) {
			next.ServeHTTP(w, r)
			return
		}
		// Signed out. No WWW-Authenticate header: that would make the
		// browser show its own login box instead of the GUI's page.
		if strings.HasPrefix(r.URL.Path, "/api/") || !isReadOnly(r) {
			writeJSONError(w, http.StatusUnauthorized, fmt.Errorf("sign in at /login, or use Basic auth"))
			return
		}
		http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
	})
}
