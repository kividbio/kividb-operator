package main

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// The engine waits out the writes in progress before it answers CLIENT
// PAUSE, up to 5 s (kividbio/kividb#629), as long as the agent waits for any
// command: a request may give its command a deadline of its own, capped so
// that a bad one cannot hold a handler for long (#81).
func TestExecGivesTheCommandTheDeadlineAskedFor(t *testing.T) {
	defer func(r, m time.Duration) { respTimeout, maxExecTimeout = r, m }(respTimeout, maxExecTimeout)
	respTimeout, maxExecTimeout = 100*time.Millisecond, 400*time.Millisecond
	slow := 250 * time.Millisecond
	rr := newRespRecorder(t, func(cmd []string) string {
		if strings.EqualFold(cmd[0], "CLIENT") {
			time.Sleep(slow)
		}
		return "+OK\r\n"
	})
	h := (&server{cfg: agentConfig{AgentToken: "s3cret", KividbAddr: rr.ln.Addr().String()}}).routes()
	exec := func(timeoutMs int64) int {
		body, _ := json.Marshal(agentapi.ExecRequest{Args: []string{"CLIENT", "PAUSE", "1000", "WRITE"}, TimeoutMs: timeoutMs})
		req := httptest.NewRequest(http.MethodPost, "/exec", bytes.NewReader(body))
		agentapi.SetToken(req, "s3cret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	for _, tc := range []struct {
		name      string
		timeoutMs int64
		want      int
	}{
		{"none: respTimeout", 0, http.StatusInternalServerError},
		{"longer than the command takes", 300, http.StatusOK},
		{"shorter than the command takes", 150, http.StatusInternalServerError},
		{"past the cap: the cap", 60_000, http.StatusOK},
		{"far past it: still the cap", math.MaxInt64, http.StatusOK},
	} {
		if got := exec(tc.timeoutMs); got != tc.want {
			t.Errorf("%s (timeoutMs %d): status %d, want %d", tc.name, tc.timeoutMs, got, tc.want)
		}
	}

	maxExecTimeout = 200 * time.Millisecond
	if got := exec(60_000); got != http.StatusInternalServerError {
		t.Errorf("timeoutMs past a cap shorter than the command: status %d, want %d", got, http.StatusInternalServerError)
	}
}
