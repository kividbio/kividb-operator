package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func TestPreStopReturnsAtOnceUnlessAMasterWithReplicas(t *testing.T) {
	for _, info := range []string{
		"# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\n",
		"# Replication\r\nrole:master\r\nconnected_slaves:0\r\n",
	} {
		rr := newRespRecorder(t, func([]string) string { return bulk(info) })
		s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), PreStopTimeout: time.Minute}}
		w := httptest.NewRecorder()
		start := time.Now()
		s.handlePreStop(w, httptest.NewRequest(http.MethodGet, "/prestop", nil))
		if w.Code != http.StatusOK || time.Since(start) > time.Second {
			t.Fatalf("%q: /prestop = %d after %s, want 200 at once", info, w.Code, time.Since(start))
		}
	}
}

// A master with replicas is held until it has become a replica (the
// operator handed its role over), or until the timeout.
func TestPreStopHoldsAMasterUntilTheRoleMoves(t *testing.T) {
	oldPoll := preStopPoll
	preStopPoll = 10 * time.Millisecond
	t.Cleanup(func() { preStopPoll = oldPoll })

	var asked atomic.Int32
	rr := newRespRecorder(t, func(cmd []string) string {
		if asked.Add(1) < 5 {
			return bulk("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n")
		}
		return bulk("# Replication\r\nrole:slave\r\n")
	})
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), PreStopTimeout: time.Minute}}
	w := httptest.NewRecorder()
	s.handlePreStop(w, httptest.NewRequest(http.MethodGet, "/prestop", nil))
	if w.Code != http.StatusOK || asked.Load() != 5 {
		t.Fatalf("/prestop = %d after %d looks, want 200 after 5", w.Code, asked.Load())
	}

	// Never handed over: let go after the timeout.
	rr = newRespRecorder(t, func([]string) string {
		return bulk("# Replication\r\nrole:master\r\nconnected_slaves:1\r\n")
	})
	s = &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), PreStopTimeout: 100 * time.Millisecond}}
	start := time.Now()
	w = httptest.NewRecorder()
	s.handlePreStop(w, httptest.NewRequest(http.MethodGet, "/prestop", nil))
	if took := time.Since(start); w.Code != http.StatusOK || took < 100*time.Millisecond || took > 2*time.Second {
		t.Fatalf("/prestop = %d after %s, want 200 after the 100ms timeout", w.Code, took)
	}
}
