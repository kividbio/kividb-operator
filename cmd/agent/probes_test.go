package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLivezPassesOnAnyReply(t *testing.T) {
	for _, reply := range []string{"+PONG\r\n", "-NOAUTH Authentication required.\r\n", "-LOADING KiviDB is loading the dataset in memory\r\n", "-BUSY a script is running\r\n"} {
		rr := newRespRecorder(t, func([]string) string { return reply })
		s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String()}}
		w := httptest.NewRecorder()
		s.handleLivez(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
		if w.Code != http.StatusOK {
			t.Errorf("reply %q: /livez = %d, want 200", strings.TrimSpace(reply), w.Code)
		}
	}
}

// A hung engine accepts the connection (the kernel does that) and never
// answers: the probe that the old TCP check passed must fail.
func TestLivezFailsOnAHungEngine(t *testing.T) {
	old := livenessTimeout
	livenessTimeout = 200 * time.Millisecond
	t.Cleanup(func() { livenessTimeout = old })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close() // held open, never answered
		}
	}()

	s := &server{cfg: agentConfig{KividbAddr: ln.Addr().String()}}
	w := httptest.NewRecorder()
	start := time.Now()
	s.handleLivez(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/livez = %d, want 503", w.Code)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s, want about the liveness timeout", took)
	}
}

func TestLivezFailsWhenNothingListens(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	s := &server{cfg: agentConfig{KividbAddr: addr}}
	w := httptest.NewRecorder()
	s.handleLivez(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/livez = %d, want 503", w.Code)
	}
}

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
