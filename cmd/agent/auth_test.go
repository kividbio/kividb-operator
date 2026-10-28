package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// respRecorder is a RESP server that records every command it is sent and
// answers each with reply(cmd).
type respRecorder struct {
	ln    net.Listener
	mu    sync.Mutex
	cmds  [][]string
	reply func(cmd []string) string
}

func newRespRecorder(t *testing.T, reply func(cmd []string) string) *respRecorder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rr := &respRecorder{ln: ln, reply: reply}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go rr.serve(conn)
		}
	}()
	return rr
}

func (rr *respRecorder) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		cmd, err := readCommand(r)
		if err != nil {
			return
		}
		rr.mu.Lock()
		rr.cmds = append(rr.cmds, cmd)
		rr.mu.Unlock()
		if _, err := io.WriteString(conn, rr.reply(cmd)); err != nil {
			return
		}
	}
}

func (rr *respRecorder) commands() [][]string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return append([][]string(nil), rr.cmds...)
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, fmt.Errorf("bad array header %q", line)
	}
	cmd := make([]string, n)
	for i := range cmd {
		if _, err := r.ReadString('\n'); err != nil { // $<len>
			return nil, err
		}
		arg, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		cmd[i] = strings.TrimRight(arg, "\r\n")
	}
	return cmd, nil
}

func TestDialAuthenticatesAsDefaultUser(t *testing.T) {
	rr := newRespRecorder(t, func([]string) string { return "+OK\r\n" })
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), AuthUsername: "default", AuthPassword: "pw"}}
	c, err := s.dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.Close()
	want := [][]string{{"AUTH", "default", "pw"}}
	if got := rr.commands(); !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %q, want %q", got, want)
	}
}

func TestSetMasterAuthSetsMasterUserAndPassword(t *testing.T) {
	// An engine that does not know masteruser still gets masterauth.
	rr := newRespRecorder(t, func(cmd []string) string {
		if len(cmd) > 2 && cmd[2] == "masteruser" {
			return "-ERR Unknown option or number of arguments for CONFIG SET - 'masteruser'\r\n"
		}
		return "+OK\r\n"
	})
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), AuthUsername: "default", AuthPassword: "pw"}}
	c, err := s.dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	s.setMasterAuth(c)
	want := [][]string{
		{"AUTH", "default", "pw"},
		{"CONFIG", "SET", "masteruser", "default"},
		{"CONFIG", "SET", "masterauth", "pw"},
	}
	if got := rr.commands(); !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %q, want %q", got, want)
	}
}

func TestSetMasterAuthWithoutPasswordSendsNothing(t *testing.T) {
	rr := newRespRecorder(t, func([]string) string { return "+OK\r\n" })
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), AuthUsername: "default"}}
	c, err := s.dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	s.setMasterAuth(c)
	if got := rr.commands(); len(got) != 0 {
		t.Fatalf("sent %q, want nothing", got)
	}
}
