package respclient

import (
	"bufio"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestAuthArgs(t *testing.T) {
	for _, tc := range []struct {
		user string
		want []string
	}{
		{"", []string{"AUTH", "default", "pw"}},
		{"default", []string{"AUTH", "default", "pw"}},
		{"app", []string{"AUTH", "app", "pw"}},
	} {
		if got := authArgs(tc.user, "pw"); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("authArgs(%q) = %q, want %q", tc.user, got, tc.want)
		}
	}
}

// fakeServer reads one RESP command from conn, sends it on got and replies
// reply.
func fakeServer(t *testing.T, conn net.Conn, reply string, got chan<- []string) {
	t.Helper()
	go func() {
		defer conn.Close()
		r := bufio.NewReader(conn)
		cmd, err := readReply(r)
		if err != nil {
			got <- nil
			return
		}
		args := make([]string, len(cmd.Array))
		for i, a := range cmd.Array {
			args[i] = a.Str
		}
		got <- args
		_, _ = conn.Write([]byte(reply))
	}()
}

func pipeClient() (*Client, net.Conn) {
	client, server := net.Pipe()
	return &Client{addr: "pipe", conn: client, r: bufio.NewReader(client), timeout: 2 * time.Second}, server
}

func TestAuthSendsTwoArgumentFormForDefaultUser(t *testing.T) {
	c, server := pipeClient()
	defer c.Close()
	got := make(chan []string, 1)
	fakeServer(t, server, "+OK\r\n", got)
	if err := c.Auth("default", "s3cret"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	if args := <-got; !reflect.DeepEqual(args, []string{"AUTH", "default", "s3cret"}) {
		t.Fatalf("sent %q, want AUTH default s3cret", args)
	}
}

func TestAuthWithoutPasswordSendsNothing(t *testing.T) {
	c, server := pipeClient()
	defer c.Close()
	defer server.Close()
	if err := c.Auth("default", ""); err != nil {
		t.Fatalf("Auth with no password: %v", err)
	}
}

func TestAuthReturnsErrorReply(t *testing.T) {
	c, server := pipeClient()
	defer c.Close()
	got := make(chan []string, 1)
	fakeServer(t, server, "-WRONGPASS invalid username-password pair or user is disabled.\r\n", got)
	if err := c.Auth("", "old"); err == nil {
		t.Fatal("Auth succeeded on an error reply")
	}
	<-got
}
