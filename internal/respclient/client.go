// Package respclient is a minimal, dependency-free client for the Redis
// RESP2 wire protocol, sufficient to drive the handful of commands the
// kividb-operator agent needs against a local (or remote) kividb instance:
// AUTH, PING, INFO, REPLICAOF, ROLE, SAVE, BGSAVE, LASTSAVE and ACL LOAD.
//
// kividb speaks plain RESP2 over a single TCP port (see cmd/agent) -- there
// is no HTTP admin API, so this package exists instead of pulling in a full
// Redis client library for a handful of commands.
package respclient

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Client is a single-connection RESP2 client. It is not safe for concurrent
// use; callers should serialize access or create one Client per goroutine.
type Client struct {
	addr    string
	conn    net.Conn
	r       *bufio.Reader
	timeout time.Duration
}

// Dial connects to addr ("host:port") with the given per-call timeout.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("respclient: dial %s: %w", addr, err)
	}
	return &Client{
		addr:    addr,
		conn:    conn,
		r:       bufio.NewReader(conn),
		timeout: timeout,
	}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Reply is a parsed RESP2 reply. Exactly one of Str/Int/Array is meaningful,
// selected by Type ('+', '-', ':', '$', '*'). IsNil is set for $-1 / *-1.
type Reply struct {
	Type  byte
	Str   string
	Int   int64
	Array []*Reply
	IsNil bool
}

// Err returns a non-nil error if this reply is a RESP error reply.
func (r *Reply) Err() error {
	if r != nil && r.Type == '-' {
		return fmt.Errorf("kividb: %s", r.Str)
	}
	return nil
}

// Do sends a command (as a RESP array of bulk strings) and returns the
// parsed reply.
func (c *Client) Do(args ...string) (*Reply, error) {
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, err
	}
	if err := writeCommand(c.conn, args); err != nil {
		return nil, fmt.Errorf("respclient: write: %w", err)
	}
	reply, err := readReply(c.r)
	if err != nil {
		return nil, fmt.Errorf("respclient: read: %w", err)
	}
	if err := reply.Err(); err != nil {
		return reply, err
	}
	return reply, nil
}

func writeCommand(w net.Conn, args []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := w.Write([]byte(b.String()))
	return err
}

func readReply(r *bufio.Reader) (*Reply, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("empty reply line")
	}
	switch line[0] {
	case '+':
		return &Reply{Type: '+', Str: line[1:]}, nil
	case '-':
		return &Reply{Type: '-', Str: line[1:]}, nil
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad integer reply %q: %w", line, err)
		}
		return &Reply{Type: ':', Int: n}, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("bad bulk length %q: %w", line, err)
		}
		if n < 0 {
			return &Reply{Type: '$', IsNil: true}, nil
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		return &Reply{Type: '$', Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("bad array length %q: %w", line, err)
		}
		if n < 0 {
			return &Reply{Type: '*', IsNil: true}, nil
		}
		arr := make([]*Reply, n)
		for i := 0; i < n; i++ {
			item, err := readReply(r)
			if err != nil {
				return nil, err
			}
			arr[i] = item
		}
		return &Reply{Type: '*', Array: arr}, nil
	default:
		return nil, fmt.Errorf("unrecognized reply prefix %q", line[0])
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Auth performs AUTH password (or AUTH username password if username != "").
func (c *Client) Auth(username, password string) error {
	if password == "" {
		return nil
	}
	var err error
	if username != "" && username != "default" {
		_, err = c.Do("AUTH", username, password)
	} else {
		_, err = c.Do("AUTH", password)
	}
	return err
}

// Ping issues PING and returns nil if the server replied PONG.
func (c *Client) Ping() error {
	reply, err := c.Do("PING")
	if err != nil {
		return err
	}
	if reply.Type == '+' && strings.EqualFold(reply.Str, "PONG") {
		return nil
	}
	return fmt.Errorf("unexpected PING reply: %+v", reply)
}

// Info issues INFO [section] and returns the raw bulk string body.
func (c *Client) Info(section string) (string, error) {
	args := []string{"INFO"}
	if section != "" {
		args = append(args, section)
	}
	reply, err := c.Do(args...)
	if err != nil {
		return "", err
	}
	return reply.Str, nil
}

// ParseInfo turns an INFO body into a flat key/value map, skipping section
// headers ("# Replication") and blank lines, matching kividb's (and
// Redis's) "key:value\r\n" INFO format.
func ParseInfo(body string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		out[parts[0]] = parts[1]
	}
	return out
}

// ReplicaOf issues REPLICAOF host port.
func (c *Client) ReplicaOf(host string, port int32) error {
	_, err := c.Do("REPLICAOF", host, strconv.Itoa(int(port)))
	return err
}

// ReplicaOfNoOne promotes this instance to master via REPLICAOF NO ONE.
func (c *Client) ReplicaOfNoOne() error {
	_, err := c.Do("REPLICAOF", "NO", "ONE")
	return err
}

// Role issues ROLE and returns the raw reply for the caller to interpret
// (["master", offset, [...]], or ["slave", host, port, state, offset]).
func (c *Client) Role() (*Reply, error) {
	return c.Do("ROLE")
}

// Save issues the synchronous SAVE command.
func (c *Client) Save() error {
	_, err := c.Do("SAVE")
	return err
}

// Bgsave issues BGSAVE and returns once the background save has been
// scheduled (not once it has completed -- poll LastSave to detect
// completion).
func (c *Client) Bgsave() error {
	_, err := c.Do("BGSAVE")
	return err
}

// LastSave issues LASTSAVE and returns the unix timestamp of the last
// successful snapshot.
func (c *Client) LastSave() (int64, error) {
	reply, err := c.Do("LASTSAVE")
	if err != nil {
		return 0, err
	}
	return reply.Int, nil
}

// AclLoad issues ACL LOAD, reloading the on-disk ACL file configured via
// --aclfile/aclfile.
func (c *Client) AclLoad() error {
	_, err := c.Do("ACL", "LOAD")
	return err
}
