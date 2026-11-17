package main

import (
	"strings"
	"testing"
)

// The key count is reported as known only when INFO keyspace says it: at
// least one dbN line read, or the section's header with nothing under it.
// Anything else, the empty reply an engine gives for a section it does not
// have included, is unknown, which the operator takes as holding keys,
// never as 0 (#110).
func TestKeyspaceKeyCount(t *testing.T) {
	for _, tc := range []struct {
		name, info string
		keys       int64
		known      bool
	}{
		{"one database", "# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=99989\r\n\r\n", 2, true},
		{"two databases", "# Keyspace\r\ndb0:keys=10,expires=0,avg_ttl=0\r\ndb3:keys=5,expires=5,avg_ttl=7\r\n", 15, true},
		{"no keys", "# Keyspace\r\n\r\n", 0, true},
		{"a field it does not know, beside a database", "# Keyspace\r\ndb0:keys=3,expires=0\r\nkeys_total:3\r\n", 3, true},
		{"an empty reply", "", 0, false},
		{"only blank lines", "\r\n\r\n", 0, false},
		{"another section", "# Server\r\nredis_version:8.10.0\r\n", 0, false},
		{"a count it cannot read", "# Keyspace\r\ndb0:keys=many,expires=0\r\n", 0, false},
		{"no count", "# Keyspace\r\ndb0:expires=1\r\n", 0, false},
		{"a negative count", "# Keyspace\r\ndb0:keys=-1\r\n", 0, false},
		{"one database it cannot read among others", "# Keyspace\r\ndb0:keys=3\r\ndb1:keys=x\r\n", 0, false},
		{"only fields it does not know", "# Keyspace\r\nkeys_total:5\r\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, known := keyspaceKeyCount(tc.info)
			if keys != tc.keys || known != tc.known {
				t.Fatalf("keyspaceKeyCount(%q) = %d, %v; want %d, %v", tc.info, keys, known, tc.keys, tc.known)
			}
		})
	}
}

// End to end, through the status the operator reads: an engine whose INFO
// keyspace comes back empty reports no key count.
func TestStatusKeyCountUnknownOnAnEmptyKeyspaceReply(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		keys        int64
		known       bool
	}{
		{"keys", bulk("# Keyspace\r\ndb0:keys=7,expires=0,avg_ttl=0\r\n"), 7, true},
		{"none", bulk("# Keyspace\r\n\r\n"), 0, true},
		{"an empty reply", bulk(""), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := newRespRecorder(t, func(cmd []string) string {
				switch strings.ToUpper(strings.Join(cmd, " ")) {
				case "ROLE":
					return "*3\r\n" + bulk("master") + ":100\r\n*0\r\n"
				case "INFO KEYSPACE":
					return tc.reply
				}
				return "-ERR unknown\r\n"
			})
			s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String()}}
			st, err := s.queryStatus()
			if err != nil {
				t.Fatal(err)
			}
			if st.KeyCount != tc.keys || st.KeyCountKnown != tc.known {
				t.Fatalf("KeyCount %d, KeyCountKnown %v; want %d, %v", st.KeyCount, st.KeyCountKnown, tc.keys, tc.known)
			}
		})
	}
}
