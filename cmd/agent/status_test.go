package main

import (
	"strings"
	"testing"
)

// The status carries what the operator needs to trust a master that
// restarted from its append-only file: the engine's version and its
// aof-ack-after-write. An engine without the setting reports none.
func TestStatusReportsEngineVersionAndAofAck(t *testing.T) {
	for _, tc := range []struct {
		name, version, configGet, wantAck string
	}{
		{"1.2 engine", "1.2.0-rc7", "*2\r\n" + bulk("aof-ack-after-write") + bulk("yes"), "yes"},
		{"1.2 engine, acks before the write", "1.2.0", "*2\r\n" + bulk("aof-ack-after-write") + bulk("no"), "no"},
		{"1.1 engine", "1.1.3", "*0\r\n", ""},
		{"CONFIG refused", "1.1.3", "-NOPERM this user has no permissions to run the 'config|get' command\r\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := newRespRecorder(t, func(cmd []string) string {
				switch strings.ToUpper(strings.Join(cmd, " ")) {
				case "ROLE":
					return "*3\r\n" + bulk("master") + ":100\r\n*0\r\n"
				case "INFO SERVER":
					return bulk("kividb_version:" + tc.version + "\r\nrun_id:edf3a91aa4a65b42e1c934a5f2e6cab04e3d000c\r\n")
				case "INFO PERSISTENCE":
					return bulk("aof_enabled:1\r\n")
				case "CONFIG GET AOF-ACK-AFTER-WRITE":
					return tc.configGet
				}
				return "-ERR unknown\r\n"
			})
			s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String()}}
			st, err := s.queryStatus()
			if err != nil {
				t.Fatal(err)
			}
			if st.EngineVersion != tc.version || st.AofAckAfterWrite != tc.wantAck || !st.AofEnabled ||
				st.RunID != "edf3a91aa4a65b42e1c934a5f2e6cab04e3d000c" {
				t.Fatalf("%+v", st)
			}
		})
	}
}
