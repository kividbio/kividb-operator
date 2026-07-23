package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kividbio/kividb-operator/internal/respclient"
)

// metricSpec maps one INFO field to one Prometheus gauge/counter. INFO
// fields that aren't present on a given kividb build/role are silently
// skipped rather than emitted as zero, so absence in scraped output means
// "not reported by kividb" rather than "measured as zero".
type metricSpec struct {
	infoKey  string
	promName string
	help     string
	counter  bool
}

var metricSpecs = []metricSpec{
	{"connected_clients", "kividb_connected_clients", "Number of client connections", false},
	{"used_memory", "kividb_used_memory_bytes", "Memory used by kividb, in bytes", false},
	{"uptime_in_seconds", "kividb_uptime_in_seconds", "Seconds since kividb started", false},
	{"connected_slaves", "kividb_connected_slaves", "Number of connected replicas", false},
	{"master_repl_offset", "kividb_master_repl_offset", "Current replication offset", false},
	{"rdb_changes_since_last_save", "kividb_rdb_changes_since_last_save", "Number of writes since the last snapshot", false},
	{"total_commands_processed", "kividb_commands_processed_total", "Total commands processed", true},
	{"total_connections_received", "kividb_connections_received_total", "Total connections accepted", true},
	{"keyspace_hits", "kividb_keyspace_hits_total", "Total successful key lookups", true},
	{"keyspace_misses", "kividb_keyspace_misses_total", "Total failed key lookups", true},
	{"expired_keys", "kividb_expired_keys_total", "Total keys expired", true},
}

func (s *server) renderMetrics() (string, error) {
	c, err := s.dial()
	if err != nil {
		return "", err
	}
	defer c.Close()

	var b strings.Builder

	if err := c.Ping(); err != nil {
		fmt.Fprintln(&b, "# HELP kividb_up Whether kividb responded to PING (1) or not (0)")
		fmt.Fprintln(&b, "# TYPE kividb_up gauge")
		fmt.Fprintln(&b, "kividb_up 0")
		return b.String(), nil
	}
	fmt.Fprintln(&b, "# HELP kividb_up Whether kividb responded to PING (1) or not (0)")
	fmt.Fprintln(&b, "# TYPE kividb_up gauge")
	fmt.Fprintln(&b, "kividb_up 1")

	info, err := c.Info("")
	if err != nil {
		return b.String(), nil //nolint:nilerr // partial metrics (kividb_up) are still useful even if INFO fails
	}
	fields := respclient.ParseInfo(info)

	if role := fields["role"]; role != "" {
		fmt.Fprintln(&b, "# HELP kividb_role_info Current replication role (always 1; role is a label)")
		fmt.Fprintln(&b, "# TYPE kividb_role_info gauge")
		fmt.Fprintf(&b, "kividb_role_info{role=%q} 1\n", role)
	}

	for _, spec := range metricSpecs {
		raw, ok := fields[spec.infoKey]
		if !ok {
			continue
		}
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue
		}
		metricType := "gauge"
		if spec.counter {
			metricType = "counter"
		}
		fmt.Fprintf(&b, "# HELP %s %s\n", spec.promName, spec.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", spec.promName, metricType)
		fmt.Fprintf(&b, "%s %s\n", spec.promName, strconv.FormatFloat(val, 'f', -1, 64))
	}

	return b.String(), nil
}
