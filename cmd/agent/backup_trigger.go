package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// terminationMessagePath returns where to write the JSON BackupResult so
// the controller can read it back from
// pod.status.containerStatuses[].state.terminated.message without
// scraping logs. Matches Kubernetes' own default
// container.terminationMessagePath; overridable for local testing.
func terminationMessagePath() string {
	if p := os.Getenv("TERMINATION_MESSAGE_PATH"); p != "" {
		return p
	}
	return "/dev/termination-log"
}

func writeBackupResult(result agentapi.BackupResult) {
	data, err := json.Marshal(result)
	if err != nil {
		return
	}
	_ = os.WriteFile(terminationMessagePath(), data, 0o644) //nolint:gosec // termination-log is world-readable by design (kubelet reads it)
}

// runBackupTrigger implements `agent backup-trigger`, the CronJob-side
// client. It asks each of the cluster's agents (--targets, their stable
// headless-Service names) for its role, picks the first that holds the
// wanted one (--role: master or replica, per the KividbSnapshotConfig's
// spec.source), and POSTs /backup to it, with the agent token from
// AGENT_TOKEN. All S3 credentials and upload logic live in the agent
// sidecar that owns the data volume (see backup.go); this command never
// touches S3 or the PVC directly, which is why the backup CronJob's pod
// needs neither mounted.
//
// --url names one agent's /backup directly instead (for testing, or a
// cluster whose roles never move).
//
// On exit it also writes a BackupResult to its termination message (see
// terminationMessagePath), which internal/controller/backup.go reads back
// to populate a KividbSnapshot's status -- this is the only channel the
// controller has for the exact object key/size/source pod, since it never
// talks to the agent's HTTP API directly for backups.
func runBackupTrigger(args []string) error {
	fs := flag.NewFlagSet("backup-trigger", flag.ExitOnError)
	url := fs.String("url", "", "URL of one agent's /backup endpoint, instead of --targets")
	targets := fs.String("targets", "", "Comma-separated host:port of every agent in the cluster, e.g. c-0.c-headless.ns.svc:8081,c-1.c-headless.ns.svc:8081")
	role := fs.String("role", string(agentapi.RoleMaster), "Role of the pod to back up: master or replica")
	timeout := fs.Duration("timeout", 15*time.Minute, "Overall HTTP timeout for the backup request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := os.Getenv(agentapi.TokenEnv)

	target := *url
	if target == "" {
		if *targets == "" {
			return fail(fmt.Errorf("--targets or --url is required"))
		}
		addr, err := findAgent(context.Background(), strings.Split(*targets, ","), agentapi.Role(*role), token)
		if err != nil {
			return fail(err)
		}
		target = "http://" + addr + "/backup"
	}

	resp, err := postBackup(target, token, *timeout)
	if err != nil {
		return fail(fmt.Errorf("backup request failed: %w", err))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var errResp agentapi.ErrorResponse
		msg := string(body)
		if jsonErr := json.Unmarshal(body, &errResp); jsonErr == nil && errResp.Error != "" {
			msg = errResp.Error
		}
		writeBackupResult(agentapi.BackupResult{Success: false, Error: msg})
		return fmt.Errorf("backup failed (%d): %s", resp.StatusCode, msg)
	}

	var result agentapi.BackupResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fail(fmt.Errorf("decoding backup response: %w", err))
	}
	writeBackupResult(agentapi.BackupResult{Success: true, BackupResponse: result})
	fmt.Printf("backup ok: object=%s size=%dB duration=%dms pruned=%d source=%s(%s)\n",
		result.ObjectKey, result.SizeBytes, result.DurationMs, result.PrunedCount, result.SourcePod, result.SourceRole)
	return nil
}

func fail(err error) error {
	writeBackupResult(agentapi.BackupResult{Success: false, Error: err.Error()})
	return err
}

func postBackup(url, token string, timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	agentapi.SetToken(req, token)
	return (&http.Client{Timeout: timeout}).Do(req)
}

// findAgent returns the first of targets whose agent reports role. An
// agent that does not answer is skipped: it may be the pod that is down.
func findAgent(ctx context.Context, targets []string, role agentapi.Role, token string) (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	var problems []string
	for _, t := range targets {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+t+"/status", nil)
		if err != nil {
			return "", err
		}
		agentapi.SetToken(req, token)
		resp, err := client.Do(req)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", t, err))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			problems = append(problems, fmt.Sprintf("%s: status %d", t, resp.StatusCode))
			continue
		}
		var st agentapi.StatusResponse
		if err := json.Unmarshal(body, &st); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", t, err))
			continue
		}
		if st.Role == role {
			return t, nil
		}
	}
	msg := fmt.Sprintf("no agent reports role %s", role)
	if len(problems) > 0 {
		msg += " (" + strings.Join(problems, "; ") + ")"
	}
	return "", fmt.Errorf("%s", msg)
}
