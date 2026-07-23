package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
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
// client: a single POST to the target Service's /backup endpoint (master
// or replica, per the owning KividbSnapshotConfig's spec.source). All S3
// credentials and upload logic live in the agent sidecar that owns the
// data volume (see backup.go); this command never touches S3 or the PVC
// directly, which is why the backup CronJob's pod needs neither mounted.
//
// On exit it also writes a BackupResult to its termination message (see
// terminationMessagePath), which internal/controller/backup.go reads back
// to populate a KividbSnapshot's status -- this is the only channel the
// controller has for the exact object key/size/source pod, since it never
// talks to the agent's HTTP API directly for backups.
func runBackupTrigger(args []string) error {
	fs := flag.NewFlagSet("backup-trigger", flag.ExitOnError)
	url := fs.String("url", "", "Full URL of the target agent's /backup endpoint, e.g. http://mycluster-master.default.svc:8081/backup")
	timeout := fs.Duration("timeout", 15*time.Minute, "Overall HTTP timeout for the backup request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("--url is required")
	}

	client := &http.Client{Timeout: *timeout}
	resp, err := client.Post(*url, "application/json", nil)
	if err != nil {
		writeBackupResult(agentapi.BackupResult{Success: false, Error: err.Error()})
		return fmt.Errorf("backup request failed: %w", err)
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
		writeBackupResult(agentapi.BackupResult{Success: false, Error: fmt.Sprintf("decoding backup response: %s", err)})
		return fmt.Errorf("decoding backup response: %w", err)
	}
	writeBackupResult(agentapi.BackupResult{Success: true, BackupResponse: result})
	fmt.Printf("backup ok: object=%s size=%dB duration=%dms pruned=%d source=%s(%s)\n",
		result.ObjectKey, result.SizeBytes, result.DurationMs, result.PrunedCount, result.SourcePod, result.SourceRole)
	return nil
}
