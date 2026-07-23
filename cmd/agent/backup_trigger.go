package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// runBackupTrigger implements `agent backup-trigger`, the CronJob-side
// client: a single POST to the master pod's agent /backup endpoint. All S3
// credentials and upload logic live in the agent sidecar that owns the
// data volume (see backup.go); this command never touches S3 or the PVC
// directly, which is why the backup CronJob's pod needs neither mounted.
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
		return fmt.Errorf("backup request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var errResp agentapi.ErrorResponse
		if jsonErr := json.Unmarshal(body, &errResp); jsonErr == nil && errResp.Error != "" {
			return fmt.Errorf("backup failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("backup failed (%d): %s", resp.StatusCode, string(body))
	}

	var result agentapi.BackupResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decoding backup response: %w", err)
	}
	fmt.Printf("backup ok: object=%s size=%dB duration=%dms pruned=%d\n",
		result.ObjectKey, result.SizeBytes, result.DurationMs, result.PrunedCount)
	return nil
}
