package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// runShardedBackup is `agent backup-trigger --shards`: one backup run of a
// sharded cluster. For every shard it finds a pod with the wanted role
// and has its agent upload a snapshot under the run, all shards at once;
// then it sends the manifest to shard 0's agent, which marks the run
// complete and prunes old runs. The shards' snapshots are each taken when
// their BGSAVE runs, a moment apart: a backup of every shard, not of one
// instant across them.
func runShardedBackup(shards [][]string, role agentapi.Role, cluster, token string, timeout time.Duration) error {
	start := time.Now()
	run := start.UTC().Format(agentapi.RunTimeFormat)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	manifest := agentapi.BackupManifest{Cluster: cluster, Run: run, Shards: make([]agentapi.ManifestShard, len(shards))}
	results := make([]agentapi.BackupShardResult, len(shards))
	addrs := make([]string, len(shards))
	var wg sync.WaitGroup
	for i, targets := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].Index = int32(i)
			ms, addr, err := backupShard(ctx, int32(i), targets, role, run, token, timeout)
			if err != nil {
				results[i].Error = err.Error()
				return
			}
			manifest.Shards[i], addrs[i] = ms, addr
			results[i].SourcePod, results[i].SizeBytes = ms.SourcePod, ms.SizeBytes
		}()
	}
	wg.Wait()

	result := agentapi.BackupResult{Shards: results}
	result.SourceRole = role
	var failed []string
	for _, r := range results {
		result.SizeBytes += r.SizeBytes
		if r.Error != "" {
			failed = append(failed, fmt.Sprintf("shard %d: %s", r.Index, r.Error))
		}
	}
	if len(failed) > 0 {
		result.Error = trimMessage(strings.Join(failed, "; "))
		writeBackupResult(fitResult(result))
		return fmt.Errorf("backup run %s incomplete: %s", run, strings.Join(failed, "; "))
	}

	var mr agentapi.ManifestResponse
	if err := postJSON(ctx, "http://"+addrs[0]+"/backup/manifest", token, manifest, &mr); err != nil {
		result.Error = trimMessage("writing the manifest: " + err.Error())
		writeBackupResult(fitResult(result))
		return fmt.Errorf("backup run %s: %s", run, result.Error)
	}
	result.Success = true
	result.ObjectKey = mr.ObjectKey
	result.PrunedCount = mr.PrunedRuns
	result.DurationMs = time.Since(start).Milliseconds()
	writeBackupResult(fitResult(result))
	fmt.Printf("backup ok: run=%s manifest=%s shards=%d size=%dB duration=%dms pruned-runs=%d\n",
		run, mr.ObjectKey, len(shards), result.SizeBytes, result.DurationMs, mr.PrunedRuns)
	return nil
}

// backupShard backs up one shard from one of its pods holding role, and
// returns the shard's manifest entry and the agent it used.
func backupShard(ctx context.Context, index int32, targets []string, role agentapi.Role, run, token string, timeout time.Duration) (agentapi.ManifestShard, string, error) {
	addr, err := findAgent(ctx, targets, role, token)
	if err != nil {
		return agentapi.ManifestShard{}, "", err
	}
	ms := agentapi.ManifestShard{Index: index, SourceRole: role}
	// Which slots the snapshot holds: the shard master's.
	var st agentapi.ClusterStatusResponse
	if err := getJSON(ctx, "http://"+addr+"/cluster/status", token, &st); err != nil {
		return ms, addr, fmt.Errorf("%s: cluster status: %w", addr, err)
	}
	if me, ok := st.Myself(); ok {
		masterID := me.ID
		if !me.IsMaster() {
			masterID = me.MasterID
		}
		ms.MasterNodeID = masterID
		for _, n := range st.Nodes {
			if n.ID == masterID {
				ms.Slots = n.Slots
			}
		}
	}
	var br agentapi.BackupResponse
	if err := postJSONTimeout(ctx, "http://"+addr+"/backup", token, agentapi.BackupRequest{Run: run, Shard: index}, &br, timeout); err != nil {
		return ms, addr, fmt.Errorf("%s: %w", addr, err)
	}
	ms.SourcePod, ms.ObjectKey, ms.SizeBytes = br.SourcePod, br.ObjectKey, br.SizeBytes
	if br.SourceRole != "" {
		ms.SourceRole = br.SourceRole
	}
	return ms, addr, nil
}

func getJSON(ctx context.Context, url, token string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	return doJSON(req, token, out, 10*time.Second)
}

func postJSON(ctx context.Context, url, token string, in, out any) error {
	return postJSONTimeout(ctx, url, token, in, out, time.Minute)
}

func postJSONTimeout(ctx context.Context, url, token string, in, out any, timeout time.Duration) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return doJSON(req, token, out, timeout)
}

func doJSON(req *http.Request, token string, out any, timeout time.Duration) error {
	agentapi.SetToken(req, token)
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var e agentapi.ErrorResponse
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return fmt.Errorf("%s", e.Error)
		}
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}

// terminationMessageLimit is what Kubernetes keeps of a termination
// message.
const terminationMessageLimit = 4096

func trimMessage(s string) string {
	if len(s) > 1024 {
		return s[:1021] + "..."
	}
	return s
}

// fitResult drops per-shard detail until the result fits a termination
// message: errors first, then the pods.
func fitResult(r agentapi.BackupResult) agentapi.BackupResult {
	fits := func() bool {
		b, _ := json.Marshal(r)
		return len(b) <= terminationMessageLimit
	}
	for i := range r.Shards {
		if fits() {
			return r
		}
		r.Shards[i].Error = ""
	}
	for i := range r.Shards {
		if fits() {
			return r
		}
		r.Shards[i].SourcePod = ""
	}
	if !fits() {
		r.Shards = nil
	}
	return r
}
