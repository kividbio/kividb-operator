package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	"github.com/minio/minio-go/v7"
)

// handleBackupManifest completes a sharded cluster's backup run: it writes
// the run's manifest, which the backup Job sends once every shard's
// snapshot is up, then prunes whole runs past the retention.
func (s *server) handleBackupManifest(w http.ResponseWriter, r *http.Request) {
	var m agentapi.BackupManifest
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := agentapi.CheckRun(m.Run); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if m.Cluster != s.cfg.ClusterName || len(m.Shards) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("a manifest of cluster %q with shards is expected", s.cfg.ClusterName))
		return
	}
	runPrefix := s.backupPrefix() + m.Run + "/"
	for i, sh := range m.Shards {
		if sh.Index != int32(i) || !strings.HasPrefix(sh.ObjectKey, runPrefix) || strings.Contains(sh.ObjectKey[len(runPrefix):], "/") {
			writeError(w, http.StatusBadRequest, fmt.Errorf("shard %d: object %q is not in run %s", i, sh.ObjectKey, m.Run))
			return
		}
	}
	if s.cfg.S3Endpoint == "" || s.cfg.S3Bucket == "" {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("S3 is not configured on this pod"))
		return
	}
	client, err := s.s3Client()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("s3 client: %w", err))
		return
	}
	ctx := r.Context()
	// Every shard's object must be there: a manifest marks the run whole.
	for _, sh := range m.Shards {
		if _, err := client.StatObject(ctx, s.cfg.S3Bucket, sh.ObjectKey, minio.StatObjectOptions{}); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("shard %d: %s: %w", sh.Index, sh.ObjectKey, err))
			return
		}
	}
	body, _ := json.MarshalIndent(m, "", "  ")
	key := runPrefix + agentapi.ManifestName
	if _, err := client.PutObject(ctx, s.cfg.S3Bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("writing the manifest: %w", err))
		return
	}
	pruned, err := s.pruneRuns(ctx, client)
	if err != nil {
		pruned = 0 // the backup is complete; pruning is tried again next time
	}
	writeJSON(w, http.StatusOK, agentapi.ManifestResponse{ObjectKey: key, PrunedRuns: pruned})
}

// pruneRuns deletes the runs older than the newest S3Retention complete
// ones.
func (s *server) pruneRuns(ctx context.Context, client *minio.Client) (int, error) {
	if s.cfg.S3Retention <= 0 {
		return 0, nil
	}
	prefix := s.backupPrefix()
	var keys []string
	for info := range client.ListObjects(ctx, s.cfg.S3Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if info.Err != nil {
			return 0, info.Err
		}
		keys = append(keys, info.Key)
	}
	doomed, runs := runsToPrune(prefix, keys, s.cfg.S3Retention)
	for _, k := range doomed {
		if err := client.RemoveObject(ctx, s.cfg.S3Bucket, k, minio.RemoveObjectOptions{}); err != nil {
			return 0, err
		}
	}
	return runs, nil
}

// runsToPrune picks the objects to delete under prefix: every object of
// a run older than the oldest of the newest `keep` complete runs (those
// with a manifest). Runs newer than that are left, complete or not: one
// may be under way. Objects that are not in a run (an unsharded
// cluster's backups under the same name) are left alone.
func runsToPrune(prefix string, keys []string, keep int) (doomed []string, runs int) {
	byRun := map[string][]string{}
	complete := map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, prefix)
		run, name, ok := strings.Cut(rest, "/")
		if !ok || agentapi.CheckRun(run) != nil || strings.Contains(name, "/") {
			continue
		}
		byRun[run] = append(byRun[run], k)
		if name == agentapi.ManifestName {
			complete[run] = true
		}
	}
	var done []string
	for run := range complete {
		done = append(done, run)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(done)))
	if len(done) <= keep {
		return nil, 0
	}
	oldestKept := done[keep-1]
	for run, ks := range byRun {
		if run < oldestKept {
			doomed = append(doomed, ks...)
			runs++
		}
	}
	sort.Strings(doomed)
	return doomed, runs
}
