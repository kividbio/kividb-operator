package agentapi

import (
	"fmt"
	"strings"
)

// Backups of a sharded cluster. One backup run takes a snapshot of every
// shard, each from one of the shard's pods, under one run ID:
//
//	<prefix>/<cluster>/<run>/shard-<i>-<pod>.tar.gz   one per shard
//	<prefix>/<cluster>/<run>/manifest.json            BackupManifest, written last
//
// A run without a manifest is incomplete. Retention counts complete runs,
// not objects, so a run is kept or pruned whole.
//
//	POST /backup           * -> optional body BackupRequest -> 200 BackupResponse
//	POST /backup/manifest  * -> body BackupManifest -> 200 ManifestResponse

// BackupRequest is the optional body of POST /backup. With Run set the
// object goes under the run, named after the shard, and the agent leaves
// pruning to the manifest.
type BackupRequest struct {
	Run   string `json:"run,omitempty"`
	Shard int32  `json:"shard"`
}

// ManifestName is the manifest's object name within a run.
const ManifestName = "manifest.json"

// BackupManifest describes one complete backup of a sharded cluster.
type BackupManifest struct {
	Cluster string `json:"cluster"`
	Run     string `json:"run"`
	// Shards, one per shard, by index.
	Shards []ManifestShard `json:"shards"`
}

// ManifestShard is one shard's snapshot in a BackupManifest.
type ManifestShard struct {
	Index      int32  `json:"index"`
	SourcePod  string `json:"sourcePod"`
	SourceRole Role   `json:"sourceRole"`
	// MasterNodeID is the node ID of the shard's master when the snapshot
	// was taken, and Slots the slots it owned: what the snapshot's keys
	// belong to.
	MasterNodeID string   `json:"masterNodeID,omitempty"`
	Slots        []string `json:"slots,omitempty"`
	ObjectKey    string   `json:"objectKey"`
	SizeBytes    int64    `json:"sizeBytes"`
}

// ManifestResponse is returned by POST /backup/manifest.
type ManifestResponse struct {
	ObjectKey  string `json:"objectKey"`
	PrunedRuns int    `json:"prunedRuns"`
}

// BackupShardResult is one shard's part of a BackupResult. Kept small: the
// whole result has to fit a termination message (4 KiB).
type BackupShardResult struct {
	Index     int32  `json:"i"`
	SourcePod string `json:"pod,omitempty"`
	SizeBytes int64  `json:"size,omitempty"`
	Error     string `json:"error,omitempty"`
}

// RunTimeFormat names a run by when it started; runs sort by name.
const RunTimeFormat = "20060102T150405Z"

// CheckRun reports whether run can name a run: a RunTimeFormat time,
// nothing that could leave the cluster's prefix.
func CheckRun(run string) error {
	if len(run) != len(RunTimeFormat) || strings.Trim(run, "0123456789TZ") != "" {
		return fmt.Errorf("%q is not a run ID", run)
	}
	return nil
}
