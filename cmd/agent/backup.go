package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// backupSaveTimeout bounds how long we wait for BGSAVE to actually finish
// (observed via LASTSAVE advancing) before giving up. This is independent
// of -- and expected to be shorter than -- the CronJob-side --timeout
// passed to `agent backup-trigger`, which also has to cover upload time.
const backupSaveTimeout = 5 * time.Minute

// dataFiles are kividb's persistence file names, in the directory it
// changed into at start (see backupDir). They are fixed: the engine
// ignores `dbfilename` and `appendfilename` (CONFIG GET reports these
// names whatever they are set to).
var dataFiles = []string{"dump.kdb", "appendonly.aof"}

func (s *server) runBackup(ctx context.Context, req agentapi.BackupRequest) (*agentapi.BackupResponse, error) {
	start := time.Now()

	if s.cfg.S3Endpoint == "" || s.cfg.S3Bucket == "" {
		return nil, fmt.Errorf("backup requested but S3 is not configured on this pod")
	}

	if err := s.triggerAndWaitForSave(ctx); err != nil {
		return nil, err
	}
	dir, err := s.backupDir()
	if err != nil {
		return nil, err
	}

	client, err := s.s3Client()
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}

	objectKey := s.objectKey()
	if req.Run != "" {
		objectKey = s.backupPrefix() + req.Run + "/" + fmt.Sprintf("shard-%d-%s.tar.gz", req.Shard, s.cfg.PodName)
	}
	size, err := s.streamUpload(ctx, client, objectKey, dir)
	if err != nil {
		return nil, fmt.Errorf("upload: %w", err)
	}

	// Part of a sharded backup: the manifest prunes, whole runs.
	pruned := 0
	if req.Run == "" {
		if pruned, err = s.pruneOldBackups(ctx, client); err != nil {
			// Pruning failures should not fail the backup itself -- the new
			// snapshot is already durably stored.
			pruned = 0
		}
	}

	role := agentapi.RoleUnknown
	if st, err := s.queryStatus(); err == nil {
		role = st.Role
	}

	return &agentapi.BackupResponse{
		ObjectKey:   objectKey,
		SizeBytes:   size,
		DurationMs:  time.Since(start).Milliseconds(),
		PrunedCount: pruned,
		SourcePod:   s.cfg.PodName,
		SourceRole:  role,
	}, nil
}

func (s *server) triggerAndWaitForSave(ctx context.Context) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	before, err := c.LastSave()
	if err != nil {
		return fmt.Errorf("LASTSAVE (before): %w", err)
	}
	if err := c.Bgsave(); err != nil {
		return fmt.Errorf("BGSAVE: %w", err)
	}

	deadline := time.Now().Add(backupSaveTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		after, err := c.LastSave()
		if err != nil {
			return fmt.Errorf("LASTSAVE (poll): %w", err)
		}
		if after > before {
			return nil
		}
	}
	return fmt.Errorf("timed out after %s waiting for BGSAVE to complete", backupSaveTimeout)
}

func (s *server) s3Client() (*minio.Client, error) {
	endpoint := s.cfg.S3Endpoint
	secure := true
	if strings.HasPrefix(endpoint, "http://") {
		secure = false
		endpoint = strings.TrimPrefix(endpoint, "http://")
	} else if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(s.cfg.S3AccessKeyID, s.cfg.S3SecretKey, ""),
		Secure: secure,
		Region: s.cfg.S3Region,
	}
	if s.cfg.S3ForcePathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}
	if s.cfg.S3InsecureTLS {
		opts.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // opt-in via S3_INSECURE_SKIP_TLS_VERIFY for self-signed dev/test MinIO only
	}

	return minio.New(endpoint, opts)
}

func (s *server) objectKey() string {
	ts := time.Now().UTC().Format("20060102T150405Z")
	return s.backupPrefix() + fmt.Sprintf("%s-%s.tar.gz", s.cfg.PodName, ts)
}

// backupPrefix is [<pathPrefix>/]<namespace>.<cluster>/. The namespace
// keeps same-named clusters apart. The dot (no namespace, cluster name or
// run ID has one) keeps the folder from being any other cluster's, old
// layout or new, so older agents, which prune their folder recursively,
// do not reach it. Old-layout backups are not pruned. Without
// POD_NAMESPACE the old layout is kept.
func (s *server) backupPrefix() string {
	dir := s.cfg.ClusterName
	if s.cfg.Namespace != "" {
		dir = s.cfg.Namespace + "." + dir
	}
	if p := strings.Trim(s.cfg.S3PathPrefix, "/"); p != "" {
		dir = p + "/" + dir
	}
	return dir + "/"
}

// backupDir is where the engine keeps its snapshot and append-only file:
// the directory it changed into at start, which CONFIG GET dir reports. A
// KividbConfig `dir` puts it under the volume (#112). The agent mounts the
// volume at the same path as the engine, so the path holds here too, but
// only on the volume, DataDir: an engine whose files are off it (a `dir`
// the operator now refuses, on a cluster that ran with it before) has
// nothing on the volume to back up. An engine that answers but will not
// say (an ACL that denies CONFIG to the default user) is taken to be where
// its pod's command line puts it, the --dir the operator also gives the
// agent (EngineDir) on a pod whose `dir` is under the volume, or else in
// DataDir, as before #112, and the log says so. One that does not answer
// at all fails the backup: DataDir may hold an older snapshot.
func (s *server) backupDir() (string, error) {
	c, err := s.dial()
	if err != nil {
		return "", err
	}
	defer c.Close()
	v, err := c.Do("CONFIG", "GET", "dir")
	if err != nil && v == nil {
		return "", fmt.Errorf("CONFIG GET dir: %w", err)
	}
	if err != nil || len(v.Array) != 2 || v.Array[1].Str == "" {
		dir, from := s.cfg.DataDir, "the volume"
		if s.cfg.EngineDir != "" {
			dir, from = s.cfg.EngineDir, "the engine's --dir"
		}
		log.Printf("CONFIG GET dir did not say where the engine keeps its files (%v): backing up %s, %s", err, dir, from)
		return s.onVolume(dir)
	}
	return s.onVolume(v.Array[1].Str)
}

// onVolume is dir, cleaned, if it is on this pod's volume, DataDir, or
// why there is nothing to back up there.
func (s *server) onVolume(dir string) (string, error) {
	dir = filepath.Clean(dir)
	if rel, err := filepath.Rel(filepath.Clean(s.cfg.DataDir), dir); err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("the engine keeps its files in %s, off this pod's volume at %s: there is nothing on the volume to back up (see TROUBLESHOOTING.md, \"A KividbConfig dir off the volume\")", dir, s.cfg.DataDir)
	}
	return dir, nil
}

// streamUpload tars+gzips the persistence files in dir directly into S3
// without buffering the whole archive in memory: a goroutine writes into
// an io.Pipe while PutObject reads from the other end with an unknown
// (-1) size, which minio-go handles via chunked multipart upload.
func (s *server) streamUpload(ctx context.Context, client *minio.Client, objectKey, dir string) (int64, error) {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		errCh <- writeTarGz(pw, dir, dataFiles)
	}()

	info, err := client.PutObject(ctx, s.cfg.S3Bucket, objectKey, pr, -1, minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
	// PutObject may stop reading early; unblock the writer.
	pr.CloseWithError(err)
	if writeErr := <-errCh; writeErr != nil && err == nil {
		err = writeErr
	}
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

func writeTarGz(pw *io.PipeWriter, dataDir string, files []string) error {
	defer pw.Close()
	gz := gzip.NewWriter(pw)
	tw := tar.NewWriter(gz)

	var wrote bool
	for _, name := range files {
		path := filepath.Join(dataDir, name)
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			continue // file doesn't exist yet (e.g. AOF disabled) -- skip, not an error
		}
		if err != nil {
			return closeAllAndErr(tw, gz, pw, err)
		}
		// Size comes from the handle that is about to be read, not from a
		// separate os.Stat of the path, so both describe the same file
		// even if kividb swaps it (BGSAVE / AOF rewrite both rename a new
		// file into place) in between.
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return closeAllAndErr(tw, gz, pw, err)
		}
		hdr := &tar.Header{Name: name, Mode: 0o600, Size: fi.Size(), ModTime: fi.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			f.Close()
			return closeAllAndErr(tw, gz, pw, err)
		}
		// Copy exactly the size the header promised. kividb keeps appending
		// to appendonly.aof while this runs; copying to EOF would write
		// more than hdr.Size and fail the whole backup with "archive/tar:
		// write too long" on any cluster taking writes.
		if _, err := io.CopyN(tw, f, fi.Size()); err != nil {
			f.Close()
			return closeAllAndErr(tw, gz, pw, fmt.Errorf("%s: %w", name, err))
		}
		f.Close()
		wrote = true
	}
	if !wrote {
		return closeAllAndErr(tw, gz, pw, fmt.Errorf("no persistence files found under %s (expected one of %v)", dataDir, files))
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func closeAllAndErr(tw *tar.Writer, gz *gzip.Writer, pw *io.PipeWriter, err error) error {
	_ = tw.Close()
	_ = gz.Close()
	pw.CloseWithError(err)
	return err
}

func (s *server) pruneOldBackups(ctx context.Context, client *minio.Client) (int, error) {
	if s.cfg.S3Retention <= 0 {
		return 0, nil
	}

	type obj struct {
		key      string
		modified time.Time
	}
	var objects []obj
	for info := range client.ListObjects(ctx, s.cfg.S3Bucket, minio.ListObjectsOptions{Prefix: s.backupPrefix(), Recursive: true}) {
		if info.Err != nil {
			return 0, info.Err
		}
		if !s.isOwnArchive(strings.TrimPrefix(info.Key, s.backupPrefix())) {
			continue
		}
		objects = append(objects, obj{key: info.Key, modified: info.LastModified})
	}
	if len(objects) <= s.cfg.S3Retention {
		return 0, nil
	}

	sort.Slice(objects, func(i, j int) bool { return objects[i].modified.After(objects[j].modified) })
	toDelete := objects[s.cfg.S3Retention:]
	for _, o := range toDelete {
		if err := client.RemoveObject(ctx, s.cfg.S3Bucket, o.key, minio.RemoveObjectOptions{}); err != nil {
			return 0, err
		}
	}
	return len(toDelete), nil
}

// isOwnArchive reports whether name, a key less backupPrefix(), is
// one objectKey() writes for a pod of this cluster. A deeper key is a
// sharded run, or another cluster's backup under a nested path prefix.
func (s *server) isOwnArchive(name string) bool {
	rest, ok := strings.CutPrefix(name, s.cfg.ClusterName+"-")
	if !ok {
		return false
	}
	ordinal, ts, ok := strings.Cut(rest, "-")
	if !ok || ordinal == "" || strings.Trim(ordinal, "0123456789") != "" {
		return false
	}
	ts, ok = strings.CutSuffix(ts, ".tar.gz")
	if !ok {
		return false
	}
	_, err := time.Parse("20060102T150405Z", ts)
	return err == nil
}
