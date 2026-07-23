package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
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

// dataFiles are kividb's hardcoded persistence file names (see
// upstream src/main.rs / src/aof.rs; there is no `dir`/`dbfilename`
// config directive, so these are always relative to the working
// directory, which the operator pins to DataDir).
var dataFiles = []string{"dump.kdb", "appendonly.aof"}

func (s *server) runBackup(ctx context.Context) (*agentapi.BackupResponse, error) {
	start := time.Now()

	if s.cfg.S3Endpoint == "" || s.cfg.S3Bucket == "" {
		return nil, fmt.Errorf("backup requested but S3 is not configured on this pod")
	}

	if err := s.triggerAndWaitForSave(ctx); err != nil {
		return nil, err
	}

	client, err := s.s3Client()
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}

	objectKey := s.objectKey()
	size, err := s.streamUpload(ctx, client, objectKey)
	if err != nil {
		return nil, fmt.Errorf("upload: %w", err)
	}

	pruned, err := s.pruneOldBackups(ctx, client)
	if err != nil {
		// Pruning failures should not fail the backup itself -- the new
		// snapshot is already durably stored.
		pruned = 0
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
	prefix := strings.Trim(s.cfg.S3PathPrefix, "/")
	ts := time.Now().UTC().Format("20060102T150405Z")
	parts := []string{}
	if prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, s.cfg.ClusterName, fmt.Sprintf("%s-%s.tar.gz", s.cfg.PodName, ts))
	return strings.Join(parts, "/")
}

func (s *server) backupPrefix() string {
	prefix := strings.Trim(s.cfg.S3PathPrefix, "/")
	if prefix == "" {
		return s.cfg.ClusterName + "/"
	}
	return prefix + "/" + s.cfg.ClusterName + "/"
}

// streamUpload tars+gzips DataDir's persistence files directly into S3
// without buffering the whole archive in memory: a goroutine writes into
// an io.Pipe while PutObject reads from the other end with an unknown
// (-1) size, which minio-go handles via chunked multipart upload.
func (s *server) streamUpload(ctx context.Context, client *minio.Client, objectKey string) (int64, error) {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		errCh <- writeTarGz(pw, s.cfg.DataDir, dataFiles)
	}()

	info, err := client.PutObject(ctx, s.cfg.S3Bucket, objectKey, pr, -1, minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
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
		fi, err := os.Stat(path)
		if err != nil {
			continue // file doesn't exist yet (e.g. AOF disabled) -- skip, not an error
		}
		f, err := os.Open(path)
		if err != nil {
			return closeAllAndErr(tw, gz, pw, err)
		}
		hdr := &tar.Header{Name: name, Mode: 0o600, Size: fi.Size(), ModTime: fi.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			f.Close()
			return closeAllAndErr(tw, gz, pw, err)
		}
		if _, err := io.Copy(tw, f); err != nil {
			f.Close()
			return closeAllAndErr(tw, gz, pw, err)
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
