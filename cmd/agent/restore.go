package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// runRestoreFromS3 downloads a snapshot tar.gz from S3 and extracts
// dump.kdb / appendonly.aof into --data-dir. Used by the operator's
// bootstrap Job to seed a new cluster PVC.
func runRestoreFromS3(args []string) error {
	fs := flag.NewFlagSet("restore-from-s3", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "S3 endpoint URL")
	bucket := fs.String("bucket", "", "S3 bucket")
	region := fs.String("region", "", "S3 region")
	objectKey := fs.String("object-key", "", "object key of the snapshot tar.gz")
	dataDir := fs.String("data-dir", "/data", "directory to extract persistence files into")
	forcePathStyle := fs.String("force-path-style", "false", "use path-style addressing")
	insecureTLS := fs.String("insecure-skip-tls-verify", "false", "skip TLS verify")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" || *bucket == "" || *objectKey == "" {
		return fmt.Errorf("--endpoint, --bucket, and --object-key are required")
	}

	accessKey := os.Getenv("S3_ACCESS_KEY_ID")
	secretKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY must be set")
	}

	forcePS, _ := strconv.ParseBool(*forcePathStyle)
	insec, _ := strconv.ParseBool(*insecureTLS)

	ep := *endpoint
	secure := true
	if strings.HasPrefix(ep, "http://") {
		secure = false
		ep = strings.TrimPrefix(ep, "http://")
	} else if strings.HasPrefix(ep, "https://") {
		ep = strings.TrimPrefix(ep, "https://")
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: *region,
	}
	if forcePS {
		opts.BucketLookup = minio.BucketLookupPath
	}
	if insec {
		opts.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	}

	client, err := minio.New(ep, opts)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	log.Printf("downloading s3://%s/%s", *bucket, *objectKey)
	obj, err := client.GetObject(ctx, *bucket, *objectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		return err
	}
	if err := extractSnapshotTarGz(obj, *dataDir); err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	log.Printf("restored snapshot into %s", *dataDir)
	return nil
}

// restoredFileMode is group-read/writable on purpose. This process runs as
// the agent image's UID, but the files are for kividb, which runs as a
// different UID and reaches the data volume only through the pod's shared
// fsGroup. Owner-only files here are unreadable to it: kividb logs
// "Permission denied", starts empty, and the bootstrap still looks
// successful.
const restoredFileMode = 0o660

// engineStateFiles are the files the engine (1.2.0) keeps next to dump.kdb
// and appendonly.aof that describe those two files. A snapshot has none of
// them, and on a kept volume the earlier cluster's would be read with the
// restored files:
//
//   - dump.kdb.gen holds the earlier dump's save number, and the engine
//     reads it without checking that it is this dump's. Below the restored
//     log's number, the engine refuses to start; above it, it skips the
//     restored log as older than the dump, and the writes in it are lost.
//   - appendonly.aof.prev is the log of a checkpoint that did not finish.
//     The engine replays it on top of dump.kdb, before appendonly.aof.
//   - appendonly.aof.prev.len and appendonly.aof.rewrite.tmp are the rest of
//     such a checkpoint, which the engine puts right on top of .prev.
//   - dump.kdb.repl (a resync point) and dump.kdb.clean (a clean stop) are
//     checked against dump.kdb by its length and a hash of its first and
//     last 64 KiB only. Two dumps of one dataset can match on that.
//
// The list is in the order they are removed (see extractSnapshotTarGz).
// kividb.lock is not on it: a lock file no process holds does nothing.
var engineStateFiles = []string{
	"dump.kdb.gen",
	"appendonly.aof.prev",
	"appendonly.aof.prev.len",
	"appendonly.aof.rewrite.tmp",
	"dump.kdb.repl",
	"dump.kdb.clean",
}

// extractSnapshotTarGz restores the archive's dump.kdb and appendonly.aof
// into dataDir. The volume may already hold these files, written by the
// engine as its own UID (a kept PVC re-used by a new cluster), which this
// process can neither chmod nor, often, open for writing. So each entry is
// written to a new temp file in dataDir, and renamed over its target only
// once the whole archive has been read: that needs write access to the
// directory, not ownership of the file, and an archive that breaks part
// way leaves the old files whole. The price is that the volume needs room
// for the old files and the restored ones at once. The volume ends up with
// the archive's files only: a dump.kdb or appendonly.aof the archive does
// not have, and the engine's state files (engineStateFiles), are removed.
func extractSnapshotTarGz(r io.Reader, dataDir string) error {
	// The only files ever written, by fixed name. An entry's own name is
	// used just to pick one of these and never becomes part of a path, so
	// a crafted archive ("../../etc/x", absolute paths, links) cannot
	// write anywhere else.
	targets := map[string]string{"dump.kdb": "dump.kdb", "appendonly.aof": "appendonly.aof"}
	persistenceFiles := []string{"dump.kdb", "appendonly.aof"}

	// A Job that was killed (OOM, eviction, node loss) never ran its
	// cleanup, and its temp files would leave the retry short of room for
	// another full copy. Only this Job writes these names, and the engine
	// is not running on the volume while it does.
	for _, name := range persistenceFiles {
		left, err := filepath.Glob(filepath.Join(dataDir, restoreTempPrefix(name)+"*"))
		if err != nil {
			return err
		}
		for _, tmp := range left {
			if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing %s, left by an earlier restore: %w", tmp, err)
			}
			log.Printf("removed %s, left by an earlier restore", tmp)
		}
	}

	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	// temps maps each target name to the temp file holding its restored
	// content. Whatever is still in it when this returns was not renamed,
	// and is removed.
	temps := map[string]string{}
	defer func() {
		for _, tmp := range temps {
			_ = os.Remove(tmp)
		}
	}()
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name, ok := targets[filepath.Base(hdr.Name)]
		if !ok || hdr.Typeflag != tar.TypeReg {
			continue
		}
		tmp, err := writeRestoreTemp(tr, dataDir, name)
		if err != nil {
			return err
		}
		// A name that appears twice is restored from its last entry.
		if prev, ok := temps[name]; ok {
			_ = os.Remove(prev)
		}
		temps[name] = tmp
	}
	// The tar reader stops at the end-of-archive marker, before the gzip
	// trailer. Read to the end of the gzip stream so its checksum is
	// checked, and a damaged archive is refused before anything is replaced.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return fmt.Errorf("reading the archive's gzip trailer: %w", err)
	}
	if len(temps) == 0 {
		return fmt.Errorf("archive contained no dump.kdb or appendonly.aof")
	}

	// Nothing on the volume has changed up to here: an archive that breaks,
	// or a volume that fills, leaves the old files whole, the engine's
	// state files with them.
	//
	// From here on the steps are not one atomic step: if one fails, or the
	// Job is killed, after another succeeded, the volume holds part of the
	// snapshot or part of the old files. While spec.bootstrapFromSnapshot
	// is set, the operator keeps the StatefulSet at 0 and the engine does
	// not start on that, and the Job's retry starts over and completes the
	// restore; a cluster whose field is removed after a failed bootstrap
	// starts on whatever is there. So that state is logged, to be restored
	// again.
	changed := false
	partial := func(err error) error {
		if changed {
			log.Printf("%s now holds a partial restore, with some of the old files removed or replaced: restore it again before starting the engine on it", dataDir)
		}
		return err
	}
	// The engine's state files go first, before the restored files take
	// their names, so that the restored dump.kdb and appendonly.aof are
	// never next to them. The other way round, a Job killed between the
	// renames and the removals would leave a volume that looks restored,
	// and that the engine reads through the earlier cluster's save number
	// and replays the earlier cluster's .prev log onto. Stopped here
	// instead, the volume holds the earlier cluster's dump and log, whose
	// data the restore replaces anyway.
	//
	// dump.kdb.gen goes first: the restored files need it gone, and without
	// it the old ones still load (the engine starts with no save number on
	// a new volume too). appendonly.aof.prev goes before .prev.len and
	// .rewrite.tmp: an engine that finds those two without .prev refuses to
	// start, rather than replaying half a checkpoint.
	for _, name := range engineStateFiles {
		stale := filepath.Join(dataDir, name)
		if err := os.Remove(stale); err == nil {
			changed = true
			log.Printf("removed %s, the earlier cluster's, which the snapshot does not have", stale)
		} else if !os.IsNotExist(err) {
			return partial(fmt.Errorf("removing %s: %w", stale, err))
		}
	}
	// A power cut must not keep a rename below and lose a removal above.
	if changed {
		if err := syncDir(dataDir); err != nil {
			return partial(err)
		}
	}

	// Rename the new files in before removing a stale dump.kdb or
	// appendonly.aof, so those are deleted only once the snapshot is in
	// place.
	restored := map[string]bool{}
	for _, name := range persistenceFiles {
		tmp, ok := temps[name]
		if !ok {
			continue
		}
		dest := filepath.Join(dataDir, name)
		if err := os.Rename(tmp, dest); err != nil {
			return partial(fmt.Errorf("replacing %s: %w", dest, err))
		}
		delete(temps, name)
		restored[name] = true
		changed = true
		log.Printf("wrote %s", dest)
	}
	// After a restore the volume holds the archive's persistence files and
	// no others. The engine loads dump.kdb and replays appendonly.aof on
	// top, so either one left from an earlier cluster on a kept PVC would
	// bring that cluster's data back into the restored one. Removing it,
	// like the rename, needs write access to the directory only.
	for _, name := range persistenceFiles {
		if restored[name] {
			continue
		}
		stale := filepath.Join(dataDir, name)
		if err := os.Remove(stale); err == nil {
			log.Printf("removed %s, which the snapshot does not have", stale)
		} else if !os.IsNotExist(err) {
			return partial(fmt.Errorf("removing %s: %w", stale, err))
		}
	}
	// A rename is durable only once the directory itself is synced.
	return syncDir(dataDir)
}

func restoreTempPrefix(name string) string {
	return ".restore-" + name + "-"
}

// writeRestoreTemp copies one archive entry into a new temp file in dataDir
// and returns its path. The file is created by this process, so this
// process owns it and can set its mode. name is one of the fixed target
// names, never the entry's own.
func writeRestoreTemp(r io.Reader, dataDir, name string) (string, error) {
	fail := func(err error) (string, error) {
		// The old files stay on the volume until the restored ones are
		// complete, which a full volume turns into a confusing failure on
		// a restore that used to fit.
		if errors.Is(err, syscall.ENOSPC) {
			return "", fmt.Errorf("writing %s: %w (the volume needs room for the old and the restored files at once)", name, err)
		}
		return "", fmt.Errorf("writing %s: %w", name, err)
	}
	f, err := os.CreateTemp(dataDir, restoreTempPrefix(name)+"*")
	if err != nil {
		return fail(err)
	}
	tmp := f.Name()
	discard := func(err error) (string, error) {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fail(err)
	}
	// Chmod explicitly: CreateTemp makes the file owner-only, and any mode
	// given at create time would be filtered by the umask.
	if err := f.Chmod(restoredFileMode); err != nil {
		return discard(err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return discard(err)
	}
	if err := f.Sync(); err != nil {
		return discard(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fail(err)
	}
	return tmp, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("syncing %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil && !dirSyncUnsupported(err) {
		_ = d.Close()
		return fmt.Errorf("syncing %s: %w", dir, err)
	}
	return d.Close()
}

// dirSyncUnsupported reports whether a directory fsync failed only because
// the filesystem does not do it, as some FUSE and network filesystems do.
// The files themselves were synced; there is nothing more to make durable
// there, and failing the restore for it would leave the cluster empty.
func dirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP)
}
