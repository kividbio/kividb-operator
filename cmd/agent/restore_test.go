package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
)

type restoreTestEntry struct {
	name string
	body []byte
}

// restoreTestArchive builds a snapshot archive with the entries in order,
// the way a backup writes it.
func restoreTestArchive(t *testing.T, entries ...restoreTestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func restoreTestWriteFile(t *testing.T, path string, body []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func restoreTestAssertFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Base(path), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s holds %d bytes %.20q, want %d bytes %.20q", filepath.Base(path), len(got), got, len(want), want)
	}
}

func restoreTestAssertGone(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s still exists after the restore (stat err: %v)", filepath.Base(path), err)
	}
}

// restoreTestAssertDir fails if dataDir holds anything but the named files,
// such as a temp file a restore left behind.
func restoreTestAssertDir(t *testing.T, dataDir string, want ...string) {
	t.Helper()
	ents, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range ents {
		got = append(got, e.Name())
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("data dir holds %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("data dir holds %q, want %q", got, want)
		}
	}
}

// The engine writes dump.kdb as its own UID, and the restore Job runs as
// another one, so it can neither chmod nor (with a read-only file) open the
// existing file for writing. A read-only file stands in for that here,
// since a test cannot chown a file to another UID without root.
func TestExtractSnapshot_ReplacesFileItCannotWrite(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write a read-only file")
	}
	dir := t.TempDir()
	dump := filepath.Join(dir, "dump.kdb")
	restoreTestWriteFile(t, dump, []byte("old cluster"), 0o400)

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("restored snapshot")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, dump, []byte("restored snapshot"))
	fi, err := os.Stat(dump)
	if err != nil {
		t.Fatal(err)
	}
	if mode := fi.Mode().Perm(); mode != restoredFileMode {
		t.Fatalf("dump.kdb mode %o, want %o", mode, restoredFileMode)
	}
	restoreTestAssertDir(t, dir, "dump.kdb")
}

// An archive that breaks after its first entry must leave the volume as it
// was: the old files whole, and no temp file behind.
func TestExtractSnapshot_TruncatedArchiveKeepsOldFiles(t *testing.T) {
	dir := t.TempDir()
	dump := filepath.Join(dir, "dump.kdb")
	aof := filepath.Join(dir, "appendonly.aof")
	restoreTestWriteFile(t, dump, []byte("old dump"), 0o660)
	restoreTestWriteFile(t, aof, []byte("old log"), 0o660)

	// Random bytes do not compress, so cutting the archive in half lands
	// inside the second entry, after the first one was read in full.
	big := make([]byte, 256<<10)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")}, restoreTestEntry{"appendonly.aof", big})
	archive = archive[:len(archive)/2]

	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err == nil {
		t.Fatal("extract of a truncated archive succeeded")
	}
	restoreTestAssertFile(t, dump, []byte("old dump"))
	restoreTestAssertFile(t, aof, []byte("old log"))
	restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof")
}

// An appendonly.aof left from an earlier cluster on a kept PVC would be
// replayed on top of the restored snapshot, bringing that cluster's data
// back.
func TestExtractSnapshot_RemovesStaleAppendOnlyFile(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)
	restoreTestWriteFile(t, filepath.Join(dir, "appendonly.aof"), []byte("old log"), 0o660)

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
	restoreTestAssertGone(t, filepath.Join(dir, "appendonly.aof"))
	restoreTestAssertDir(t, dir, "dump.kdb")
}

// The engine loads dump.kdb and replays appendonly.aof on top, so an old
// dump.kdb under a restored log would mix the two clusters' data.
func TestExtractSnapshot_RemovesStaleDump(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)

	archive := restoreTestArchive(t, restoreTestEntry{"appendonly.aof", []byte("new log")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
	restoreTestAssertGone(t, filepath.Join(dir, "dump.kdb"))
	restoreTestAssertDir(t, dir, "appendonly.aof")
}

func TestExtractSnapshot_BothFilesNoTempLeft(t *testing.T) {
	dir := t.TempDir()
	archive := restoreTestArchive(t,
		restoreTestEntry{"./dump.kdb", []byte("new dump")},
		restoreTestEntry{"other.txt", []byte("ignored")},
		restoreTestEntry{"appendonly.aof", []byte("new log")},
	)
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
	restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof")
}

// A name that appears twice is restored from its last entry, and the
// first one's temp file does not stay on the volume.
func TestExtractSnapshot_DuplicateEntryLastWins(t *testing.T) {
	dir := t.TempDir()
	archive := restoreTestArchive(t,
		restoreTestEntry{"dump.kdb", []byte("first")},
		restoreTestEntry{"dump.kdb", []byte("second")},
	)
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("second"))
	restoreTestAssertDir(t, dir, "dump.kdb")
}

// An archive without either persistence file fails and leaves the volume
// alone, stale files included.
func TestExtractSnapshot_NoPersistenceFilesLeavesVolume(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)
	restoreTestWriteFile(t, filepath.Join(dir, "appendonly.aof"), []byte("old log"), 0o660)

	archive := restoreTestArchive(t, restoreTestEntry{"../../etc/passwd", []byte("x")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err == nil {
		t.Fatal("extract of an archive without dump.kdb or appendonly.aof succeeded")
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"))
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("old log"))
	restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof")
}

// A gzip stream whose checksum does not match was damaged in transit or at
// rest, even if its tar entries read cleanly, and is not restored.
func TestExtractSnapshot_BadGzipChecksumKeepsOldFiles(t *testing.T) {
	dir := t.TempDir()
	dump := filepath.Join(dir, "dump.kdb")
	restoreTestWriteFile(t, dump, []byte("old dump"), 0o660)

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	archive[len(archive)-8] ^= 0xff // the CRC-32 in the gzip trailer

	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err == nil {
		t.Fatal("extract of an archive with a bad gzip checksum succeeded")
	}
	restoreTestAssertFile(t, dump, []byte("old dump"))
	restoreTestAssertDir(t, dir, "dump.kdb")
}

// restoreTestCaptureLog collects what the code under test logs.
func restoreTestCaptureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

func restoreTestAssertErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v, want one that contains %q", err, want)
	}
}

// restoreTestBlockPath puts a non-empty directory where a file is expected,
// so that renaming over it or removing it fails.
func restoreTestBlockPath(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(path, "x"), 0o750); err != nil {
		t.Fatal(err)
	}
}

// A Job killed while writing (OOM, eviction) leaves its temp files behind,
// and the retry would otherwise need room for one more full copy.
func TestExtractSnapshot_RemovesTempsLeftByKilledJob(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, ".restore-dump.kdb-123"), []byte("half"), 0o600)
	restoreTestWriteFile(t, filepath.Join(dir, ".restore-appendonly.aof-456"), []byte("half"), 0o600)
	restoreTestWriteFile(t, filepath.Join(dir, ".restore-other-1"), []byte("not ours"), 0o600)

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertDir(t, dir, "dump.kdb", ".restore-other-1")
}

type restoreTestFailingReader struct{ err error }

func (r restoreTestFailingReader) Read([]byte) (int, error) { return 0, r.err }

// The old files stay until the restored ones are complete, so the volume
// needs room for both, and a full volume has to say so.
func TestWriteRestoreTemp_NoSpaceSaysWhy(t *testing.T) {
	dir := t.TempDir()
	noSpace := &fs.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC}
	_, err := writeRestoreTemp(restoreTestFailingReader{noSpace}, dir, "dump.kdb")
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("error %v, want ENOSPC", err)
	}
	restoreTestAssertErr(t, err, "writing dump.kdb")
	restoreTestAssertErr(t, err, "room for the old and the restored files")
	restoreTestAssertDir(t, dir)
}

func TestExtractSnapshot_TrailingGarbageNamesTheStep(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)
	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	archive = append(archive, []byte("garbage")...)

	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "gzip trailer")
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"))
	restoreTestAssertDir(t, dir, "dump.kdb")
}

func TestExtractSnapshot_CreateTempFailureNamesTheFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write a read-only directory")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "writing dump.kdb")
}

// A rename that fails after another one succeeded leaves a volume that is
// neither the old data nor the snapshot, and the log has to say so.
func TestExtractSnapshot_RenameFailureAfterRenameLogsPartial(t *testing.T) {
	dir := t.TempDir()
	logged := restoreTestCaptureLog(t)
	restoreTestBlockPath(t, filepath.Join(dir, "appendonly.aof"))

	archive := restoreTestArchive(t,
		restoreTestEntry{"dump.kdb", []byte("new dump")},
		restoreTestEntry{"appendonly.aof", []byte("new log")},
	)
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "replacing "+filepath.Join(dir, "appendonly.aof"))
	if !strings.Contains(logged.String(), "partial restore") {
		t.Fatalf("log %q does not say the volume holds a partial restore", logged)
	}
}

func TestExtractSnapshot_StaleRemovalFailureLogsPartial(t *testing.T) {
	dir := t.TempDir()
	logged := restoreTestCaptureLog(t)
	restoreTestBlockPath(t, filepath.Join(dir, "appendonly.aof"))

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "removing "+filepath.Join(dir, "appendonly.aof"))
	if !strings.Contains(logged.String(), "partial restore") {
		t.Fatalf("log %q does not say the volume holds a partial restore", logged)
	}
}

// A first rename that fails has changed nothing, so it is not reported as
// a partial restore.
func TestExtractSnapshot_FirstRenameFailureIsNotPartial(t *testing.T) {
	dir := t.TempDir()
	logged := restoreTestCaptureLog(t)
	restoreTestBlockPath(t, filepath.Join(dir, "dump.kdb"))

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "replacing "+filepath.Join(dir, "dump.kdb"))
	if strings.Contains(logged.String(), "partial restore") {
		t.Fatalf("log %q reports a partial restore though nothing was replaced", logged)
	}
}

func TestSyncDir_FailureNamesTheDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	restoreTestAssertErr(t, syncDir(missing), "syncing "+missing)
}

// Some FUSE and network filesystems cannot fsync a directory; that is not a
// reason to fail a restore whose files were all synced.
func TestDirSyncUnsupported(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&fs.PathError{Op: "sync", Path: "/data", Err: syscall.EINVAL}, true},
		{&fs.PathError{Op: "sync", Path: "/data", Err: syscall.ENOTSUP}, true},
		{&fs.PathError{Op: "sync", Path: "/data", Err: syscall.EIO}, false},
		{&fs.PathError{Op: "sync", Path: "/data", Err: syscall.ENOSPC}, false},
	} {
		if got := dirSyncUnsupported(tc.err); got != tc.want {
			t.Errorf("dirSyncUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
