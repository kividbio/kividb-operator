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
	restoreTestStopRenameOf(t, filepath.Join(dir, "appendonly.aof"))

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

// An earlier cluster's log that cannot be removed stops the restore before
// the restored dump goes in: the restored dump is never left with the old
// log, which the engine would replay over it with dump.kdb.gen gone (#58).
func TestExtractSnapshot_StaleRemovalFailureLogsPartial(t *testing.T) {
	dir := t.TempDir()
	logged := restoreTestCaptureLog(t)
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb.gen"), []byte("old gen"), 0o660)
	restoreTestBlockPath(t, filepath.Join(dir, "appendonly.aof"))

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "removing "+filepath.Join(dir, "appendonly.aof"))
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"))
	restoreTestAssertGone(t, filepath.Join(dir, "dump.kdb.gen"))
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

// restoreTestKeptVolume fills dir the way an earlier cluster's engine left
// a kept PVC: its dump and log, the state files that describe them, the
// logs engine 1.2.1 kept aside, temp files, a Redis dump, and its lock
// file. It returns every file name and the content each was given.
func restoreTestKeptVolume(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	old := map[string][]byte{
		"dump.kdb":       []byte("old dump"),
		"appendonly.aof": []byte("old log"),
		"kividb.lock":    nil,
	}
	others := []string{
		"superseded.1762000000000.0.appendonly.aof",
		"superseded.1762000000000.1.appendonly.aof.prev",
		"appendonly.aof.tmp",
		"dump.kdb.1234.0.tmp",
		"dump.kdb.gen.tmp",
		"dump.rdb",
	}
	for _, name := range append(others, engineStateFiles...) {
		old[name] = []byte("old " + name)
	}
	for name, body := range old {
		restoreTestWriteFile(t, filepath.Join(dir, name), body, 0o660)
	}
	return old
}

// A kept volume holds the earlier cluster's save number (dump.kdb.gen) and
// what a checkpoint it did not finish left (appendonly.aof.prev and the
// rest). Read with the restored files, the save number makes the engine
// refuse to start or skip the restored log, and .prev is replayed on top
// of the restored dump. The restore leaves the snapshot's files only, and
// the unheld lock file.
func TestExtractSnapshot_KeptVolumeEndsWithTheSnapshotsFilesOnly(t *testing.T) {
	dir := t.TempDir()
	restoreTestKeptVolume(t, dir)

	archive := restoreTestArchive(t,
		restoreTestEntry{"dump.kdb", []byte("new dump")},
		restoreTestEntry{"appendonly.aof", []byte("new log")},
	)
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
	for _, name := range engineStateFiles {
		restoreTestAssertGone(t, filepath.Join(dir, name))
	}
	restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof", "kividb.lock")
}

// An archive that breaks leaves the old set whole: the engine's state files
// describe the old dump and log, and are kept with them.
func TestExtractSnapshot_TruncatedArchiveKeepsEngineStateFiles(t *testing.T) {
	dir := t.TempDir()
	old := restoreTestKeptVolume(t, dir)

	big := make([]byte, 256<<10)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")}, restoreTestEntry{"appendonly.aof", big})
	archive = archive[:len(archive)/2]

	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err == nil {
		t.Fatal("extract of a truncated archive succeeded")
	}
	var names []string
	for name, body := range old {
		restoreTestAssertFile(t, filepath.Join(dir, name), body)
		names = append(names, name)
	}
	restoreTestAssertDir(t, dir, names...)
}

// The state files go before the restored files take their names, so a
// failure among them leaves the old dump and log in place, never the
// restored ones next to the earlier cluster's state files.
func TestExtractSnapshot_StateFileRemovalFailureKeepsOldDump(t *testing.T) {
	dir := t.TempDir()
	logged := restoreTestCaptureLog(t)
	old := restoreTestKeptVolume(t, dir)
	blocked := filepath.Join(dir, "appendonly.aof.prev.len")
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	restoreTestBlockPath(t, blocked)

	archive := restoreTestArchive(t,
		restoreTestEntry{"dump.kdb", []byte("new dump")},
		restoreTestEntry{"appendonly.aof", []byte("new log")},
	)
	err := extractSnapshotTarGz(bytes.NewReader(archive), dir)
	restoreTestAssertErr(t, err, "removing "+blocked)
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"))
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("old log"))
	restoreTestAssertGone(t, filepath.Join(dir, "dump.kdb.gen"))
	restoreTestAssertGone(t, filepath.Join(dir, "appendonly.aof.prev"))
	var left []string
	for name := range old {
		if name != "dump.kdb.gen" && name != "appendonly.aof.prev" {
			left = append(left, name)
		}
	}
	restoreTestAssertDir(t, dir, left...)
	if !strings.Contains(logged.String(), "partial restore") {
		t.Fatalf("log %q does not say the volume holds a partial restore", logged)
	}
}

// restoreTestStopRenameOf makes the restore's rename of dest fail, the way a
// Job killed between two renames stops, until the test ends.
func restoreTestStopRenameOf(t *testing.T, dest string) {
	t.Helper()
	restoreTestHookRename(t, func(_, to string) error {
		if to == dest {
			return &fs.PathError{Op: "rename", Path: to, Err: syscall.EIO}
		}
		return nil
	})
}

// restoreTestHookRename calls before ahead of each rename of a restored
// file into place; an error from it is that rename's.
func restoreTestHookRename(t *testing.T, before func(from, to string) error) {
	t.Helper()
	t.Cleanup(func() { restoreRename = os.Rename })
	restoreRename = func(from, to string) error {
		if err := before(from, to); err != nil {
			return err
		}
		return os.Rename(from, to)
	}
}

// restoreTestNames lists dataDir's file names, sorted.
func restoreTestNames(t *testing.T, dataDir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func TestEarlierEngineFile(t *testing.T) {
	for name, want := range map[string]bool{
		"appendonly.aof":                                 true,
		"appendonly.aof.tmp":                             true,
		"appendonly.aof.manifest":                        true,
		"appendonly.aof.1.base.kdb":                      true,
		"appendonly.aof.2.incr.aof":                      true,
		"superseded.1762000000000.0.appendonly.aof":      true,
		"superseded.1762000000000.1.appendonly.aof.prev": true,
		"dump.kdb.1234.0.tmp":                            true,
		"dump.kdb.gen.tmp":                               true,
		"dump.rdb":                                       true,
		"dump.kdb":                                       false,
		"kividb.lock":                                    false,
		"lost+found":                                     false,
		".restore-dump.kdb-123":                          false,
		".restore-appendonly.aof-456":                    false,
		"my-appendonly.aof":                              false,
	} {
		if got := earlierEngineFile(name); got != want {
			t.Errorf("earlierEngineFile(%q) = %v, want %v", name, got, want)
		}
	}
}

// Issue #58: with dump.kdb.gen gone, which the restore removes, engine 1.2.0
// and 1.2.1 both replay appendonly.aof over dump.kdb, and 1.2.1 keeps the
// logs it left out as superseded.* files that it never removes. Every log of
// the earlier cluster's, and the engine's other leftovers, go: the volume
// holds the snapshot's dump, the unheld lock file, and what is not the
// engine's (lost+found).
func TestExtractSnapshot_RemovesEveryFileOfTheEarlierEngine(t *testing.T) {
	dir := t.TempDir()
	restoreTestKeptVolume(t, dir)
	if err := os.Mkdir(filepath.Join(dir, "lost+found"), 0o700); err != nil {
		t.Fatal(err)
	}

	archive := restoreTestArchive(t, restoreTestEntry{"dump.kdb", []byte("new dump")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
	restoreTestAssertDir(t, dir, "dump.kdb", "kividb.lock", "lost+found")
}

// The restored files are renamed in only once every file of the earlier
// cluster's is gone, so a Job stopped between any two steps never leaves a
// restored file next to the earlier cluster's logs or state files; and the
// restored log goes in only once the restored dump has.
func TestExtractSnapshot_RestoredFilesNeverBesideTheEarlierLogs(t *testing.T) {
	dir := t.TempDir()
	restoreTestKeptVolume(t, dir)
	var renamed []string
	restoreTestHookRename(t, func(_, to string) error {
		for _, name := range restoreTestNames(t, dir) {
			if earlierEngineFile(name) {
				t.Errorf("%s is still there when %s is renamed in", name, filepath.Base(to))
			}
		}
		if filepath.Base(to) == "appendonly.aof" {
			restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
		}
		renamed = append(renamed, filepath.Base(to))
		return nil
	})

	archive := restoreTestArchive(t,
		restoreTestEntry{"appendonly.aof", []byte("new log")},
		restoreTestEntry{"dump.kdb", []byte("new dump")},
	)
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(renamed) != 2 || renamed[0] != "dump.kdb" || renamed[1] != "appendonly.aof" {
		t.Fatalf("renamed %q, want dump.kdb then appendonly.aof", renamed)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
	restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof", "kividb.lock")
}

// A Job stopped after the restored dump went in, and before the restored
// log did, leaves the restored dump with no log at all: not the earlier
// cluster's, which the engine would replay over it. Its retry, and any run
// after that, end with the snapshot's files only.
func TestExtractSnapshot_RetryAfterStoppedJobEndsTheSame(t *testing.T) {
	dir := t.TempDir()
	restoreTestKeptVolume(t, dir)
	archive := restoreTestArchive(t,
		restoreTestEntry{"dump.kdb", []byte("new dump")},
		restoreTestEntry{"appendonly.aof", []byte("new log")},
	)

	restoreTestStopRenameOf(t, filepath.Join(dir, "appendonly.aof"))
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err == nil {
		t.Fatal("extract with the log's rename stopped succeeded")
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
	restoreTestAssertGone(t, filepath.Join(dir, "appendonly.aof"))
	restoreTestAssertDir(t, dir, "dump.kdb", "kividb.lock")
	// A killed Job does not get to remove its temp file.
	restoreTestWriteFile(t, filepath.Join(dir, ".restore-appendonly.aof-999"), []byte("new log"), 0o600)

	restoreRename = os.Rename
	for attempt := 0; attempt < 2; attempt++ {
		if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
			t.Fatalf("extract, run %d after the stopped one: %v", attempt+1, err)
		}
		restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("new dump"))
		restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
		restoreTestAssertDir(t, dir, "dump.kdb", "appendonly.aof", "kividb.lock")
	}
}

// An archive with a log and no dump: the engine would load a Redis dump.rdb
// in place of the missing dump.kdb and replay the restored log over it.
func TestExtractSnapshot_LogOnlyArchiveRemovesEveryDump(t *testing.T) {
	dir := t.TempDir()
	restoreTestWriteFile(t, filepath.Join(dir, "dump.kdb"), []byte("old dump"), 0o660)
	restoreTestWriteFile(t, filepath.Join(dir, "dump.rdb"), []byte("old redis dump"), 0o660)

	archive := restoreTestArchive(t, restoreTestEntry{"appendonly.aof", []byte("new log")})
	if err := extractSnapshotTarGz(bytes.NewReader(archive), dir); err != nil {
		t.Fatalf("extract: %v", err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "appendonly.aof"), []byte("new log"))
	restoreTestAssertDir(t, dir, "appendonly.aof")
}
