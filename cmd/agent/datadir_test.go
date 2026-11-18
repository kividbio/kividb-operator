package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// dataDirTestVolume is a volume root as the kubelet leaves it with fsGroup:
// group-writable and setgid. Running as root (the CI container), it also
// gets another group than ours without setgid, so that the group a new
// directory gets is the one makeDataDir gives it, not one it inherited.
func dataDirTestVolume(t *testing.T) (string, int) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("directory groups and modes are Unix")
	}
	vol := t.TempDir()
	gid := os.Getgid()
	mode := os.FileMode(0o775) | os.ModeSetgid
	if os.Getuid() == 0 {
		gid, mode = 1000, 0o770
		if err := os.Chown(vol, -1, gid); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(vol, mode); err != nil {
		t.Fatal(err)
	}
	return vol, gid
}

// The engine changes into `dir` at start and does not create it: a new
// volume with `dir sub` exits at once (#112). makeDataDir makes it, and
// the directories above it, each with the volume's group and mode, which
// the engine (another user, in that group) needs to write there.
func TestMakeDataDir_MakesItWithTheVolumesGroupAndMode(t *testing.T) {
	vol, gid := dataDirTestVolume(t)
	root, err := os.Stat(vol)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(vol, "a", "b")
	made, err := makeDataDir(vol, dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Join(vol, "a"), dir}; strings.Join(made, " ") != strings.Join(want, " ") {
		t.Fatalf("made %v, want %v", made, want)
	}
	for _, p := range made {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() || fi.Mode() != root.Mode() {
			t.Errorf("%s: mode %v, want the volume's, %v", p, fi.Mode(), root.Mode())
		}
		if g, ok := fileGroup(fi); !ok || g != gid {
			t.Errorf("%s: group %d, want the volume's, %d", p, g, gid)
		}
	}
}

// A volume the kubelet did not give the pod's fsGroup (hostPath, kind's
// local-path) can be in a group the agent is not in, which it cannot give:
// the directory is still made, with the volume's mode, and keeps the
// agent's group. The system's temp directory, root's, stands in for it.
func TestMakeDataDir_VolumeInAGroupTheAgentIsNotIn(t *testing.T) {
	if runtime.GOOS == "windows" || os.Getuid() == 0 {
		t.Skip("needs a Unix user that is not root")
	}
	vol := os.TempDir()
	root, err := os.Stat(vol)
	if err != nil {
		t.Fatal(err)
	}
	gid, ok := fileGroup(root)
	groups, _ := os.Getgroups()
	if !ok || gid == os.Getegid() || slices.Contains(groups, gid) {
		t.Skipf("%s is in group %d, which this user is in", vol, gid)
	}
	dir := filepath.Join(vol, fmt.Sprintf("kividb-make-data-dir-%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(dir) })
	made, err := makeDataDir(vol, dir)
	if err != nil || len(made) != 1 {
		t.Fatalf("made %v, %v; want %s made", made, err, dir)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode() != root.Mode() {
		t.Errorf("%s: mode %v, want the volume's, %v", dir, fi.Mode(), root.Mode())
	}
	if g, _ := fileGroup(fi); g != os.Getegid() {
		t.Errorf("%s: group %d, want the agent's, %d", dir, g, os.Getegid())
	}
}

// A cluster already running with `dir sub` has the directory, with the
// engine's files in it: it is left as it is, its mode too.
func TestMakeDataDir_LeavesADirThatIsThere(t *testing.T) {
	vol, _ := dataDirTestVolume(t)
	dir := filepath.Join(vol, "sub")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dump.kdb"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	made, err := makeDataDir(vol, dir)
	if err != nil || len(made) != 0 {
		t.Fatalf("made %v, %v; want nothing", made, err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("%s: %v, %v; want it left at 0700", dir, fi.Mode(), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "dump.kdb")); err != nil {
		t.Fatal(err)
	}
	if made, err := makeDataDir(vol, vol); err != nil || len(made) != 0 {
		t.Fatalf("the volume itself: made %v, %v; want nothing", made, err)
	}
}

// Only a directory under the volume is ever made: one off it (after
// cleaning "..") is refused, and so is a path through something that is
// not a directory, a symbolic link included, which could lead off it.
func TestMakeDataDir_MakesNothingOffTheVolume(t *testing.T) {
	vol, _ := dataDirTestVolume(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(vol, "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(vol, "link")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		dir  string
		want string
	}{
		{filepath.Join(vol, "..", "x"), "not under " + vol},
		{outside, "not under " + vol},
		{filepath.Join(vol, "file", "sub"), "not a directory"},
		{filepath.Join(vol, "link", "sub"), "not a directory"},
	} {
		made, err := makeDataDir(vol, tc.dir)
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(made) != 0 {
			t.Errorf("%s: made %v, %v; want nothing made and an error saying %q", tc.dir, made, err, tc.want)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, "sub")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s made through the link: %v", filepath.Join(outside, "sub"), err)
	}
}

// A restore into a `dir` under the volume (#112) makes that directory as
// the init container would, then extracts into it.
func TestRestoreDir_MakesTheEnginesDir(t *testing.T) {
	vol, _ := dataDirTestVolume(t)
	root, err := os.Stat(vol)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(vol, "sub")
	if err := prepareRestoreDir(vol, dir); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode() != root.Mode() {
		t.Fatalf("%s: %v, %v; want the volume's mode, %v", dir, fi, err, root.Mode())
	}
	archive := restoreTestArchive(t, restoreTestEntry{name: "dump.kdb", body: []byte("snapshot")})
	if err := extractSnapshotTarGz(strings.NewReader(string(archive)), dir); err != nil {
		t.Fatal(err)
	}
	restoreTestAssertFile(t, filepath.Join(dir, "dump.kdb"), []byte("snapshot"))
	if _, err := os.Stat(filepath.Join(vol, "dump.kdb")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("dump.kdb restored into the volume root too: %v", err)
	}
}
