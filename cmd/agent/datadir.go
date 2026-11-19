package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// runMakeDataDir makes the engine's `dir` on the pod's volume before the
// engine starts. The operator runs it as an init container when a
// KividbConfig's `dir` is a strict subdirectory of the volume (#112): the
// engine changes into `dir` at start and does not create it, so on a new
// volume `dir sub` exits at once ("Can't chdir to 'sub'").
func runMakeDataDir(args []string) error {
	fs := flag.NewFlagSet("make-data-dir", flag.ExitOnError)
	volume := fs.String("volume", "/data", "the pod's data volume: what is made is under it, with its group and mode")
	dataDir := fs.String("data-dir", "", "the directory the engine keeps its files in, under --volume")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataDir == "" {
		return fmt.Errorf("--data-dir is required")
	}
	made, err := makeDataDir(*volume, *dataDir)
	for _, p := range made {
		log.Printf("made %s", p)
	}
	if err == nil && len(made) == 0 {
		log.Printf("%s is there already", *dataDir)
	}
	return err
}

// makeDataDir makes dir, under volume, and the directories above it that
// are missing, and returns them. Each gets the volume's group and mode: the
// kubelet sets them on the volume (fsGroup) for every container in the pod,
// and the engine, which runs as another user than the agent, writes its
// files there through that group. The owner is the agent's own user: a
// process that is not root cannot give a directory to another one (see
// volumeGroup for a group it is not in).
//
// Nothing off the volume is ever made: dir must be under it once cleaned,
// and a path through anything that is not a directory, a symbolic link
// included, is refused, since a link could lead off the volume. A dir that
// is there already, as on a cluster that has run with it, is left as it is.
func makeDataDir(volume, dir string) ([]string, error) {
	volume, dir = filepath.Clean(volume), filepath.Clean(dir)
	rel, err := filepath.Rel(volume, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, fmt.Errorf("%s is not under %s, the pod's volume: not made", dir, volume)
	}
	root, err := os.Stat(volume)
	if err != nil {
		return nil, err
	}
	mode := root.Mode() & (os.ModePerm | os.ModeSetgid | os.ModeSticky)
	gid, hasGroup := fileGroup(root)

	var made []string
	p := volume
	for _, name := range strings.Split(rel, string(filepath.Separator)) {
		p = filepath.Join(p, name)
		fi, err := os.Lstat(p)
		switch {
		case err == nil && fi.IsDir():
			continue
		case err == nil:
			return made, fmt.Errorf("%s is in the way of %s: it is not a directory (%s)", p, dir, fi.Mode().Type())
		case !errors.Is(err, fs.ErrNotExist):
			return made, err
		}
		if err := os.Mkdir(p, mode.Perm()); err != nil {
			return made, err
		}
		made = append(made, p)
		// The group first: a change of group can clear the setgid bit.
		if hasGroup {
			if err := volumeGroup(p, gid); err != nil {
				return made, fmt.Errorf("giving %s the volume's group: %w", p, err)
			}
		}
		// Mkdir's mode is cut by the umask, and leaves out setgid.
		if err := os.Chmod(p, mode); err != nil {
			return made, fmt.Errorf("giving %s the volume's mode: %w", p, err)
		}
	}
	return made, nil
}

// volumeGroup gives the directory p the volume's group, gid. On a volume
// the kubelet gave the pod's fsGroup, which is setgid, p has it already.
// On one it did not (hostPath, kind's local-path), the volume's group may
// be one the agent is not in, which it cannot give: p then keeps the
// agent's group, with the volume's mode, and the log says so.
func volumeGroup(p string, gid int) error {
	fi, err := os.Lstat(p)
	if err != nil {
		return err
	}
	has, ok := fileGroup(fi)
	if !ok || has == gid {
		return nil
	}
	if err := os.Lchown(p, -1, gid); errors.Is(err, fs.ErrPermission) {
		log.Printf("%s keeps group %d: the agent is not in the volume's group, %d", p, has, gid)
	} else if err != nil {
		return err
	}
	return nil
}
