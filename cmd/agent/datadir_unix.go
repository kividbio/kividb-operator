//go:build unix

package main

import (
	"os"
	"syscall"
)

// fileGroup is fi's group ID.
func fileGroup(fi os.FileInfo) (int, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Gid), true
}
