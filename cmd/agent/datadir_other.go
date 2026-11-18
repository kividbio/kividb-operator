//go:build !unix

package main

import "os"

// fileGroup is fi's group ID: none off Unix, where the agent does not run.
func fileGroup(os.FileInfo) (int, bool) { return 0, false }
