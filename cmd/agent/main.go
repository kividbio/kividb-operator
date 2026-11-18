// Command agent is the kividb-operator sidecar. It runs as a second
// container in every kividb pod (see internal/controller/statefulset.go),
// sharing the pod's network namespace and data volume. It exposes an HTTP
// API the operator's controller calls to drive replication role changes
// and trigger backups, since kividb itself speaks only the RESP protocol
// and has no HTTP admin surface of its own.
//
// Usage:
//
//	agent serve                                    # run the HTTP sidecar (default)
//	agent backup-trigger --url <master-svc-url>    # one-shot CronJob client
//	agent make-data-dir --data-dir <dir>           # init container: make the engine's dir
package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd = args[0]
		args = args[1:]
	}

	var err error
	switch cmd {
	case "serve":
		err = runServe(args)
	case "backup-trigger":
		err = runBackupTrigger(args)
	case "restore-from-s3":
		err = runRestoreFromS3(args)
	case "make-data-dir":
		err = runMakeDataDir(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q (expected \"serve\", \"backup-trigger\", \"restore-from-s3\" or \"make-data-dir\")\n", cmd)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}
