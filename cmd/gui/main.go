// Command gui is a read-only web dashboard for KividbCluster objects. It is
// intentionally a single static Go binary with no Node/npm build step: the
// HTML/CSS/JS under static/ are plain files served as-is (embedded into the
// binary via embed.FS), and the only "framework" involved is vanilla
// fetch()/setInterval() JavaScript for auto-refresh.
//
// The GUI never reads Secrets. It only ever calls:
//   - get/list on kividbclusters.kividb.io (via a controller-runtime client)
//   - get/list on pods, services, statefulsets.apps, cronjobs.batch, and
//     events (via a client-go clientset)
//
// See docs/GUI.md for the exact RBAC this requires and how to deploy it.
package main

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
)

const defaultPort = 8090

//go:embed static
var staticFiles embed.FS

// staticSub is staticFiles rooted at "static/" so http.FileServerFS serves
// e.g. static/app.js at /static/app.js once mounted with StripPrefix.
var staticSub = mustSub(staticFiles, "static")

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		log.Fatalf("embed static assets: %v", err)
	}
	return sub
}

func main() {
	port := defaultPort
	if v := os.Getenv("GUI_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p <= 0 || p > 65535 {
			log.Fatalf("invalid GUI_PORT %q: must be a port number 1-65535", v)
		}
		port = p
	}

	watchNamespace := os.Getenv("WATCH_NAMESPACE")

	cfg, err := loadKubeConfig()
	if err != nil {
		log.Fatalf("building kubeconfig (tried in-cluster, then default kubeconfig loading rules): %v", err)
	}

	ctrlClient, err := newControllerRuntimeClient(cfg)
	if err != nil {
		log.Fatalf("building KividbCluster client: %v", err)
	}

	clientset, err := newClientset(cfg)
	if err != nil {
		log.Fatalf("building Kubernetes clientset: %v", err)
	}

	srv := &server{
		ctrlClient:     ctrlClient,
		clientset:      clientset,
		watchNamespace: watchNamespace,
	}

	addr := fmt.Sprintf(":%d", port)
	scope := "all namespaces"
	if watchNamespace != "" {
		scope = fmt.Sprintf("namespace %q", watchNamespace)
	}
	log.Printf("kividb-operator GUI listening on %s (watching %s)", addr, scope)

	if err := http.ListenAndServe(addr, srv.routes()); err != nil {
		log.Fatal(err)
	}
}
