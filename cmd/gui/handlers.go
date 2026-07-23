package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// server holds everything the HTTP handlers need. It is deliberately tiny:
// two Kubernetes clients and the optional namespace restriction.
type server struct {
	ctrlClient     client.Client        // KividbCluster only
	clientset      kubernetes.Interface // Pods, Services, StatefulSets, CronJobs, Events -- never Secrets
	watchNamespace string               // "" means all namespaces
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// serveEmbeddedHTML writes one of the embedded static/*.html files verbatim.
// The dashboard and detail pages are plain static shells; all data comes in
// afterwards via fetch() calls to the JSON API below, which is also what
// drives the 10s auto-refresh.
func serveEmbeddedHTML(w http.ResponseWriter, name string) {
	data, err := staticFiles.ReadFile(name)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Printf("read embedded file %s: %v", name, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	serveEmbeddedHTML(w, "static/index.html")
}

func (s *server) handleClusterPage(w http.ResponseWriter, r *http.Request) {
	serveEmbeddedHTML(w, "static/detail.html")
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleAPIClusters(w http.ResponseWriter, r *http.Request) {
	summaries, err := listClusterSummaries(r.Context(), s.ctrlClient, s.watchNamespace)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, summaries)
}

func (s *server) handleAPIClusterDetail(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	if s.watchNamespace != "" && namespace != s.watchNamespace {
		writeJSONError(w, http.StatusForbidden, fmt.Errorf("namespace %q is not watched by this GUI instance (WATCH_NAMESPACE=%q)", namespace, s.watchNamespace))
		return
	}

	detail, err := getClusterDetail(r.Context(), s.ctrlClient, s.clientset, namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			writeJSONError(w, http.StatusNotFound, err)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /clusters/{namespace}/{name}", s.handleClusterPage)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/clusters", s.handleAPIClusters)
	mux.HandleFunc("GET /api/clusters/{namespace}/{name}", s.handleAPIClusterDetail)
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(staticSub)))

	return mux
}
