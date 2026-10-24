package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	metricsTTL          = 24 * time.Hour
	metricsScrapeEvery  = 15 * time.Second
	metricsPersistEvery = 1 * time.Minute
)

// metricPoint is one scraped gauge at a wall-clock instant.
type metricPoint struct {
	T int64   `json:"t"` // unix milliseconds
	V float64 `json:"v"`
}

type podSeries struct {
	Memory     []metricPoint `json:"memory"`
	Clients    []metricPoint `json:"clients"`
	ReplOffset []metricPoint `json:"replOffset"`
	Commands   []metricPoint `json:"commands"` // total_commands_processed (counter; chart diffs client-side)
}

type metricsSnapshot struct {
	// Series keyed by "namespace/cluster/pod".
	Series map[string]*podSeries `json:"series"`
}

// metricsStore holds ~24h of agent scrapes, optionally persisted under
// GUI_METRICS_DIR (chart PVC). No Prometheus dependency.
type metricsStore struct {
	mu          sync.RWMutex
	series      map[string]*podSeries
	dataDir     string
	persistPath string
}

func newMetricsStore(dataDir string) *metricsStore {
	ms := &metricsStore{
		series:      make(map[string]*podSeries),
		dataDir:     dataDir,
		persistPath: filepath.Join(dataDir, "metrics.json"),
	}
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			log.Printf("metrics: mkdir %s: %v (continuing in-memory only)", dataDir, err)
			ms.dataDir = ""
			ms.persistPath = ""
		} else if err := ms.load(); err != nil {
			log.Printf("metrics: load %s: %v (starting empty)", ms.persistPath, err)
		}
	}
	return ms
}

func seriesKey(namespace, cluster, pod string) string {
	return namespace + "/" + cluster + "/" + pod
}

func (ms *metricsStore) load() error {
	if ms.persistPath == "" {
		return nil
	}
	b, err := os.ReadFile(ms.persistPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap metricsSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	if snap.Series == nil {
		return nil
	}
	cutoff := time.Now().Add(-metricsTTL).UnixMilli()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for k, s := range snap.Series {
		if s == nil {
			continue
		}
		ms.series[k] = &podSeries{
			Memory:     prunePoints(s.Memory, cutoff),
			Clients:    prunePoints(s.Clients, cutoff),
			ReplOffset: prunePoints(s.ReplOffset, cutoff),
			Commands:   prunePoints(s.Commands, cutoff),
		}
	}
	return nil
}

func (ms *metricsStore) persist() error {
	if ms.persistPath == "" {
		return nil
	}
	ms.mu.RLock()
	snap := metricsSnapshot{Series: make(map[string]*podSeries, len(ms.series))}
	for k, s := range ms.series {
		cp := *s
		snap.Series[k] = &cp
	}
	ms.mu.RUnlock()
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := ms.persistPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, ms.persistPath)
}

func prunePoints(pts []metricPoint, cutoff int64) []metricPoint {
	if len(pts) == 0 {
		return pts
	}
	i := 0
	for i < len(pts) && pts[i].T < cutoff {
		i++
	}
	if i == 0 {
		return pts
	}
	out := make([]metricPoint, len(pts)-i)
	copy(out, pts[i:])
	return out
}

func appendPoint(pts []metricPoint, t int64, v float64, cutoff int64) []metricPoint {
	pts = append(pts, metricPoint{T: t, V: v})
	return prunePoints(pts, cutoff)
}

func (ms *metricsStore) record(namespace, cluster, pod string, t int64, memory, clients, replOffset, commands float64, haveMem, haveClients, haveRepl, haveCmd bool) {
	key := seriesKey(namespace, cluster, pod)
	cutoff := time.Now().Add(-metricsTTL).UnixMilli()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	s := ms.series[key]
	if s == nil {
		s = &podSeries{}
		ms.series[key] = s
	}
	if haveMem {
		s.Memory = appendPoint(s.Memory, t, memory, cutoff)
	}
	if haveClients {
		s.Clients = appendPoint(s.Clients, t, clients, cutoff)
	}
	if haveRepl {
		s.ReplOffset = appendPoint(s.ReplOffset, t, replOffset, cutoff)
	}
	if haveCmd {
		s.Commands = appendPoint(s.Commands, t, commands, cutoff)
	}
}

func (ms *metricsStore) query(namespace, cluster string, from, to int64) map[string]*podSeries {
	prefix := namespace + "/" + cluster + "/"
	out := make(map[string]*podSeries)
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for k, s := range ms.series {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		pod := strings.TrimPrefix(k, prefix)
		out[pod] = &podSeries{
			Memory:     filterRange(s.Memory, from, to),
			Clients:    filterRange(s.Clients, from, to),
			ReplOffset: filterRange(s.ReplOffset, from, to),
			Commands:   filterRange(s.Commands, from, to),
		}
	}
	return out
}

// statsWindow and statsBucket shape the dashboard's sparklines: the last
// hour, a point a minute. statsFresh is how old a pod's last scrape may be
// and still count as its current value (a few missed scrapes).
const (
	statsWindow = time.Hour
	statsBucket = time.Minute
	statsFresh  = 4 * metricsScrapeEvery
)

// clusterStats sums one cluster's pods for the dashboard, or nil when there
// is nothing scraped for it yet.
func (ms *metricsStore) clusterStats(namespace, cluster string, now time.Time) *ClusterStats {
	prefix := namespace + "/" + cluster + "/"
	from := now.Add(-statsWindow).UnixMilli()
	fresh := now.Add(-statsFresh).UnixMilli()
	bucket := statsBucket.Milliseconds()

	stats := &ClusterStats{}
	memBuckets := map[int64]float64{}
	opsBuckets := map[int64]float64{}
	found := false

	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for k, s := range ms.series {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		found = true
		if n := len(s.Memory); n > 0 && s.Memory[n-1].T >= fresh {
			stats.MemoryBytes += s.Memory[n-1].V
		}
		if n := len(s.Clients); n > 0 && s.Clients[n-1].T >= fresh {
			stats.Clients += s.Clients[n-1].V
		}
		// Memory: the pod's last value in each minute, added across pods.
		last := map[int64]float64{}
		for _, pt := range s.Memory {
			if pt.T >= from {
				last[pt.T/bucket*bucket] = pt.V
			}
		}
		for b, v := range last {
			memBuckets[b] += v
		}
		// Commands: a counter. Its rate between scrapes, averaged in each
		// minute; a drop (a restart) is no rate rather than a negative one.
		sum := map[int64]float64{}
		cnt := map[int64]float64{}
		for i := 1; i < len(s.Commands); i++ {
			a, b := s.Commands[i-1], s.Commands[i]
			if b.T < from || b.T <= a.T || b.V < a.V {
				continue
			}
			rate := (b.V - a.V) / (float64(b.T-a.T) / 1000)
			k := b.T / bucket * bucket
			sum[k] += rate
			cnt[k]++
			if i == len(s.Commands)-1 && b.T >= fresh {
				stats.OpsPerSec += rate
			}
		}
		for k, v := range sum {
			opsBuckets[k] += v / cnt[k]
		}
	}
	if !found {
		return nil
	}
	stats.Memory = sortedPoints(memBuckets)
	stats.Ops = sortedPoints(opsBuckets)
	return stats
}

func sortedPoints(m map[int64]float64) []metricPoint {
	if len(m) == 0 {
		return nil
	}
	out := make([]metricPoint, 0, len(m))
	for t, v := range m {
		out = append(out, metricPoint{T: t, V: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func filterRange(pts []metricPoint, from, to int64) []metricPoint {
	if len(pts) == 0 {
		return nil
	}
	out := make([]metricPoint, 0, len(pts))
	for _, p := range pts {
		if from > 0 && p.T < from {
			continue
		}
		if to > 0 && p.T > to {
			continue
		}
		out = append(out, p)
	}
	return out
}

func (s *server) startMetricsScraper(ctx context.Context) {
	if s.metrics == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(metricsScrapeEvery)
		defer ticker.Stop()
		persistTicker := time.NewTicker(metricsPersistEvery)
		defer persistTicker.Stop()
		s.scrapeOnce(ctx)
		for {
			select {
			case <-ctx.Done():
				_ = s.metrics.persist()
				return
			case <-ticker.C:
				s.scrapeOnce(ctx)
			case <-persistTicker.C:
				if err := s.metrics.persist(); err != nil {
					log.Printf("metrics: persist: %v", err)
				}
			}
		}
	}()
}

func (s *server) scrapeOnce(ctx context.Context) {
	var list kividbv1alpha1.KividbClusterList
	opts := []client.ListOption{}
	if s.watchNamespace != "" {
		opts = append(opts, client.InNamespace(s.watchNamespace))
	}
	if err := s.ctrlClient.List(ctx, &list, opts...); err != nil {
		log.Printf("metrics: list clusters: %v", err)
		return
	}
	now := time.Now().UnixMilli()
	for i := range list.Items {
		c := &list.Items[i]
		pods, err := s.clientset.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: clusterLabelSelector(c.Name),
		})
		if err != nil {
			continue
		}
		for j := range pods.Items {
			p := &pods.Items[j]
			if p.Status.PodIP == "" || !podReady(p) {
				continue
			}
			gauges, err := scrapeAgentGauges(ctx, p.Status.PodIP)
			if err != nil {
				continue
			}
			s.metrics.record(c.Namespace, c.Name, p.Name, now,
				gauges.memory, gauges.clients, gauges.replOffset, gauges.commands,
				gauges.haveMem, gauges.haveClients, gauges.haveRepl, gauges.haveCmd)
		}
	}
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

type agentGauges struct {
	memory, clients, replOffset, commands   float64
	haveMem, haveClients, haveRepl, haveCmd bool
}

func scrapeAgentGauges(ctx context.Context, podIP string) (agentGauges, error) {
	var out agentGauges
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:8081/metrics", podIP), nil)
	if err != nil {
		return out, err
	}
	client := &http.Client{Timeout: agentHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := splitPromSample(line)
		if !ok {
			continue
		}
		switch name {
		case "kividb_used_memory_bytes":
			out.memory, out.haveMem = val, true
		case "kividb_connected_clients":
			out.clients, out.haveClients = val, true
		case "kividb_master_repl_offset":
			out.replOffset, out.haveRepl = val, true
		case "kividb_commands_processed_total":
			out.commands, out.haveCmd = val, true
		}
	}
	return out, nil
}

func splitPromSample(line string) (name string, val float64, ok bool) {
	// gauge lines are "name value" or "name{labels} value" — we only emit unlabeled gauges.
	sp := strings.LastIndexByte(line, ' ')
	if sp <= 0 {
		return "", 0, false
	}
	namePart := line[:sp]
	if i := strings.IndexByte(namePart, '{'); i >= 0 {
		namePart = namePart[:i]
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(line[sp+1:]), 64)
	if err != nil {
		return "", 0, false
	}
	return namePart, v, true
}

func (s *server) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		writeJSONError(w, http.StatusServiceUnavailable, fmt.Errorf("metrics store not initialized"))
		return
	}
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
	if from == 0 {
		from = time.Now().Add(-metricsTTL).UnixMilli()
	}
	if to == 0 {
		to = time.Now().UnixMilli()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"from": from,
		"to":   to,
		"pods": s.metrics.query(namespace, name, from, to),
	})
}
