package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	"github.com/minio/minio-go/v7"
)

// appendWhileReading drains the archive, appending to path on every read, the
// way kividb keeps appending to appendonly.aof during a backup.
func appendWhileReading(t *testing.T, pr *io.PipeReader, path string) []byte {
	t.Helper()
	var out bytes.Buffer
	buf := make([]byte, 512)
	for {
		n, err := pr.Read(buf)
		out.Write(buf[:n])
		f, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if openErr == nil {
			_, _ = f.Write(bytes.Repeat([]byte("x"), 4096))
			f.Close()
		}
		if err != nil {
			if err != io.EOF {
				t.Fatalf("reading archive: %v", err)
			}
			return out.Bytes()
		}
	}
}

func TestWriteTarGz_FileGrowsDuringBackup(t *testing.T) {
	dir := t.TempDir()
	aof := filepath.Join(dir, "appendonly.aof")
	original := bytes.Repeat([]byte("SET k v\r\n"), 100_000)
	if err := os.WriteFile(aof, original, 0o644); err != nil {
		t.Fatal(err)
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- writeTarGz(pw, dir, dataFiles) }()

	archive := appendWhileReading(t, pr, aof)
	if err := <-errCh; err != nil {
		t.Fatalf("writeTarGz: %v", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "appendonly.aof" || !bytes.Equal(got, original) {
		t.Fatalf("archived %s: %d bytes, want the %d bytes present when the backup started", hdr.Name, len(got), len(original))
	}
}

func TestWriteTarGz_NoFiles(t *testing.T) {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- writeTarGz(pw, t.TempDir(), dataFiles) }()
	_, _ = io.Copy(io.Discard, pr)
	if err := <-errCh; err == nil {
		t.Fatal("expected an error when no persistence files exist")
	}
}

// fakeS3 is a path-style S3 bucket named "bucket" that serves
// ListObjectsV2 (recursive) and DeleteObject.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]time.Time // key -> LastModified
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/bucket")
	switch {
	case r.Method == http.MethodGet && (path == "" || path == "/"):
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, prefix, len(keys))
		for _, k := range keys {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>"e"</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>`, k, f.objects[k].UTC().Format("2006-01-02T15:04:05.000Z"))
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
	case r.Method == http.MethodDelete:
		delete(f.objects, strings.TrimPrefix(path, "/"))
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// Cluster "prod" with no path prefix writes under "prod/"; cluster "web"
// with path prefix "prod" writes under "prod/web/". prod's retention must
// count the archives of all its pods (the master moves) and nothing else.
func TestPruneOldBackups_OnlyThisClustersArchives(t *testing.T) {
	now := time.Now().UTC()
	key := func(dir, pod string, age time.Duration) string {
		return dir + pod + "-" + now.Add(-age).Format("20060102T150405Z") + ".tar.gz"
	}
	fs := &fakeS3{objects: map[string]time.Time{}}
	add := func(k string, age time.Duration) string {
		fs.objects[k] = now.Add(-age)
		return k
	}
	add(key("prod/", "prod-1", time.Hour), time.Hour)
	add(key("prod/", "prod-0", 2*time.Hour), 2*time.Hour)
	oldest := add(key("prod/", "prod-2", 3*time.Hour), 3*time.Hour)
	for d := 1; d <= 3; d++ {
		add(key("prod/web/", "web-0", time.Duration(d)*24*time.Hour), time.Duration(d)*24*time.Hour)
	}
	add("prod/20260101T000000Z/shard-0-prod-0.tar.gz", 72*time.Hour)
	add("prod/20260101T000000Z/manifest.json", 72*time.Hour)
	add("prod/README.txt", 96*time.Hour)

	srv := httptest.NewServer(fs)
	defer srv.Close()
	s := &server{cfg: agentConfig{
		ClusterName: "prod", PodName: "prod-0",
		S3Endpoint: srv.URL, S3Bucket: "bucket", S3Region: "us-east-1", S3ForcePathStyle: true,
		S3AccessKeyID: "k", S3SecretKey: "s", S3Retention: 2,
	}}
	client, err := s.s3Client()
	if err != nil {
		t.Fatal(err)
	}
	before := len(fs.objects)
	pruned, err := s.pruneOldBackups(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for k := range fs.objects {
		left = append(left, k)
	}
	sort.Strings(left)
	if pruned != 1 {
		t.Fatalf("pruned %d, want only %s; left %v", pruned, oldest, left)
	}
	if _, ok := fs.objects[oldest]; ok {
		t.Fatalf("%s was not pruned; left %v", oldest, left)
	}
	if len(left) != before-1 {
		t.Fatalf("%d objects left, want %d; left %v", len(left), before-1, left)
	}
}

// s3Server is cluster "cache" in namespace ns with path prefix "backups",
// on fs.
func s3Server(t *testing.T, fs *fakeS3, ns string, retention int) (*server, *minio.Client) {
	t.Helper()
	srv := httptest.NewServer(fs)
	t.Cleanup(srv.Close)
	s := &server{cfg: agentConfig{
		ClusterName: "cache", Namespace: ns, PodName: "cache-0",
		S3Endpoint: srv.URL, S3Bucket: "bucket", S3Region: "us-east-1", S3ForcePathStyle: true,
		S3AccessKeyID: "k", S3SecretKey: "s", S3PathPrefix: "backups", S3Retention: retention,
	}}
	client, err := s.s3Client()
	if err != nil {
		t.Fatal(err)
	}
	return s, client
}

// Issue #33: cluster "cache" in team-a and in team-b, same bucket and
// path prefix. Their keys must differ, also within the same second, and
// one's retention must not delete the other's backups. Backups written
// before the namespace was in the key are left alone: either cluster
// may have written them.
func TestBackupKeys_SameClusterNameInTwoNamespaces(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "team-a")
	if ns := loadConfig().Namespace; ns != "team-a" {
		t.Fatalf("POD_NAMESPACE read as %q", ns)
	}

	fs := &fakeS3{objects: map[string]time.Time{}}
	a, client := s3Server(t, fs, "team-a", 1)
	b, _ := s3Server(t, fs, "team-b", 1)
	if ka, kb := a.objectKey(), b.objectKey(); ka == kb || !strings.Contains(ka, "team-a") || !strings.Contains(kb, "team-b") {
		t.Fatalf("object keys %q and %q", ka, kb)
	}

	now := time.Now().UTC()
	add := func(prefix string, age time.Duration) string {
		k := prefix + "cache-0-" + now.Add(-age).Format("20060102T150405Z") + ".tar.gz"
		fs.objects[k] = now.Add(-age)
		return k
	}
	add(a.backupPrefix(), time.Hour)
	oldest := add(a.backupPrefix(), 2*time.Hour)
	theirs := []string{add(b.backupPrefix(), 3*time.Hour), add(b.backupPrefix(), 4*time.Hour)}
	legacy := add("backups/cache/", 48*time.Hour) // the old layout

	pruned, err := a.pruneOldBackups(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for k := range fs.objects {
		left = append(left, k)
	}
	sort.Strings(left)
	if _, ok := fs.objects[oldest]; pruned != 1 || ok {
		t.Fatalf("pruned %d, want only %s; left %v", pruned, oldest, left)
	}
	for _, k := range append(theirs, legacy) {
		if _, ok := fs.objects[k]; !ok {
			t.Fatalf("%s was pruned; left %v", k, left)
		}
	}
}

// The new layout must not reach the old one, whatever path prefix anyone
// used. A folder backups/team-a/cache/ for "cache" in team-a with prefix
// "backups" would be the old folder of a "cache" with prefix
// "backups/team-a": the same keys, and each pruning the other's.
func TestBackupKeys_NeverAnOldLayoutKey(t *testing.T) {
	fs := &fakeS3{objects: map[string]time.Time{}}
	a, client := s3Server(t, fs, "team-a", 1)
	old, _ := s3Server(t, fs, "", 1)
	old.cfg.S3PathPrefix = "backups/team-a"
	same, _ := s3Server(t, fs, "", 1) // the old layout of a's own prefix
	if ka, ko := a.objectKey(), old.objectKey(); ka == ko {
		t.Errorf("both write %s", ka)
	}

	now := time.Now().UTC()
	add := func(k string, age time.Duration) string {
		fs.objects[k] = now.Add(-age)
		return k
	}
	archive := func(s *server, age time.Duration) string {
		return add(s.backupPrefix()+"cache-0-"+now.Add(-age).Format("20060102T150405Z")+".tar.gz", age)
	}
	run := func(s *server, age time.Duration) []string {
		dir := s.backupPrefix() + now.Add(-age).Format(agentapi.RunTimeFormat) + "/"
		return []string{add(dir+"shard-0-cache-0-0.tar.gz", age), add(dir+agentapi.ManifestName, age)}
	}
	mine := append([]string{archive(a, time.Hour)}, run(a, time.Hour)...)
	doomed := []string{archive(a, 2*time.Hour)}
	doomed = append(doomed, run(a, 2*time.Hour)...)
	var kept []string
	for _, s := range []*server{old, same} {
		kept = append(kept, archive(s, 3*time.Hour), archive(s, 4*time.Hour))
		kept = append(kept, run(s, 3*time.Hour)...)
		kept = append(kept, run(s, 4*time.Hour)...)
	}

	if _, err := a.pruneOldBackups(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if _, err := a.pruneRuns(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	var left []string
	for k := range fs.objects {
		left = append(left, k)
	}
	sort.Strings(left)
	for _, k := range doomed {
		if _, ok := fs.objects[k]; ok {
			t.Errorf("%s was not pruned", k)
		}
	}
	for _, k := range kept {
		if _, ok := fs.objects[k]; !ok {
			t.Errorf("old-layout %s was pruned", k)
		}
	}
	if t.Failed() {
		t.Logf("left %v", left)
	}

	// Nor does this agent without POD_NAMESPACE (a pod template older
	// than the agent) prune the new layout.
	for _, s := range []*server{old, same} {
		if _, err := s.pruneOldBackups(context.Background(), client); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pruneRuns(context.Background(), client); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range mine {
		if _, ok := fs.objects[k]; !ok {
			t.Errorf("an old-layout agent pruned %s", k)
		}
	}
}

// prune061 is v0.6.1's pruneOldBackups: every object under
// [<pathPrefix>/]<cluster>/, at any depth, past the newest retention.
func prune061(fs *fakeS3, pathPrefix, cluster string, retention int) {
	prefix := cluster + "/"
	if p := strings.Trim(pathPrefix, "/"); p != "" {
		prefix = p + "/" + prefix
	}
	var keys []string
	for k := range fs.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return fs.objects[keys[i]].After(fs.objects[keys[j]]) })
	if len(keys) > retention {
		for _, k := range keys[retention:] {
			delete(fs.objects, k)
		}
	}
}

// A 0.6.x agent (a pod not rolled yet, a pinned spec.agentImage, another
// operator still on 0.6.x) prunes everything under
// [<pathPrefix>/]<cluster>/. The new keys must not be there, or a
// same-named cluster in another namespace on such an agent deletes them.
func TestBackupKeys_OutOfReachOfOldAgents(t *testing.T) {
	fs := &fakeS3{objects: map[string]time.Time{}}
	a, _ := s3Server(t, fs, "team-a", 1)
	now := time.Now().UTC()
	var mine []string
	for i := 1; i <= 2; i++ {
		age := time.Duration(i) * time.Hour
		k := a.backupPrefix() + "cache-0-" + now.Add(-age).Format("20060102T150405Z") + ".tar.gz"
		fs.objects[k] = now.Add(-age)
		r := a.backupPrefix() + now.Add(-age).Format(agentapi.RunTimeFormat) + "/" + agentapi.ManifestName
		fs.objects[r] = now.Add(-age)
		mine = append(mine, k, r)
	}
	// team-b's "cache" on a 0.6.x agent, same bucket and path prefix,
	// with old backups of its own.
	for i := 3; i <= 5; i++ {
		fs.objects["backups/cache/cache-0-"+now.Add(-time.Duration(i)*time.Hour).Format("20060102T150405Z")+".tar.gz"] = now.Add(-time.Duration(i) * time.Hour)
	}
	prune061(fs, "backups", "cache", 1)
	for _, k := range mine {
		if _, ok := fs.objects[k]; !ok {
			t.Errorf("a 0.6.x agent pruned %s", k)
		}
	}
}

// The engine keeps its files in the directory it changed into at start, a
// KividbConfig `dir` under the volume (#112): the backup asks it (CONFIG
// GET dir) and packs the files from there, not from the volume root, which
// may still hold an older snapshot.
func TestBackupDir_IsWhereTheEngineKeepsItsFiles(t *testing.T) {
	vol := t.TempDir()
	dir := filepath.Join(vol, "sub")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	for p, body := range map[string]string{filepath.Join(vol, "dump.kdb"): "stale", filepath.Join(dir, "dump.kdb"): "current"} {
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rr := newRespRecorder(t, func(cmd []string) string {
		if strings.EqualFold(strings.Join(cmd, " "), "CONFIG GET dir") {
			return "*2\r\n" + bulk("dir") + bulk(dir)
		}
		return "-ERR unknown\r\n"
	})
	s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), DataDir: vol}}
	got, err := s.backupDir()
	if err != nil || got != dir {
		t.Fatalf("backup dir %q, %v; want %q", got, err, dir)
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- writeTarGz(pw, got, dataFiles) }()
	gz, err := gzip.NewReader(pr)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(tr)
	if err != nil || hdr.Name != "dump.kdb" || string(body) != "current" {
		t.Fatalf("archive has %s %q (%v), want dump.kdb %q", hdr.Name, body, err, "current")
	}
	_, _ = io.Copy(io.Discard, pr)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

// A connection that drops before the engine answers says nothing about
// where its files are: the backup fails, rather than pack what is in the
// volume root, which for a `dir` under it is an older snapshot (#112).
func TestBackupDir_NoAnswerFailsTheBackup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	s := &server{cfg: agentConfig{KividbAddr: ln.Addr().String(), DataDir: t.TempDir()}}
	if got, err := s.backupDir(); err == nil {
		t.Fatalf("backup dir %q, want an error: the engine never answered", got)
	}
}

// An engine whose files are off this pod's volume (a `dir` the operator now
// refuses, on a cluster that already ran with it) has nothing on the volume
// to back up: the backup fails and says where they are. An engine that
// answers but will not say (an ACL that denies CONFIG to the default user,
// or no such parameter) is taken to be in the volume root, as before #112.
func TestBackupDir_OffTheVolumeOrRefused(t *testing.T) {
	vol := t.TempDir()
	outside := t.TempDir()
	for _, tc := range []struct {
		name  string
		reply string
		want  string // the dir; "" when the backup fails
		err   string
	}{
		{"off the volume", "*2\r\n" + bulk("dir") + bulk(outside), "", "keeps its files in " + outside},
		{"next to the volume", "*2\r\n" + bulk("dir") + bulk(vol+"2"), "", "keeps its files in " + vol + "2"},
		{"the volume root", "*2\r\n" + bulk("dir") + bulk(vol), vol, ""},
		{"CONFIG refused", "-NOPERM User default has no permissions to run the 'config|get' command\r\n", vol, ""},
		{"no such parameter", "*0\r\n", vol, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := newRespRecorder(t, func([]string) string { return tc.reply })
			s := &server{cfg: agentConfig{KividbAddr: rr.ln.Addr().String(), DataDir: vol}}
			got, err := s.backupDir()
			switch {
			case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err) || !strings.Contains(err.Error(), vol)):
				t.Fatalf("backup dir %q, %v; want an error saying %q and naming the volume %s", got, err, tc.err, vol)
			case tc.err == "" && (err != nil || got != tc.want):
				t.Fatalf("backup dir %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
