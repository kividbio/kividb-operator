package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
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
