package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
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
