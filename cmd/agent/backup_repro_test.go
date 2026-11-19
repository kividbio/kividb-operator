package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// startUpload streams a 64 KiB snapshot to an S3 endpoint served by h.
func startUpload(ctx context.Context, t *testing.T, h http.Handler) <-chan error {
	t.Helper()
	dir := t.TempDir()
	buf := make([]byte, 64<<10)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "dump.kdb"), buf, 0o600,
	); err != nil {
		t.Fatal(err)
	}

	s, client := s3Server(t, h, "", 0)
	done := make(chan error, 1)
	go func() {
		_, err := s.streamUpload(ctx, client, "c/x.tar.gz", dir)
		done <- err
	}()
	return done
}

// uploadErr waits for streamUpload, which returns only after its archive writer exits.
func uploadErr(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("streamUpload still blocked after 10s")
		return nil
	}
}

func TestStreamUploadReturnsWhenS3RefusesUpfront(t *testing.T) {
	done := startUpload(context.Background(), t, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(
				`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`,
			))
		},
	))

	var resp minio.ErrorResponse
	if err := uploadErr(t, done); !errors.As(err, &resp) || resp.Code != "AccessDenied" {
		t.Fatalf("streamUpload: %v, want S3's AccessDenied", err)
	}
}

func TestStreamUploadReturnsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reached := make(chan struct{}, 1)
	done := startUpload(ctx, t, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case reached <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		},
	))

	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the upload never reached S3")
	}
	cancel()
	if err := uploadErr(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("streamUpload: %v, want context.Canceled", err)
	}
}
