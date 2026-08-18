package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// runRestoreFromS3 downloads a snapshot tar.gz from S3 and extracts
// dump.kdb / appendonly.aof into --data-dir. Used by the operator's
// bootstrap Job to seed a new cluster PVC.
func runRestoreFromS3(args []string) error {
	fs := flag.NewFlagSet("restore-from-s3", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "S3 endpoint URL")
	bucket := fs.String("bucket", "", "S3 bucket")
	region := fs.String("region", "", "S3 region")
	objectKey := fs.String("object-key", "", "object key of the snapshot tar.gz")
	dataDir := fs.String("data-dir", "/data", "directory to extract persistence files into")
	forcePathStyle := fs.String("force-path-style", "false", "use path-style addressing")
	insecureTLS := fs.String("insecure-skip-tls-verify", "false", "skip TLS verify")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" || *bucket == "" || *objectKey == "" {
		return fmt.Errorf("--endpoint, --bucket, and --object-key are required")
	}

	accessKey := os.Getenv("S3_ACCESS_KEY_ID")
	secretKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY must be set")
	}

	forcePS, _ := strconv.ParseBool(*forcePathStyle)
	insec, _ := strconv.ParseBool(*insecureTLS)

	ep := *endpoint
	secure := true
	if strings.HasPrefix(ep, "http://") {
		secure = false
		ep = strings.TrimPrefix(ep, "http://")
	} else if strings.HasPrefix(ep, "https://") {
		ep = strings.TrimPrefix(ep, "https://")
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: *region,
	}
	if forcePS {
		opts.BucketLookup = minio.BucketLookupPath
	}
	if insec {
		opts.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	}

	client, err := minio.New(ep, opts)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	log.Printf("downloading s3://%s/%s", *bucket, *objectKey)
	obj, err := client.GetObject(ctx, *bucket, *objectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		return err
	}
	if err := extractSnapshotTarGz(obj, *dataDir); err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	log.Printf("restored snapshot into %s", *dataDir)
	return nil
}

func extractSnapshotTarGz(r io.Reader, dataDir string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	allowed := map[string]bool{"dump.kdb": true, "appendonly.aof": true}
	var wrote bool
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		base := filepath.Base(hdr.Name)
		if !allowed[base] || hdr.Typeflag != tar.TypeReg {
			continue
		}
		dest := filepath.Join(dataDir, base)
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return err
		}
		f.Close()
		wrote = true
		log.Printf("wrote %s", dest)
	}
	if !wrote {
		return fmt.Errorf("archive contained no dump.kdb or appendonly.aof")
	}
	return nil
}
