package output

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCollectAndUploadDeduplicatesBlobUploadsBySHA(t *testing.T) {
	t.Helper()

	var (
		mu          sync.Mutex
		uploaded    = make(map[string][]byte)
		putRequests []string
		headChecks  []string
	)

	manager, err := NewManager(Config{
		BucketURL:       "https://storage.test/bucket",
		PublicBucketURL: "https://public.storage.test/bucket",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Region:          "auto",
		Service:         "s3",
		Timeout:         5 * time.Second,
		MaxFiles:        10,
		MaxBytes:        1 << 20,
	})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	manager.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch request.Method {
		case http.MethodHead:
			headChecks = append(headChecks, request.URL.Path)
			if _, ok := uploaded[request.URL.Path]; ok {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader("")),
					Header:     make(http.Header),
					Request:    request,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		case http.MethodPut:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read uploaded body: %v", err)
			}
			uploaded[request.URL.Path] = body
			putRequests = append(putRequests, request.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		default:
			t.Fatalf("unexpected request method %s", request.Method)
			return nil, nil
		}
	})

	rootDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootDir, "metrics.json"), []byte(`{"accuracy":0.99}`), 0o644); err != nil {
		t.Fatalf("write metrics.json: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(rootDir, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "nested", "copy.json"), []byte(`{"accuracy":0.99}`), 0o644); err != nil {
		t.Fatalf("write nested/copy.json: %v", err)
	}

	result, err := manager.CollectAndUpload(context.Background(), "job-123", rootDir)
	if err != nil {
		t.Fatalf("CollectAndUpload returned error: %v", err)
	}

	if len(result.Artifacts) != 2 {
		t.Fatalf("expected 2 output artifacts, got %d", len(result.Artifacts))
	}
	if result.ManifestURI == "" {
		t.Fatal("expected manifest URI to be populated")
	}

	blobPuts := 0
	manifestPuts := 0
	for _, path := range putRequests {
		switch {
		case strings.Contains(path, "/computehive/job-output-blobs/sha256/"):
			blobPuts++
		case strings.HasSuffix(path, "/computehive/job-results/job-123/manifest.json"):
			manifestPuts++
		}
	}

	if blobPuts != 1 {
		t.Fatalf("expected exactly 1 blob upload for duplicate output content, got %d (%v)", blobPuts, putRequests)
	}
	if manifestPuts != 1 {
		t.Fatalf("expected exactly 1 manifest upload, got %d (%v)", manifestPuts, putRequests)
	}
	if len(headChecks) != 1 {
		t.Fatalf("expected exactly 1 blob existence check, got %d (%v)", len(headChecks), headChecks)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
