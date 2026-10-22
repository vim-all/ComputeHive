package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
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

// Self-hosted mode: with no bucket, files go to the coordinator with the worker's credential and a hash.
func TestOutputsAreStoredOnTheCoordinatorWhenThereIsNoBucket(t *testing.T) {
	type put struct {
		path, sha, auth, worker string
		body                    []byte
	}
	var got []put
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, put{r.URL.Path, r.Header.Get("X-Content-SHA256"), r.Header.Get("Authorization"), r.Header.Get("X-Worker-Id"), b})
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"uri":"computehive-output://job1/2` + strings.TrimPrefix(r.URL.Path, "/outputs/job1") + `"}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "sub dir"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "result.csv"), []byte("a,b\n1,2\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "sub dir", "log.txt"), []byte("done"), 0o644)

	m, err := NewManager(Config{Timeout: 5 * time.Second, MaxFiles: 10, MaxBytes: 1 << 20, CoordinatorURL: srv.URL + "/",
		CoordinatorAuth: func(set func(k, v string)) { set("Authorization", "Bearer chwc_x"); set("X-Worker-Id", "w1") }})
	if err != nil {
		t.Fatal(err)
	}
	res, err := m.CollectAndUpload(context.Background(), "job1", dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(res.Artifacts) != 2 {
		t.Fatalf("uploads %d artifacts %d", len(got), len(res.Artifacts))
	}
	for _, p := range got {
		if p.auth != "Bearer chwc_x" || p.worker != "w1" {
			t.Errorf("credential headers missing on %s", p.path)
		}
		sum := sha256.Sum256(p.body)
		if p.sha != hex.EncodeToString(sum[:]) {
			t.Errorf("%s: X-Content-SHA256 %s does not match the body", p.path, p.sha)
		}
	}
	byPath := map[string]string{}
	for _, a := range res.Artifacts {
		byPath[a.RelativePath] = a.URI
	}
	if byPath["result.csv"] != "computehive-output://job1/2/result.csv" || !strings.HasPrefix(byPath["sub dir/log.txt"], "computehive-output://job1/2/sub") {
		t.Fatalf("artifact URIs: %v", byPath)
	}
}

func TestACoordinatorRefusalFailsTheUpload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no lease", http.StatusForbidden) }))
	defer srv.Close()
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "r.txt"), []byte("x"), 0o644)
	m, _ := NewManager(Config{Timeout: 2 * time.Second, MaxFiles: 10, MaxBytes: 1 << 20, CoordinatorURL: srv.URL})
	if _, err := m.CollectAndUpload(context.Background(), "job1", dir); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("got %v", err)
	}
	empty, _ := NewManager(Config{Timeout: time.Second, MaxFiles: 1, MaxBytes: 1})
	if _, err := empty.CollectAndUpload(context.Background(), "job1", dir); err == nil || !strings.Contains(err.Error(), "no output storage") {
		t.Fatalf("no sink at all must say what to configure, got %v", err)
	}
}
