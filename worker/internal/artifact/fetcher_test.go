package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

func TestFetchDownloadsAndVerifiesArtifact(t *testing.T) {
	payload := []byte("artifact-bytes")
	sum := sha256.Sum256(payload)

	fetcher := newWithClient(&http.Client{
		Timeout: 2 * time.Second,
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(payload)),
				Header:     make(http.Header),
			}, nil
		}),
	}, 1024, SigV4Config{})
	job := domain.Job{
		ID:             "task-01",
		ArtifactURL:    "https://artifact.test/task-01.tar.gz",
		ArtifactSHA256: hex.EncodeToString(sum[:]),
		ImageRef:       "computehive/task-01:latest",
	}

	bundle, err := fetcher.Fetch(context.Background(), job)
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	defer func() {
		if err := Cleanup(bundle); err != nil {
			t.Fatalf("Cleanup returned error: %v", err)
		}
	}()

	if bundle.SizeBytes != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), bundle.SizeBytes)
	}
	content, err := os.ReadFile(bundle.ArchivePath)
	if err != nil {
		t.Fatalf("failed reading artifact: %v", err)
	}
	if string(content) != string(payload) {
		t.Fatalf("downloaded content mismatch: %q", string(content))
	}
}

func TestFetchRejectsHashMismatch(t *testing.T) {
	fetcher := newWithClient(&http.Client{
		Timeout: 2 * time.Second,
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader([]byte("artifact-bytes"))),
				Header:     make(http.Header),
			}, nil
		}),
	}, 1024, SigV4Config{})
	job := domain.Job{
		ID:             "task-01",
		ArtifactURL:    "https://artifact.test/task-01.tar.gz",
		ArtifactSHA256: "deadbeef",
		ImageRef:       "computehive/task-01:latest",
	}

	if _, err := fetcher.Fetch(context.Background(), job); err == nil {
		t.Fatal("expected hash mismatch error")
	}
}

type roundTripFunc func(request *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
