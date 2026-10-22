package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T, max int64, ready ReadyFunc) *httptest.Server {
	t.Helper()
	h, err := New(t.TempDir(), max, ready, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func put(t *testing.T, url string, body []byte) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestArtifactRoundTrip(t *testing.T) {
	srv := newTestServer(t, 1<<20, nil)
	data := []byte("hello artifact")
	sum := sha256.Sum256(data)
	url := srv.URL + "/artifacts/" + hex.EncodeToString(sum[:])

	if got := put(t, url, data); got != http.StatusCreated {
		t.Fatalf("first PUT = %d, want 201", got)
	}
	if got := put(t, url, data); got != http.StatusOK {
		t.Fatalf("repeat PUT = %d, want 200 (idempotent)", got)
	}

	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("GET = %d %q, want 200 %q", resp.StatusCode, body, data)
	}
}

func TestArtifactRejections(t *testing.T) {
	srv := newTestServer(t, 16, nil)
	good := []byte("0123456789abcdef!") // 17 bytes > 16 limit
	sum := sha256.Sum256(good)
	goodName := hex.EncodeToString(sum[:])
	wrongName := strings.Repeat("a", 64)

	if got := put(t, srv.URL+"/artifacts/"+goodName, good); got != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize = %d, want 413", got)
	}
	if got := put(t, srv.URL+"/artifacts/"+wrongName, []byte("short")); got != http.StatusBadRequest {
		t.Errorf("hash mismatch = %d, want 400", got)
	}
	if got := put(t, srv.URL+"/artifacts/not-a-hash", []byte("x")); got != http.StatusBadRequest {
		t.Errorf("bad name = %d, want 400", got)
	}
	if got := put(t, srv.URL+"/artifacts/..%2f..%2fetc%2fpasswd", []byte("x")); got != http.StatusBadRequest && got != http.StatusNotFound {
		t.Errorf("traversal = %d, want 400/404", got)
	}

	resp, err := http.Get(srv.URL + "/artifacts/" + wrongName)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing GET = %d, want 404", resp.StatusCode)
	}
}

func TestReadyz(t *testing.T) {
	var fail bool
	srv := newTestServer(t, 1, func(context.Context) error {
		if fail {
			return errors.New("redis down")
		}
		return nil
	})

	status := func(path string) int {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := status("/readyz"); got != http.StatusOK {
		t.Errorf("readyz healthy = %d", got)
	}
	fail = true
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz with redis down = %d, want 503", got)
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Errorf("healthz must stay 200 when deps are down, got %d", got)
	}
}
