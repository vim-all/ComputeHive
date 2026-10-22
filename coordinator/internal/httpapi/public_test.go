package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type limitedAuthz struct{ fakeAuthz }

func (limitedAuthz) AllowAnon(string) bool { return false }

func TestPublicFileIsServedWithoutCredentialsAndOnlyAtItsPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, []byte("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "next:"+r.Method+" "+r.URL.Path)
	})
	srv := httptest.NewServer(WithPublicFile(next, "/ca.crt", path, "application/x-pem-file", fakeAuthz{}))
	defer srv.Close()

	do := func(method, p string) (int, string, string) {
		req, _ := http.NewRequest(method, srv.URL+p, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header.Get("Content-Type")
	}
	if code, body, ct := do(http.MethodGet, "/ca.crt"); code != 200 || !strings.Contains(body, "BEGIN CERTIFICATE") || ct != "application/x-pem-file" {
		t.Fatalf("GET /ca.crt: %d %q %q", code, body, ct)
	}
	if code, body, _ := do(http.MethodGet, "/other"); code != 200 || body != "next:GET /other" {
		t.Fatalf("other paths must reach the normal handler: %d %q", code, body)
	}
	if code, body, _ := do(http.MethodPost, "/ca.crt"); body != "next:POST /ca.crt" {
		t.Fatalf("only GET is served from the file: %d %q", code, body)
	}

	// The file is read fresh each time: a rotated CA is served without a restart.
	if err := os.WriteFile(path, []byte("rotated"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, body, _ := do(http.MethodGet, "/ca.crt"); body != "rotated" {
		t.Fatalf("stale content: %q", body)
	}
}

func TestPublicFileIsRateLimitedAndHandlesAMissingFile(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	path := filepath.Join(t.TempDir(), "ca.crt")
	_ = os.WriteFile(path, []byte("x"), 0o644)

	rec := httptest.NewRecorder()
	WithPublicFile(next, "/ca.crt", path, "text/plain", limitedAuthz{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ca.crt", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a throttled source must get 429, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	WithPublicFile(next, "/ca.crt", filepath.Join(t.TempDir(), "missing"), "text/plain", fakeAuthz{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ca.crt", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("a missing file must be 404, got %d", rec.Code)
	}
}
