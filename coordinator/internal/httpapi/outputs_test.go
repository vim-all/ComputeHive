package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeAuthz: workers authenticate with "Bearer w-<id>"; the API token is "api".
type fakeAuthz struct{}

func (fakeAuthz) HTTPWriteAllowed(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer api"
}
func (fakeAuthz) HTTPReadAllowed(r *http.Request) bool {
	a := r.Header.Get("Authorization")
	return a == "Bearer api" || strings.HasPrefix(a, "Bearer w-")
}
func (fakeAuthz) AllowAnon(string) bool { return true }
func (fakeAuthz) WorkerFromRequest(r *http.Request) (string, bool) {
	id := r.Header.Get("X-Worker-Id")
	return id, id != "" && r.Header.Get("Authorization") == "Bearer w-"+id
}

type outRig struct {
	srv     *httptest.Server
	dir     string
	holder  atomic.Value // worker holding the lease on job "job1"
	attempt atomic.Int64
	checks  atomic.Int64
}

func newOutRig(t *testing.T, max int64) *outRig {
	t.Helper()
	r := &outRig{dir: t.TempDir()}
	r.holder.Store("worker-a")
	r.attempt.Store(1)
	h, err := New(r.dir, max, nil, fakeAuthz{}, func(_ context.Context, workerID, jobID string) (int, bool) {
		r.checks.Add(1)
		if jobID != "job1" || workerID != r.holder.Load().(string) {
			return 0, false
		}
		return int(r.attempt.Load()), true
	})
	if err != nil {
		t.Fatal(err)
	}
	r.srv = httptest.NewServer(h)
	t.Cleanup(r.srv.Close)
	return r
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func (r *outRig) put(t *testing.T, worker, token, job, rel string, body []byte, sha string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/outputs/"+job+"/"+rel, bytes.NewReader(body))
	if worker != "" {
		req.Header.Set("X-Worker-Id", worker)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if sha != "" {
		req.Header.Set("X-Content-SHA256", sha)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (r *outRig) get(t *testing.T, token, rel string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, r.srv.URL+"/outputs/"+rel, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestLeaseHolderUploadsAndSubmitterDownloadsWithVerifiedHash(t *testing.T) {
	r := newOutRig(t, 1<<20)
	body := []byte("a,b\n1,2\n")
	code, resp := r.put(t, "worker-a", "w-worker-a", "job1", "results/out.csv", body, sum(body))
	if code != http.StatusCreated {
		t.Fatalf("upload: %d %s", code, resp)
	}
	var out map[string]string
	_ = json.Unmarshal([]byte(resp), &out)
	if out["uri"] != "computehive-output://job1/1/results/out.csv" {
		t.Fatalf("uri %q", out["uri"])
	}
	if code, got := r.get(t, "api", "job1/1/results/out.csv"); code != 200 || !bytes.Equal(got, body) {
		t.Fatalf("download: %d %q", code, got)
	}
	if code, _ := r.get(t, "", "job1/1/results/out.csv"); code != http.StatusUnauthorized {
		t.Fatalf("anonymous download: %d", code)
	}
	if code, _ := r.get(t, "api", "job1/1/results/missing.csv"); code != http.StatusNotFound {
		t.Fatalf("missing file: %d", code)
	}
}

func TestOnlyTheCurrentLeaseHolderMayWriteOutputs(t *testing.T) {
	r := newOutRig(t, 1<<20)
	body := []byte("real result")
	if code, _ := r.put(t, "worker-b", "w-worker-b", "job1", "out.txt", body, sum(body)); code != http.StatusForbidden {
		t.Fatalf("a worker without the lease uploaded: %d", code)
	}
	if code, _ := r.put(t, "worker-a", "wrong-token", "job1", "out.txt", body, sum(body)); code != http.StatusUnauthorized {
		t.Fatalf("bad credential: %d", code)
	}
	if code, _ := r.put(t, "", "", "job1", "out.txt", body, sum(body)); code != http.StatusUnauthorized {
		t.Fatalf("anonymous upload: %d", code)
	}
	if code, _ := r.put(t, "worker-a", "api", "job1", "out.txt", body, sum(body)); code != http.StatusUnauthorized {
		t.Fatalf("the API token is not a worker credential: %d", code)
	}
	if code, _ := r.put(t, "worker-a", "w-worker-a", "other-job", "out.txt", body, sum(body)); code != http.StatusForbidden {
		t.Fatalf("a worker uploading to a job it does not hold: %d", code)
	}

	// The lease moves to worker-b (attempt 2): the zombie worker-a can no longer overwrite anything,
	// and the new attempt's files live in a separate directory.
	if code, _ := r.put(t, "worker-a", "w-worker-a", "job1", "out.txt", body, sum(body)); code != http.StatusCreated {
		t.Fatal("holder could not upload")
	}
	r.holder.Store("worker-b")
	r.attempt.Store(2)
	if code, _ := r.put(t, "worker-a", "w-worker-a", "job1", "out.txt", []byte("zombie overwrite"), sum([]byte("zombie overwrite"))); code != http.StatusForbidden {
		t.Fatalf("zombie upload accepted: %d", code)
	}
	good := []byte("attempt two result")
	if code, _ := r.put(t, "worker-b", "w-worker-b", "job1", "out.txt", good, sum(good)); code != http.StatusCreated {
		t.Fatal("new holder could not upload")
	}
	if _, got := r.get(t, "api", "job1/1/out.txt"); !bytes.Equal(got, body) {
		t.Fatalf("attempt 1 output was disturbed: %q", got)
	}
	if _, got := r.get(t, "api", "job1/2/out.txt"); !bytes.Equal(got, good) {
		t.Fatalf("attempt 2 output: %q", got)
	}
}

func TestOutputUploadValidation(t *testing.T) {
	r := newOutRig(t, 64)
	body := []byte("x")
	for name, rel := range map[string]string{
		"parent traversal":   "../../etc/passwd",
		"embedded traversal": "a/../../b",
		"backslash":          `a\b`,
		"nul":                "a%00b",
		"too deep dots":      "..",
	} {
		code, _ := r.put(t, "worker-a", "w-worker-a", "job1", rel, body, sum(body))
		if code == http.StatusCreated {
			t.Errorf("%s: path %q accepted", name, rel)
		}
	}
	if code, _ := r.put(t, "worker-a", "w-worker-a", "job1", "ok.txt", body, ""); code != http.StatusBadRequest {
		t.Fatalf("missing hash header: %d", code)
	}
	if code, _ := r.put(t, "worker-a", "w-worker-a", "job1", "ok.txt", body, strings.Repeat("0", 64)); code != http.StatusBadRequest {
		t.Fatalf("wrong hash: %d", code)
	}
	big := bytes.Repeat([]byte("y"), 200)
	if code, _ := r.put(t, "worker-a", "w-worker-a", "job1", "big.bin", big, sum(big)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized file: %d", code)
	}
	if code, _ := r.put(t, "worker-a", "w-worker-a", "bad job!", "ok.txt", body, sum(body)); code != http.StatusBadRequest {
		t.Fatalf("bad job id: %d", code)
	}
	// Paths such as "a//b" or "/etc/passwd" are normalised by the HTTP router into harmless
	// relative paths *inside* the attempt directory; what must hold is that nothing is ever
	// written outside it, and that rejected uploads leave no temp files behind.
	r.put(t, "worker-a", "w-worker-a", "job1", "a//b", body, sum(body))
	r.put(t, "worker-a", "w-worker-a", "job1", "a/./c", body, sum(body))
	attemptDir := filepath.Join(r.dir, "outputs", "job1", "1")
	_ = filepath.WalkDir(r.dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if rel, err := filepath.Rel(attemptDir, p); err != nil || strings.HasPrefix(rel, "..") {
			t.Errorf("file written outside the attempt directory: %s", p)
		}
		if strings.Contains(filepath.Base(p), ".upload-") {
			t.Errorf("temp file left behind: %s", p)
		}
		return nil
	})
	if _, err := os.Stat(filepath.Join(filepath.Dir(r.dir), "etc")); err == nil {
		t.Fatal("a traversal created a directory outside the store")
	}
}

func TestOutputDownloadCannotFollowSymlinksOrTraverse(t *testing.T) {
	r := newOutRig(t, 1<<20)
	secret := filepath.Join(t.TempDir(), "secret.txt")
	_ = os.WriteFile(secret, []byte("host secret"), 0o600)
	dir := filepath.Join(r.dir, "outputs", "job1", "1")
	_ = os.MkdirAll(dir, 0o750)
	_ = os.Symlink(secret, filepath.Join(dir, "link.txt"))
	if code, got := r.get(t, "api", "job1/1/link.txt"); code == 200 || strings.Contains(string(got), "host secret") {
		t.Fatalf("a symlink in the output tree exposed a host file: %d %q", code, got)
	}
	for _, p := range []string{"job1/1/../../../secret.txt", "job1/x/out.txt", "../job1/1/out.txt"} {
		if code, _ := r.get(t, "api", p); code == 200 {
			t.Errorf("GET %s = 200", p)
		}
	}
}

func TestOutputsRefuseToRunWithoutAuthz(t *testing.T) {
	h, _ := New(t.TempDir(), 1<<20, nil, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/outputs/job1/x.txt", strings.NewReader("x"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("an unauthenticated coordinator must not accept output uploads: %d", resp.StatusCode)
	}
}
