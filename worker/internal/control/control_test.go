package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/history"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
)

type fakeBackend struct {
	mu     sync.Mutex
	pol    policy.Policy
	paused bool
	now    bool
	killed bool
	hist   []history.Entry
}

func (f *fakeBackend) Status() Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := "running"
	if f.paused {
		st = "paused"
	}
	return Status{WorkerID: "w1", State: st, Policy: f.pol}
}
func (f *fakeBackend) Pause(now bool) error {
	f.mu.Lock()
	f.paused, f.now = true, now
	f.mu.Unlock()
	return nil
}
func (f *fakeBackend) Resume() error { f.mu.Lock(); f.paused = false; f.mu.Unlock(); return nil }
func (f *fakeBackend) Kill() error   { f.mu.Lock(); f.killed = true; f.mu.Unlock(); return nil }
func (f *fakeBackend) Policy() policy.Policy {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pol
}
func (f *fakeBackend) SetPolicy(p policy.Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	f.pol = p
	f.mu.Unlock()
	return nil
}
func (f *fakeBackend) History(n int) ([]history.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n < len(f.hist) {
		return f.hist[:n], nil
	}
	return f.hist, nil
}

func startServer(t *testing.T) (*fakeBackend, *http.Client, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "run")
	sock := filepath.Join(dir, "control.sock")
	b := &fakeBackend{pol: policy.Default(), hist: []history.Entry{{JobID: "j1"}, {JobID: "j2"}, {JobID: "j3"}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = Serve(ctx, sock, b, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	return b, client, sock
}

func call(t *testing.T, c *http.Client, method, path string, body any) (int, string) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, "http://control"+path, rdr)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestSocketIsPrivateToTheWorkersUser(t *testing.T) {
	_, _, sock := startServer(t)
	st, err := os.Stat(sock)
	if err != nil || st.Mode()&os.ModeSocket == 0 {
		t.Fatalf("not a socket: %v %v", st, err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket mode %o, want 0600: other local users must not be able to pause or kill the worker", perm)
	}
	if dst, _ := os.Stat(filepath.Dir(sock)); dst.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode %o, want 0700", dst.Mode().Perm())
	}
}

func TestPauseResumeAndKill(t *testing.T) {
	b, c, _ := startServer(t)
	if code, body := call(t, c, "POST", "/v1/pause", nil); code != 200 || !strings.Contains(body, "will finish") {
		t.Fatalf("pause: %d %s", code, body)
	}
	if !b.paused || b.now {
		t.Fatalf("soft pause: paused=%v now=%v", b.paused, b.now)
	}
	if code, body := call(t, c, "POST", "/v1/pause?now=true", nil); code != 200 || !strings.Contains(body, "handed back") {
		t.Fatalf("hard pause: %d %s", code, body)
	}
	if !b.now {
		t.Fatal("pause?now=true must evict the running job")
	}
	if code, _ := call(t, c, "GET", "/v1/status", nil); code != 200 {
		t.Fatal("status")
	}
	if code, _ := call(t, c, "POST", "/v1/resume", nil); code != 200 || b.paused {
		t.Fatalf("resume: paused=%v", b.paused)
	}
	if code, body := call(t, c, "POST", "/v1/kill", nil); code != 200 || !b.killed || !strings.Contains(body, "kill switch") {
		t.Fatalf("kill: %d %s killed=%v", code, body, b.killed)
	}
	if code, _ := call(t, c, "GET", "/v1/pause", nil); code != http.StatusMethodNotAllowed {
		t.Fatalf("state-changing endpoints must not answer GET, got %d", code)
	}
}

func TestPolicyRoundTripAndValidation(t *testing.T) {
	b, c, _ := startServer(t)
	p := policy.Default()
	p.MaxCPUs, p.MaxMemoryMB, p.Schedule = 2, 4096, "22:00-07:00"
	p.AllowNetwork, p.AllowedHosts = true, []string{"pypi.org"}
	if code, body := call(t, c, "PUT", "/v1/policy", p); code != 200 {
		t.Fatalf("put policy: %d %s", code, body)
	}
	if got := b.Policy(); got.MaxCPUs != 2 || got.Schedule != "22:00-07:00" || !got.AllowNetwork {
		t.Fatalf("policy not applied: %+v", got)
	}
	code, body := call(t, c, "GET", "/v1/policy", nil)
	var back policy.Policy
	if code != 200 || json.Unmarshal([]byte(body), &back) != nil || back.MaxMemoryMB != 4096 {
		t.Fatalf("get policy: %d %s", code, body)
	}

	// Invalid values are refused with 400 and change nothing.
	p.AllowedHosts = []string{"169.254.169.254"}
	if code, _ := call(t, c, "PUT", "/v1/policy", p); code != http.StatusBadRequest {
		t.Fatalf("an IP in allowed_hosts must be rejected, got %d", code)
	}
	if b.Policy().AllowedHosts[0] != "pypi.org" {
		t.Fatal("rejected policy was applied")
	}
	// A misspelt field must not silently leave a protection unset.
	if code, body := call(t, c, "PUT", "/v1/policy", map[string]any{"alow_network": true}); code != http.StatusBadRequest || !strings.Contains(body, "unknown field") {
		t.Fatalf("unknown field: %d %s", code, body)
	}
}

func TestHistoryLimit(t *testing.T) {
	_, c, _ := startServer(t)
	code, body := call(t, c, "GET", "/v1/history?limit=2", nil)
	var got []history.Entry
	if code != 200 || json.Unmarshal([]byte(body), &got) != nil || len(got) != 2 {
		t.Fatalf("history: %d %s", code, body)
	}
}

func TestServeReplacesAStaleSocketButRefusesToClobberAFile(t *testing.T) {
	dir := t.TempDir()
	b := &fakeBackend{pol: policy.Default()}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	notSocket := filepath.Join(dir, "important.txt")
	_ = os.WriteFile(notSocket, []byte("precious"), 0o600)
	if err := Serve(context.Background(), notSocket, b, log); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("Serve must refuse to delete a regular file, got %v", err)
	}
	if data, _ := os.ReadFile(notSocket); string(data) != "precious" {
		t.Fatal("the file was destroyed")
	}

	stale := filepath.Join(dir, "stale.sock")
	l, _ := net.Listen("unix", stale)
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	l.Close() // leaves a dead socket file, as a crashed worker would
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- Serve(ctx, stale, b, log) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("stale socket was not replaced: %v", err)
	}
}
