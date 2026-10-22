// Package control is the contributor's local control surface: pause/resume, the kill switch,
// donation and schedule settings, network permission, and job history. It listens on a Unix
// socket that only the worker's own user can open (mode 0600 inside a 0700 directory), so it is
// never reachable from the network or by other local users. The desktop app and the CLI are both
// clients of this API.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/history"
	"github.com/vim-all/ComputeHive/worker/internal/monitor"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
)

// Status is what `computehive worker status` shows.
type Status struct {
	WorkerID   string           `json:"worker_id"`
	Version    string           `json:"version"`
	Isolation  string           `json:"isolation"`
	State      string           `json:"state"`      // running, paused, waiting, overloaded, killed
	NotTaking  string           `json:"not_taking"` // why new jobs are not being accepted ("" if they are)
	CurrentJob string           `json:"current_job,omitempty"`
	Uptime     string           `json:"uptime"`
	Policy     policy.Policy    `json:"policy"`
	Metrics    *monitor.Metrics `json:"metrics,omitempty"`
}

// Backend is the worker as the control API sees it.
type Backend interface {
	Status() Status
	Pause(now bool) error // stop taking jobs; now=true also evicts the running job (it is requeued, not blamed)
	Resume() error
	Kill() error // evict the running job immediately and shut the worker down
	Policy() policy.Policy
	SetPolicy(policy.Policy) error
	History(n int) ([]history.Entry, error)
}

// Handler returns the control API.
func Handler(b Backend) http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, code int, err error) {
		writeJSON(w, code, map[string]string{"error": err.Error()})
	}
	ok := func(w http.ResponseWriter, msg string) { writeJSON(w, http.StatusOK, map[string]string{"ok": msg}) }

	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, b.Status()) })
	mux.HandleFunc("POST /v1/pause", func(w http.ResponseWriter, r *http.Request) {
		now := r.URL.Query().Get("now") == "true"
		if err := b.Pause(now); err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
		if now {
			ok(w, "paused; the running job (if any) was handed back to the cluster")
		} else {
			ok(w, "paused; the running job (if any) will finish")
		}
	})
	mux.HandleFunc("POST /v1/resume", func(w http.ResponseWriter, _ *http.Request) {
		if err := b.Resume(); err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
		ok(w, "resumed")
	})
	mux.HandleFunc("POST /v1/kill", func(w http.ResponseWriter, _ *http.Request) {
		if err := b.Kill(); err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
		ok(w, "kill switch engaged: the running job is being stopped and the worker is exiting")
	})
	mux.HandleFunc("GET /v1/policy", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, b.Policy()) })
	mux.HandleFunc("PUT /v1/policy", func(w http.ResponseWriter, r *http.Request) {
		var p policy.Policy
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields() // a typo in a field name must not silently leave a protection off
		if err := dec.Decode(&p); err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("invalid policy: %w", err))
			return
		}
		if err := b.SetPolicy(p); err != nil {
			fail(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, b.Policy())
	})
	mux.HandleFunc("GET /v1/history", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if n <= 0 || n > 500 {
			n = 50
		}
		h, err := b.History(n)
		if err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, h)
	})
	return mux
}

// Serve listens on a private Unix socket until ctx is done.
func Serve(ctx context.Context, socketPath string, b Backend, log *slog.Logger) error {
	dir := filepath.Dir(socketPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create control directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure control directory: %w", err)
	}
	if st, err := os.Lstat(socketPath); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("%s exists and is not a socket", socketPath)
		}
		_ = os.Remove(socketPath) // stale socket from a previous run
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("secure control socket: %w", err)
	}
	srv := &http.Server{Handler: Handler(b), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		_ = os.Remove(socketPath)
	}()
	log.Info("control socket listening", "path", socketPath)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
