// Package httpapi serves the coordinator's HTTP surface: a content-addressed
// artifact store (so a cluster works without S3/R2) and health endpoints.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ReadyFunc reports whether dependencies (Redis) are reachable.
type ReadyFunc func(ctx context.Context) error

// Authz decides who may use the artifact store. A nil Authz leaves it open (tests, dev mode).
type Authz interface {
	HTTPWriteAllowed(r *http.Request) bool
	HTTPReadAllowed(r *http.Request) bool
	AllowAnon(remote string) bool
	WorkerFromRequest(r *http.Request) (workerID string, ok bool)
}

// OutputGuard says whether workerID may store output files for jobID right now, and for which attempt.
// Only the worker that currently holds the job's lease may: a worker whose lease was revoked or
// superseded (a zombie) cannot overwrite the real result.
type OutputGuard func(ctx context.Context, workerID, jobID string) (attempt int, ok bool)

// New returns the HTTP handler. Artifacts are stored under dir, named by their SHA-256.
func New(dir string, maxBytes int64, ready ReadyFunc, authz Authz, outputs ...OutputGuard) (http.Handler, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create artifact dir %s: %w", dir, err)
	}
	s := &server{dir: dir, maxBytes: maxBytes, ready: ready, authz: authz}
	if len(outputs) > 0 {
		s.outputs = outputs[0]
	}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /artifacts/{sha}", s.guard(true, s.putArtifact))
	mux.HandleFunc("GET /artifacts/{sha}", s.guard(false, s.getArtifact))
	mux.HandleFunc("HEAD /artifacts/{sha}", s.guard(false, s.getArtifact))
	mux.HandleFunc("PUT /outputs/{job}/{path...}", s.putOutput)
	mux.HandleFunc("GET /outputs/{job}/{attempt}/{path...}", s.guard(false, s.getOutput))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux, nil
}

type server struct {
	dir      string
	maxBytes int64
	ready    ReadyFunc
	authz    Authz
	outputs  OutputGuard
}

// guard rate-limits by source address, then requires the right credential for the operation.
func (s *server) guard(write bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authz != nil {
			if !s.authz.AllowAnon(r.RemoteAddr) {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			ok := s.authz.HTTPReadAllowed(r)
			if write {
				ok = s.authz.HTTPWriteAllowed(r)
			}
			if !ok {
				w.Header().Set("WWW-Authenticate", `Bearer realm="computehive"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *server) path(sha string) string { return filepath.Join(s.dir, sha) }

func (s *server) putArtifact(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha")
	if !sha256Pattern.MatchString(sha) {
		http.Error(w, "artifact name must be a lowercase hex SHA-256", http.StatusBadRequest)
		return
	}
	if r.ContentLength > s.maxBytes {
		http.Error(w, "artifact exceeds size limit", http.StatusRequestEntityTooLarge)
		return
	}

	if _, err := os.Stat(s.path(sha)); err == nil {
		w.WriteHeader(http.StatusOK) // content-addressed: already stored
		return
	}

	tmp, err := os.CreateTemp(s.dir, ".upload-*")
	if err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmp.Name()) // no-op after successful rename

	hash := sha256.New()
	body := http.MaxBytesReader(w, r.Body, s.maxBytes)
	if _, err := io.Copy(io.MultiWriter(tmp, hash), body); err != nil {
		_ = tmp.Close()
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "artifact exceeds size limit", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "upload interrupted", http.StatusBadRequest)
		return
	}
	if err := tmp.Close(); err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != sha {
		http.Error(w, "content hash does not match "+sha, http.StatusBadRequest)
		return
	}
	if err := os.Rename(tmp.Name(), s.path(sha)); err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *server) getArtifact(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha")
	if !sha256Pattern.MatchString(sha) {
		http.Error(w, "artifact name must be a lowercase hex SHA-256", http.StatusBadRequest)
		return
	}
	f, err := os.Open(s.path(sha))
	if errors.Is(err, os.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, sha, time.Time{}, f)
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.ready(ctx); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = io.WriteString(w, "ready\n")
}

// WithMetrics serves metricsHandler at GET /metrics in front of next. The endpoint exposes worker ids,
// queue sizes and per-worker utilisation, so it requires the API token (the same credential as artifact
// uploads) and is rate-limited by source address before the token is examined.
func WithMetrics(next, metricsHandler http.Handler, authz Authz) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		if authz != nil {
			if !authz.AllowAnon(r.RemoteAddr) {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			if !authz.HTTPWriteAllowed(r) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="computehive"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		metricsHandler.ServeHTTP(w, r)
	})
}
