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

// New returns the HTTP handler. Artifacts are stored under dir, named by their SHA-256.
func New(dir string, maxBytes int64, ready ReadyFunc) (http.Handler, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create artifact dir %s: %w", dir, err)
	}
	s := &server{dir: dir, maxBytes: maxBytes, ready: ready}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /artifacts/{sha}", s.putArtifact)
	mux.HandleFunc("GET /artifacts/{sha}", s.getArtifact)
	mux.HandleFunc("HEAD /artifacts/{sha}", s.getArtifact)
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
