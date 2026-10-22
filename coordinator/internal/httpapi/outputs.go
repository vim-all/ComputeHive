package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Output files let a self-hosted cluster return job results without any cloud object storage:
// the worker that holds a job's lease uploads the files the job wrote, and the submitter
// downloads them with `computehive result <id> --download DIR`.

const maxOutputFilesPerAttempt = 512

var (
	jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$`)
	attemptRe    = regexp.MustCompile(`^[0-9]{1,4}$`)
)

// cleanOutputPath validates a slash-separated relative path from a job. It must stay inside the
// job's directory: no absolute paths, no "..", no empty or hidden-traversal segments.
func cleanOutputPath(p string) (string, bool) {
	if p == "" || len(p) > 1024 || strings.ContainsAny(p, "\\\x00") || strings.HasPrefix(p, "/") {
		return "", false
	}
	clean := path.Clean(p)
	if clean != p || clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", false
	}
	for _, seg := range strings.Split(clean, "/") {
		if seg == "" || seg == "." || seg == ".." || len(seg) > 255 {
			return "", false
		}
	}
	return clean, true
}

func (s *server) outputRoot() string { return filepath.Join(s.dir, "outputs") }

func (s *server) putOutput(w http.ResponseWriter, r *http.Request) {
	if s.authz == nil || s.outputs == nil {
		http.Error(w, "output storage is not enabled on this coordinator", http.StatusNotImplemented)
		return
	}
	if !s.authz.AllowAnon(r.RemoteAddr) {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	workerID, ok := s.authz.WorkerFromRequest(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="computehive"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	job := r.PathValue("job")
	rel, relOK := cleanOutputPath(r.PathValue("path"))
	if !jobIDPattern.MatchString(job) || !relOK {
		http.Error(w, "invalid job id or file path", http.StatusBadRequest)
		return
	}
	wantSHA := strings.ToLower(r.Header.Get("X-Content-SHA256"))
	if !sha256Pattern.MatchString(wantSHA) {
		http.Error(w, "X-Content-SHA256 (hex) is required", http.StatusBadRequest)
		return
	}
	attempt, ok := s.outputs(r.Context(), workerID, job)
	if !ok {
		http.Error(w, "this worker does not hold the lease on that job", http.StatusForbidden)
		return
	}
	if r.ContentLength > s.maxBytes {
		http.Error(w, "file exceeds size limit", http.StatusRequestEntityTooLarge)
		return
	}

	attemptDir := filepath.Join(s.outputRoot(), job, strconv.Itoa(attempt))
	dest := filepath.Join(attemptDir, filepath.FromSlash(rel))
	if within, err := filepath.Rel(attemptDir, dest); err != nil || strings.HasPrefix(within, "..") {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(dest); errors.Is(err, fs.ErrNotExist) && countFiles(attemptDir) >= maxOutputFilesPerAttempt {
		http.Error(w, "too many output files for one attempt", http.StatusRequestEntityTooLarge)
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".upload-*")
	if err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), http.MaxBytesReader(w, r.Body, s.maxBytes)); err != nil {
		_ = tmp.Close()
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "file exceeds size limit", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "upload interrupted", http.StatusBadRequest)
		return
	}
	if err := tmp.Close(); err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	if hex.EncodeToString(h.Sum(nil)) != wantSHA {
		http.Error(w, "content hash does not match X-Content-SHA256", http.StatusBadRequest)
		return
	}
	// Re-check the lease after the upload: the worker may have lost it while sending a large file.
	if _, still := s.outputs(r.Context(), workerID, job); !still {
		http.Error(w, "this worker lost the lease on that job during the upload", http.StatusForbidden)
		return
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		http.Error(w, "storage unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"uri": "computehive-output://" + job + "/" + strconv.Itoa(attempt) + "/" + rel})
}

func countFiles(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func (s *server) getOutput(w http.ResponseWriter, r *http.Request) {
	job, attempt := r.PathValue("job"), r.PathValue("attempt")
	rel, relOK := cleanOutputPath(r.PathValue("path"))
	if !jobIDPattern.MatchString(job) || !attemptRe.MatchString(attempt) || !relOK {
		http.Error(w, "invalid output path", http.StatusBadRequest)
		return
	}
	attemptDir := filepath.Join(s.outputRoot(), job, attempt)
	full := filepath.Join(attemptDir, filepath.FromSlash(rel))
	info, err := os.Lstat(full) // Lstat: never follow a symlink out of the output tree
	if err != nil || !info.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(full)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filepath.Base(full)+`"`)
	http.ServeContent(w, r, filepath.Base(full), time.Time{}, f)
}
