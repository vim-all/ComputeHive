// Package history keeps a local, human-readable record of every job this worker ran, so a
// contributor can see exactly what executed on their machine.
package history

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Entry describes one job attempt. Environment values are never recorded (only names), and the
// command is truncated, so the history cannot itself leak a submitter's secrets.
type Entry struct {
	JobID          string    `json:"job_id"`
	Attempt        int       `json:"attempt"`
	Image          string    `json:"image"`
	Command        string    `json:"command"`
	EnvNames       []string  `json:"env_names,omitempty"`
	ArtifactSHA256 string    `json:"artifact_sha256,omitempty"`
	CPUs           float64   `json:"cpus"`
	MemoryMB       int64     `json:"memory_mb"`
	NetworkHosts   []string  `json:"network_hosts,omitempty"`
	Isolation      string    `json:"isolation"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	Status         string    `json:"status"`
	ExitCode       int       `json:"exit_code"`
	FailureReason  string    `json:"failure_reason,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// Summarize makes a command line safe and short enough to store.
func Summarize(command []string) string {
	s := strings.Join(command, " ")
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// SortedNames returns map keys sorted, for recording environment variable names without values.
func SortedNames(m map[string]string) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// Store appends entries to a JSON-lines file and trims it to the most recent max entries.
type Store struct {
	mu    sync.Mutex
	path  string
	max   int
	count int
	init  bool
}

// Open returns a Store for path keeping at most max entries (default 500).
func Open(path string, max int) *Store {
	if max <= 0 {
		max = 500
	}
	return &Store{path: path, max: max}
}

func (s *Store) Append(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(append(raw, '\n'))
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	if cerr != nil {
		return cerr
	}
	if !s.init {
		all, _ := s.readAll()
		s.count, s.init = len(all)-1, true
	}
	s.count++
	if s.count > 2*s.max { // trim occasionally, not on every write
		return s.trim()
	}
	return nil
}

func (s *Store) readAll() ([]Entry, error) {
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) == nil { // skip a torn line left by a crash
			out = append(out, e)
		}
	}
	return out, sc.Err()
}

func (s *Store) trim() error {
	all, err := s.readAll()
	if err != nil {
		return err
	}
	if len(all) > s.max {
		all = all[len(all)-s.max:]
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, e := range all {
		if err := enc.Encode(e); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.count = len(all)
	return os.Rename(tmp, s.path)
}

// Recent returns up to n most recent entries, newest first.
func (s *Store) Recent(n int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.readAll()
	if err != nil {
		return nil, err
	}
	if n <= 0 || n > len(all) {
		n = len(all)
	}
	out := make([]Entry, 0, n)
	for i := len(all) - 1; i >= len(all)-n; i-- {
		out = append(out, all[i])
	}
	return out, nil
}
