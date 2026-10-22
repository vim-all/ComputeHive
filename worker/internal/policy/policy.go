// Package policy is the contributor's local rulebook. The coordinator cannot override it:
// the worker consults it before taking work and before running anything.
package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Policy is persisted as JSON (mode 0600) and edited through the control API or by hand.
type Policy struct {
	// Paused: take no new jobs. (Pausing "now" also evicts the running job; that is the control API's job.)
	Paused bool `json:"paused"`

	// Donation. Zero means "everything the machine has" for CPUs and memory.
	MaxCPUs      float64 `json:"max_cpus"`        // most CPU cores advertised to the cluster and given to one job
	MaxMemoryMB  int64   `json:"max_memory_mb"`   // most memory advertised and given to one job
	MaxJobDiskMB int64   `json:"max_job_disk_mb"` // writable work-directory quota per job (default 256)

	// When to work.
	IdleOnly       bool    `json:"idle_only"`        // take new jobs only while the machine is otherwise idle
	IdleCPUPercent float64 `json:"idle_cpu_percent"` // "idle" means host CPU below this (default 25)
	Schedule       string  `json:"schedule"`         // local time window "HH:MM-HH:MM", may span midnight; "" = always

	// Network. Off by default: jobs get no network at all.
	AllowNetwork bool     `json:"allow_network"`
	AllowedHosts []string `json:"allowed_hosts"` // exact host names or "*.example.com"; nothing else is ever reachable

	// Images the worker may run when they are not delivered as an artifact. Deny wins; empty allow = none.
	ImageAllow []string `json:"image_allow"`
	ImageDeny  []string `json:"image_deny"`

	// RequireGVisor: refuse to run anything unless the worker has the gVisor runtime.
	RequireGVisor bool `json:"require_gvisor"`
}

// Default is the conservative starting point: working, no network, 256 MB of work disk per job.
func Default() Policy {
	return Policy{MaxJobDiskMB: 256, IdleCPUPercent: 25}
}

// Validate reports the first invalid setting.
func (p Policy) Validate() error {
	if p.MaxCPUs < 0 || p.MaxMemoryMB < 0 || p.MaxJobDiskMB < 0 {
		return errors.New("max_cpus, max_memory_mb and max_job_disk_mb must be >= 0")
	}
	if p.IdleCPUPercent < 0 || p.IdleCPUPercent > 100 {
		return errors.New("idle_cpu_percent must be between 0 and 100")
	}
	if _, _, _, err := ParseSchedule(p.Schedule); err != nil {
		return err
	}
	for _, h := range p.AllowedHosts {
		if err := validateHostPattern(h); err != nil {
			return err
		}
	}
	for _, pat := range append(append([]string(nil), p.ImageAllow...), p.ImageDeny...) {
		if _, err := path.Match(pat, ""); err != nil {
			return fmt.Errorf("image pattern %q is not a valid glob: %w", pat, err)
		}
	}
	return nil
}

func validateHostPattern(h string) error {
	h = strings.TrimPrefix(h, "*.")
	if h == "" || len(h) > 253 || strings.ContainsAny(h, "/:@* ") {
		return fmt.Errorf("allowed host %q must be a host name or *.suffix (no IP addresses, ports or paths)", h)
	}
	for _, r := range h {
		if !(r == '.' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return fmt.Errorf("allowed host %q contains an invalid character", h)
		}
	}
	if net := strings.Trim(h, "0123456789."); net == "" {
		return fmt.Errorf("allowed host %q is an IP address; only host names are accepted", h)
	}
	return nil
}

// ParseSchedule parses "HH:MM-HH:MM" into minutes since midnight. ok is false for "" (always on).
func ParseSchedule(s string) (start, end int, ok bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, false, nil
	}
	a, b, found := strings.Cut(s, "-")
	if !found {
		return 0, 0, false, fmt.Errorf("schedule %q must look like 22:00-07:00", s)
	}
	parse := func(v string) (int, error) {
		t, err := time.Parse("15:04", strings.TrimSpace(v))
		if err != nil {
			return 0, fmt.Errorf("schedule %q: %q is not HH:MM", s, v)
		}
		return t.Hour()*60 + t.Minute(), nil
	}
	if start, err = parse(a); err != nil {
		return 0, 0, false, err
	}
	if end, err = parse(b); err != nil {
		return 0, 0, false, err
	}
	if start == end {
		return 0, 0, false, fmt.Errorf("schedule %q is empty (start equals end)", s)
	}
	return start, end, true, nil
}

// InWindow reports whether t (local time) falls inside the schedule window.
func (p Policy) InWindow(t time.Time) bool {
	start, end, ok, err := ParseSchedule(p.Schedule)
	if err != nil || !ok {
		return err == nil
	}
	m := t.Hour()*60 + t.Minute()
	if start < end {
		return m >= start && m < end
	}
	return m >= start || m < end // spans midnight
}

// Admission says whether the worker may take a new job right now, and why not.
type Admission struct {
	Allowed bool
	Reason  string
}

// Admit applies pause, schedule and idle rules. hostCPUPercent is the machine's current CPU load.
func (p Policy) Admit(now time.Time, hostCPUPercent float64) Admission {
	switch {
	case p.Paused:
		return Admission{false, "paused"}
	case !p.InWindow(now):
		return Admission{false, "outside_schedule"}
	case p.IdleOnly && hostCPUPercent >= p.IdleCPUPercent:
		return Admission{false, "host_busy"}
	}
	return Admission{true, ""}
}

// HostAllowed reports whether a job may be given network access to host: the contributor must have
// enabled network jobs and listed the host (exact name or *.suffix).
func (p Policy) HostAllowed(host string) bool {
	if !p.AllowNetwork {
		return false
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, pat := range p.AllowedHosts {
		pat = strings.ToLower(pat)
		if strings.HasPrefix(pat, "*.") {
			if strings.HasSuffix(host, pat[1:]) && len(host) > len(pat)-1 {
				return true
			}
			continue
		}
		if host == pat {
			return true
		}
	}
	return false
}

// Manager keeps the policy in memory and on disk, safe for concurrent use.
type Manager struct {
	mu   sync.RWMutex
	p    Policy
	path string
}

// Open loads the policy file, creating it with defaults if it does not exist.
func Open(file string) (*Manager, error) {
	m := &Manager{path: file, p: Default()}
	raw, err := os.ReadFile(file)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return m, m.save()
	case err != nil:
		return nil, fmt.Errorf("read policy %s: %w", file, err)
	}
	if err := json.Unmarshal(raw, &m.p); err != nil {
		return nil, fmt.Errorf("parse policy %s: %w", file, err)
	}
	if err := m.p.Validate(); err != nil {
		return nil, fmt.Errorf("policy %s: %w", file, err)
	}
	return m, nil
}

// Get returns a copy of the current policy.
func (m *Manager) Get() Policy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return clone(m.p)
}

// Update applies fn to a copy, validates, saves atomically, and only then makes it current.
func (m *Manager) Update(fn func(*Policy)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := clone(m.p)
	fn(&next)
	if err := next.Validate(); err != nil {
		return err
	}
	prev := m.p
	m.p = next
	if err := m.save(); err != nil {
		m.p = prev
		return err
	}
	return nil
}

func clone(p Policy) Policy {
	p.AllowedHosts = append([]string(nil), p.AllowedHosts...)
	p.ImageAllow = append([]string(nil), p.ImageAllow...)
	p.ImageDeny = append([]string(nil), p.ImageDeny...)
	return p
}

func (m *Manager) save() error {
	if err := os.MkdirAll(filepath.Dir(m.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m.p, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(m.path), ".policy-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), m.path)
}
