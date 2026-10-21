package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Config holds the safety thresholds. All percentages are 0-100.
type Config struct {
	RAMLimit  float64 // overloaded when RAM used >= this (default 90)
	CPULimit  float64 // overloaded when (smoothed) CPU >= this (default 90)
	DiskLimit float64 // overloaded when either disk's used% >= this (default 90)

	// Recovery: every metric must stay below ReleasePercent for ReleaseAfter.
	ReleasePercent float64 // default 80
	ReleaseAfter   time.Duration

	CPUWindow         int // CPU is averaged over this many samples so a one-sample spike does not trip the limit
	MaxSampleFailures int // consecutive sampling errors before the worker fails safe
}

// DefaultConfig: enter at 90%, leave after 10s below 80%.
func DefaultConfig() Config {
	return Config{RAMLimit: 90, CPULimit: 90, DiskLimit: 90, ReleasePercent: 80, ReleaseAfter: 10 * time.Second, CPUWindow: 3, MaxSampleFailures: 3}
}

// Validate reports unusable settings with the flag that controls each.
func (c Config) Validate() error {
	for _, l := range []struct {
		name string
		v    float64
	}{{"ram-limit-percent", c.RAMLimit}, {"cpu-limit-percent", c.CPULimit}, {"disk-limit-percent", c.DiskLimit}} {
		if l.v <= 0 || l.v > 100 {
			return fmt.Errorf("%s %.1f must be in (0, 100]", l.name, l.v)
		}
	}
	if c.ReleasePercent <= 0 || c.ReleasePercent >= min(c.RAMLimit, c.CPULimit, c.DiskLimit) {
		return fmt.Errorf("release-percent %.1f must be positive and below every limit (ram %.1f, cpu %.1f, disk %.1f); otherwise there is no hysteresis band and the worker would flap",
			c.ReleasePercent, c.RAMLimit, c.CPULimit, c.DiskLimit)
	}
	if c.ReleaseAfter < 0 {
		return fmt.Errorf("release-after %s must be >= 0", c.ReleaseAfter)
	}
	return nil
}

// Status is the monitor's current verdict.
type Status struct {
	Overloaded bool
	Reasons    []string  // e.g. "ram>=90%", "docker_unhealthy"
	Since      time.Time // when the current overload began (zero if not overloaded)
	Metrics    Metrics
}

// Monitor turns a stream of samples into an overload decision with hysteresis:
// it becomes overloaded the moment any metric reaches its limit, and recovers only
// after every metric has stayed one band below its limit (below 80% with the defaults)
// for ReleaseAfter. Readings in between (for example 85% on the way down) leave the
// state unchanged.
type Monitor struct {
	sampler Sampler
	cfg     Config
	now     func() time.Time
	log     *slog.Logger

	mu         sync.RWMutex
	latest     Metrics
	cpuHist    []float64
	over       bool // threshold-driven, sticky
	reasons    []string
	since      time.Time
	belowSince time.Time
	failures   int
	haveSample bool
}

// New creates a Monitor. now may be nil (time.Now); tests inject a fake clock.
func New(s Sampler, cfg Config, now func() time.Time, log *slog.Logger) *Monitor {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.CPUWindow < 1 {
		cfg.CPUWindow = 1
	}
	if cfg.MaxSampleFailures < 1 {
		cfg.MaxSampleFailures = 3
	}
	return &Monitor{sampler: s, cfg: cfg, now: now, log: log}
}

// Run samples every interval until ctx is done.
func (m *Monitor) Run(ctx context.Context, interval time.Duration) {
	m.sampleOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.sampleOnce(ctx)
		}
	}
}

func (m *Monitor) sampleOnce(ctx context.Context) {
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	mx, err := m.sampler.Sample(sctx)
	if err != nil {
		if ctx.Err() == nil {
			m.sampleFailed(err)
		}
		return
	}
	m.Observe(mx)
}

func (m *Monitor) sampleFailed(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures++
	m.log.Warn("resource sampling failed", "error", err, "consecutive_failures", m.failures)
}

// Observe applies one sample to the state machine.
func (m *Monitor) Observe(mx Metrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mx.At.IsZero() {
		mx.At = m.now()
	}
	m.failures = 0
	m.haveSample = true
	m.latest = mx

	m.cpuHist = append(m.cpuHist, mx.CPUPercent)
	if len(m.cpuHist) > m.cfg.CPUWindow {
		m.cpuHist = m.cpuHist[len(m.cpuHist)-m.cfg.CPUWindow:]
	}
	var cpu float64
	for _, v := range m.cpuHist {
		cpu += v
	}
	cpu /= float64(len(m.cpuHist))

	ram, disk := mx.MemPercent, mx.DiskPercent()
	var reasons []string
	if ram >= m.cfg.RAMLimit {
		reasons = append(reasons, fmt.Sprintf("ram>=%.0f%%", m.cfg.RAMLimit))
	}
	if cpu >= m.cfg.CPULimit {
		reasons = append(reasons, fmt.Sprintf("cpu>=%.0f%%", m.cfg.CPULimit))
	}
	if disk >= m.cfg.DiskLimit {
		reasons = append(reasons, fmt.Sprintf("disk>=%.0f%%", m.cfg.DiskLimit))
	}

	now := m.now()
	switch {
	case len(reasons) > 0:
		if !m.over {
			m.since = now
			m.log.Warn("worker overloaded: no new jobs will be accepted", "event", "overload_entered",
				"reasons", strings.Join(reasons, ","), "ram_percent", ram, "cpu_percent", cpu, "disk_percent", disk)
		}
		m.over, m.reasons, m.belowSince = true, reasons, time.Time{}
	case m.over:
		// Each metric must be one hysteresis band below its own limit. The band is the gap
		// between the lowest limit and ReleasePercent (10 points with the 90/80 defaults), so a
		// metric far from its limit (or with its limit raised to disable it) cannot block recovery.
		band := min(m.cfg.RAMLimit, m.cfg.CPULimit, m.cfg.DiskLimit) - m.cfg.ReleasePercent
		if ram < m.cfg.RAMLimit-band && cpu < m.cfg.CPULimit-band && disk < m.cfg.DiskLimit-band {
			if m.belowSince.IsZero() {
				m.belowSince = now
			}
			if now.Sub(m.belowSince) >= m.cfg.ReleaseAfter {
				m.over, m.reasons, m.since, m.belowSince = false, nil, time.Time{}, time.Time{}
				m.log.Info("worker recovered: accepting jobs again", "event", "overload_cleared",
					"ram_percent", ram, "cpu_percent", cpu, "disk_percent", disk)
			}
		} else {
			m.belowSince = time.Time{} // back in the 80-90% band: recovery restarts
		}
	}
}

// Status returns the current verdict. A worker that cannot sample its own resources
// fails safe (overloaded) after MaxSampleFailures consecutive errors, and one that has
// never produced a sample is not yet cleared to take work.
func (m *Monitor) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := Status{Overloaded: m.over, Reasons: append([]string(nil), m.reasons...), Since: m.since, Metrics: m.latest}
	switch {
	case !m.haveSample:
		st.Overloaded, st.Reasons = true, append(st.Reasons, "no_samples_yet")
	case m.failures >= m.cfg.MaxSampleFailures:
		st.Overloaded, st.Reasons = true, append(st.Reasons, "metrics_unavailable")
	case !m.latest.DockerHealthy:
		st.Overloaded, st.Reasons = true, append(st.Reasons, "docker_unhealthy")
	}
	return st
}

// Overloaded reports whether new jobs must not be accepted.
func (m *Monitor) Overloaded() bool { return m.Status().Overloaded }

// OverloadedFor is how long the host has continuously been over a threshold (0 if not).
func (m *Monitor) OverloadedFor() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.over || m.since.IsZero() {
		return 0
	}
	return m.now().Sub(m.since)
}
