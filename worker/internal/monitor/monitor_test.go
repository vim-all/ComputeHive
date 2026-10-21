package monitor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newMon(cfg Config) (*Monitor, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	return New(nil, cfg, clk.now, quiet()), clk
}

func sample(ram, cpu, disk float64) Metrics {
	return Metrics{MemPercent: ram, CPUPercent: cpu, WorkDiskUsedPercent: disk, DockerDiskUsedPercent: disk, DockerHealthy: true}
}

func cfg10s() Config {
	c := DefaultConfig()
	c.CPUWindow = 1
	c.ReleaseAfter = 10 * time.Second
	return c
}

// step feeds a sample, then advances the clock by the interval.
type step struct {
	ram, cpu, disk float64
	after          time.Duration // clock advance after observing
	wantOver       bool
	note           string
}

func runSteps(t *testing.T, cfg Config, steps []step) {
	t.Helper()
	m, clk := newMon(cfg)
	for i, s := range steps {
		m.Observe(sample(s.ram, s.cpu, s.disk))
		if got := m.Overloaded(); got != s.wantOver {
			t.Fatalf("step %d (%s): ram=%.0f cpu=%.0f disk=%.0f -> overloaded=%v, want %v", i, s.note, s.ram, s.cpu, s.disk, got, s.wantOver)
		}
		clk.advance(s.after)
	}
}

func TestEntersAtLimitAndOnlyLeavesAfterSustainedRecovery(t *testing.T) {
	sec := time.Second
	// The recovery timer starts at the first sub-80% sample (step 5) and must run for ReleaseAfter = 10s.
	runSteps(t, cfg10s(), []step{
		{50, 10, 10, sec, false, "healthy"},
		{89.9, 10, 10, sec, false, "just under the limit"},
		{90, 10, 10, sec, true, "exactly at the limit enters"},
		{95, 10, 10, sec, true, "stays overloaded"},
		{85, 10, 10, sec, true, "85% is inside the 80-90 band: still overloaded"},
		{79, 10, 10, 5 * sec, true, "below 80%: recovery timer starts (t=0)"},
		{79, 10, 10, 4 * sec, true, "t=5s"},
		{79, 10, 10, sec, true, "t=9s: one second short"},
		{79, 10, 10, 0, false, "t=10s: 10s continuously below 80% clears it"},
	})
}

func TestBouncingBackIntoTheBandRestartsTheRecoveryTimer(t *testing.T) {
	m, clk := newMon(cfg10s())
	m.Observe(sample(92, 10, 10))
	clk.advance(time.Second)
	m.Observe(sample(70, 10, 10)) // timer starts
	clk.advance(8 * time.Second)
	m.Observe(sample(82, 10, 10)) // 82% is in the 80-90 band: timer resets
	clk.advance(time.Second)
	m.Observe(sample(70, 10, 10)) // timer starts again
	clk.advance(9 * time.Second)
	m.Observe(sample(70, 10, 10)) // only 9s since the restart
	if !m.Overloaded() {
		t.Fatal("recovery must restart after a bounce into the hysteresis band")
	}
	clk.advance(time.Second)
	m.Observe(sample(70, 10, 10))
	if m.Overloaded() {
		t.Fatal("should have cleared 10s after the restart")
	}
}

func TestEveryMetricMustRecover(t *testing.T) {
	m, clk := newMon(cfg10s())
	m.Observe(sample(95, 10, 10))
	for i := 0; i < 30; i++ {
		clk.advance(time.Second)
		m.Observe(sample(50, 10, 85)) // RAM fine, but disk stuck in the band
	}
	if !m.Overloaded() {
		t.Fatal("must not recover while disk is still at 85%")
	}
	for i := 0; i < 11; i++ {
		clk.advance(time.Second)
		m.Observe(sample(50, 10, 40))
	}
	if m.Overloaded() {
		t.Fatal("should recover once every metric is below 80% for 10s")
	}
}

func TestEachMetricTripsIndependentlyWithAReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		mx   Metrics
		want string
	}{
		{"ram", sample(91, 10, 10), "ram>=90%"},
		{"cpu", sample(10, 99, 10), "cpu>=90%"},
		{"disk (work dir)", func() Metrics { m := sample(10, 10, 10); m.WorkDiskUsedPercent = 95; return m }(), "disk>=90%"},
		{"disk (docker root)", func() Metrics { m := sample(10, 10, 10); m.DockerDiskUsedPercent = 95; return m }(), "disk>=90%"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newMon(cfg10s())
			m.Observe(tc.mx)
			st := m.Status()
			if !st.Overloaded || len(st.Reasons) != 1 || st.Reasons[0] != tc.want {
				t.Fatalf("status %+v, want reason %q", st, tc.want)
			}
		})
	}
}

func TestThresholdsAreConfigurable(t *testing.T) {
	cfg := cfg10s()
	cfg.RAMLimit, cfg.ReleasePercent = 70, 50
	m, _ := newMon(cfg)
	m.Observe(sample(75, 10, 10))
	if !m.Overloaded() {
		t.Fatal("a 70% limit must trip at 75%")
	}
	if st := m.Status(); st.Reasons[0] != "ram>=70%" {
		t.Fatalf("reason %v", st.Reasons)
	}
}

func TestCPUSpikesAreSmoothed(t *testing.T) {
	cfg := cfg10s()
	cfg.CPUWindow = 3
	m, _ := newMon(cfg)
	for _, cpu := range []float64{20, 20, 100} { // one 100% sample averages to 46%
		m.Observe(sample(10, cpu, 10))
	}
	if m.Overloaded() {
		t.Fatal("a single CPU spike must not trip the limit")
	}
	for i := 0; i < 3; i++ {
		m.Observe(sample(10, 100, 10)) // sustained
	}
	if !m.Overloaded() {
		t.Fatal("sustained 100% CPU must trip the limit")
	}
}

func TestDockerUnhealthyBlocksWorkWithoutHysteresis(t *testing.T) {
	m, _ := newMon(cfg10s())
	bad := sample(10, 10, 10)
	bad.DockerHealthy = false
	m.Observe(bad)
	if st := m.Status(); !st.Overloaded || !strings.Contains(strings.Join(st.Reasons, ","), "docker_unhealthy") {
		t.Fatalf("status %+v", st)
	}
	m.Observe(sample(10, 10, 10))
	if m.Overloaded() {
		t.Fatal("recovers as soon as Docker answers again")
	}
}

func TestFailsSafeWhenSamplingBreaksOrHasNotStarted(t *testing.T) {
	cfg := cfg10s()
	cfg.MaxSampleFailures = 3
	calls := 0
	var fail bool
	s := SamplerFunc(func(context.Context) (Metrics, error) {
		calls++
		if fail {
			return Metrics{}, errors.New("boom")
		}
		return sample(10, 10, 10), nil
	})
	m := New(s, cfg, nil, quiet())
	if !m.Overloaded() {
		t.Fatal("a worker with no samples yet must not take work")
	}
	m.sampleOnce(context.Background())
	if m.Overloaded() {
		t.Fatal("healthy sample should clear it")
	}
	fail = true
	m.sampleOnce(context.Background())
	m.sampleOnce(context.Background())
	if m.Overloaded() {
		t.Fatal("two failures are tolerated")
	}
	m.sampleOnce(context.Background())
	if st := m.Status(); !st.Overloaded || st.Reasons[len(st.Reasons)-1] != "metrics_unavailable" {
		t.Fatalf("three failures must fail safe: %+v", st)
	}
	fail = false
	m.sampleOnce(context.Background())
	if m.Overloaded() {
		t.Fatal("recovers when sampling works again")
	}
}

func TestOverloadedForTracksDuration(t *testing.T) {
	m, clk := newMon(cfg10s())
	m.Observe(sample(50, 10, 10))
	if m.OverloadedFor() != 0 {
		t.Fatal("not overloaded")
	}
	m.Observe(sample(95, 10, 10))
	clk.advance(7 * time.Second)
	m.Observe(sample(96, 10, 10))
	if got := m.OverloadedFor(); got != 7*time.Second {
		t.Fatalf("OverloadedFor = %s, want 7s", got)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"ram over 100", func(c *Config) { c.RAMLimit = 120 }, "ram-limit-percent"},
		{"cpu zero", func(c *Config) { c.CPULimit = 0 }, "cpu-limit-percent"},
		{"no hysteresis band", func(c *Config) { c.ReleasePercent = 90 }, "release-percent"},
		{"release above a limit", func(c *Config) { c.DiskLimit = 70 }, "release-percent"},
		{"negative release time", func(c *Config) { c.ReleaseAfter = -1 }, "release-after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultConfig()
			tc.mut(&c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error mentioning %q", err, tc.want)
			}
		})
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
}

// A metric whose limit has been raised out of the way must not block recovery just because it sits above the release level.
func TestMetricFarFromItsOwnLimitDoesNotBlockRecovery(t *testing.T) {
	cfg := cfg10s()
	cfg.RAMLimit, cfg.ReleasePercent = 72, 70 // band = 2 points
	cfg.CPULimit, cfg.DiskLimit = 100, 100    // effectively disabled
	m, clk := newMon(cfg)
	m.Observe(sample(75, 10, 10))
	if !m.Overloaded() {
		t.Fatal("RAM 75% >= 72% must trip")
	}
	for i := 0; i < 12; i++ {
		clk.advance(time.Second)
		m.Observe(sample(60, 85, 85)) // CPU and disk at 85% are far below their own 100% limits
	}
	if m.Overloaded() {
		t.Fatal("disk/cpu at 85% (limit 100%) must not hold a recovered worker hostage")
	}
	// ...but a metric inside its own band still blocks.
	m.Observe(sample(95, 10, 10))
	for i := 0; i < 12; i++ {
		clk.advance(time.Second)
		m.Observe(sample(71, 10, 10)) // RAM 71% is inside its band (>= 72-2=70%)
	}
	if !m.Overloaded() {
		t.Fatal("RAM at 71% (band 70-72%) must still block recovery")
	}
}
