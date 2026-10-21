package monitor

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestParseDockerSizeMB(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want float64
		ok   bool
	}{
		{"12.5MiB", 12.5, true},
		{"1GiB", 1024, true},
		{"1.5GiB", 1536, true},
		{"512KiB", 0.5, true},
		{"0B", 0, true},
		{"  7.0MiB ", 7, true},
		{"1MB", 1e6 / (1 << 20), true},
		{"", 0, false},
		{"abc", 0, false},
		{"12parsecs", 0, false},
	} {
		got, ok := ParseDockerSizeMB(tc.in)
		if ok != tc.ok || (ok && math.Abs(got-tc.want) > 1e-6) {
			t.Errorf("ParseDockerSizeMB(%q) = %v,%v want %v,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func dockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
}

func TestSystemSamplerReportsSaneValues(t *testing.T) {
	s := NewSystemSampler(t.TempDir(), "docker", "t")
	ctx := context.Background()
	_, _ = s.Sample(ctx) // the first CPU/network reading has no baseline
	time.Sleep(300 * time.Millisecond)
	m, err := s.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inPct := func(name string, v float64) {
		if v < 0 || v > 100 {
			t.Errorf("%s = %v, want 0-100", name, v)
		}
	}
	inPct("MemPercent", m.MemPercent)
	inPct("CPUPercent", m.CPUPercent)
	inPct("WorkDiskUsedPercent", m.WorkDiskUsedPercent)
	inPct("DockerDiskUsedPercent", m.DockerDiskUsedPercent)
	inPct("SwapPercent", m.SwapPercent)
	if m.MemTotalMB < 256 || m.MemUsedMB <= 0 || m.MemUsedMB > m.MemTotalMB {
		t.Errorf("memory: total=%d used=%d", m.MemTotalMB, m.MemUsedMB)
	}
	if m.MemPercent == 0 || m.WorkDiskUsedPercent == 0 {
		t.Errorf("suspicious zero readings: %+v", m)
	}
	if m.Load1 < 0 || m.CPUTempC < 0 || m.NetEgressBytesPerSec < 0 {
		t.Errorf("negative reading: %+v", m)
	}
	t.Logf("ram %.1f%% (%d/%d MB) cpu %.1f%% load %.2f disk %.1f%% swap %.1f%% temp %.0fC docker=%v gpu=%d egress %.0f B/s",
		m.MemPercent, m.MemUsedMB, m.MemTotalMB, m.CPUPercent, m.Load1, m.WorkDiskUsedPercent, m.SwapPercent, m.CPUTempC, m.DockerHealthy, m.GPUCount, m.NetEgressBytesPerSec)
}

// A real container allocates memory (tmpfs pages are not reclaimable, so MemAvailable really drops).
// The real sampler must see it, the monitor must flip to overloaded, and once the container is gone
// and the host stays low for ReleaseAfter, the monitor must resume.
func TestRealContainerMemoryPressureTripsAndClearsOverload(t *testing.T) {
	dockerAvailable(t)
	s := NewSystemSampler(t.TempDir(), "docker", "pressure-test")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// Baseline: the highest of a few samples, so a noisy machine does not fool the thresholds.
	var base float64
	var total int64
	for i := 0; i < 4; i++ {
		m, err := s.Sample(ctx)
		if err != nil {
			t.Fatal(err)
		}
		base, total = max(base, m.MemPercent), m.MemTotalMB
		time.Sleep(250 * time.Millisecond)
	}
	allocMB := int(float64(total) * 0.09)
	limit, release := base+4, base+2
	if limit > 97 || allocMB < 200 {
		t.Skipf("not enough free memory headroom to test safely (baseline %.1f%%, would allocate %d MB)", base, allocMB)
	}

	cfg := DefaultConfig()
	cfg.RAMLimit, cfg.ReleasePercent = limit, release
	cfg.CPULimit, cfg.DiskLimit = 100, 100
	cfg.CPUWindow, cfg.ReleaseAfter = 1, 2*time.Second
	mon := New(s, cfg, nil, quiet())
	go mon.Run(ctx, 300*time.Millisecond)

	waitState := func(want bool, within time.Duration, what string) {
		t.Helper()
		deadline := time.Now().Add(within)
		for mon.Overloaded() != want {
			if time.Now().After(deadline) {
				st := mon.Status()
				t.Fatalf("%s: overloaded never became %v within %s (ram %.1f%%, limit %.1f%%, release %.1f%%, reasons %v)",
					what, want, within, st.Metrics.MemPercent, limit, release, st.Reasons)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitState(false, 20*time.Second, "baseline")

	name := fmt.Sprintf("ch-pressure-%d", time.Now().UnixNano())
	script := fmt.Sprintf("dd if=/dev/zero of=/m/blob bs=1M count=%d 2>/dev/null; sleep 300", allocMB)
	if out, err := exec.Command("docker", "run", "-d", "--name", name, "--tmpfs", fmt.Sprintf("/m:rw,size=%dm", allocMB+64),
		"alpine:3.21", "sh", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("start allocator: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", name).Run() })

	waitState(true, 60*time.Second, "after allocating")
	st := mon.Status()
	if len(st.Reasons) == 0 || !strings.HasPrefix(st.Reasons[0], "ram>=") {
		t.Fatalf("reasons %v", st.Reasons)
	}
	t.Logf("allocating %d MB pushed RAM from ~%.1f%% to %.1f%% (limit %.1f%%): worker stopped taking jobs", allocMB, base, st.Metrics.MemPercent, limit)

	_ = exec.Command("docker", "rm", "-f", "-v", name).Run()
	start := time.Now()
	waitState(false, 60*time.Second, "after freeing memory")
	t.Logf("memory freed: worker resumed after %s (ReleaseAfter was %s)", time.Since(start).Round(100*time.Millisecond), cfg.ReleaseAfter)
}
