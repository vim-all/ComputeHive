package monitor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	gnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/sensors"
)

// SystemSampler reads real host and Docker state. Slow probes (Docker, GPU, per-container
// memory) are cached for a few seconds so sampling stays cheap at 1 Hz.
type SystemSampler struct {
	workDir      string
	dockerBinary string
	workerID     string

	mu         sync.Mutex
	lastNet    uint64
	lastNetAt  time.Time
	haveNet    bool
	dockerRoot string
	rootProbed bool

	dockerOK    bool
	dockerAt    time.Time
	gpuCount    int
	gpuUtil     float64
	gpuAt       time.Time
	jobsMemMB   int64
	jobsMemAt   time.Time
	probeTTL    time.Duration
	jobsMemTTL  time.Duration
	commandTime time.Duration
}

// NewSystemSampler watches workDir (where job directories live) and the containers
// labelled computehive.worker.id=workerID.
func NewSystemSampler(workDir, dockerBinary, workerID string) *SystemSampler {
	if workDir == "" {
		workDir = os.TempDir()
	}
	return &SystemSampler{workDir: workDir, dockerBinary: dockerBinary, workerID: workerID,
		probeTTL: 5 * time.Second, jobsMemTTL: 2 * time.Second, commandTime: 4 * time.Second}
}

func (s *SystemSampler) Sample(ctx context.Context) (Metrics, error) {
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return Metrics{}, fmt.Errorf("read memory: %w", err)
	}
	now := time.Now()
	m := Metrics{
		At:         now,
		MemTotalMB: int64(vm.Total >> 20),
		MemUsedMB:  int64((vm.Total - vm.Available) >> 20),
		MemPercent: 100 * float64(vm.Total-vm.Available) / float64(vm.Total),
	}
	if sw, err := mem.SwapMemoryWithContext(ctx); err == nil {
		m.SwapPercent = sw.UsedPercent
	}
	if p, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(p) > 0 {
		m.CPUPercent = p[0]
	}
	if l, err := load.AvgWithContext(ctx); err == nil {
		m.Load1, m.Load5, m.Load15 = l.Load1, l.Load5, l.Load15
	}
	if u, err := disk.UsageWithContext(ctx, s.workDir); err == nil {
		m.WorkDiskUsedPercent = u.UsedPercent
		m.WorkDiskFreeMB = int64(u.Free >> 20)
	} else {
		return Metrics{}, fmt.Errorf("read disk usage of %s: %w", s.workDir, err)
	}
	m.DockerDiskUsedPercent = m.WorkDiskUsedPercent
	if root := s.dockerRootDir(ctx); root != "" {
		if u, err := disk.UsageWithContext(ctx, root); err == nil {
			m.DockerDiskUsedPercent = u.UsedPercent
		}
	}
	m.CPUTempC = cpuTemperature(ctx)
	m.NetEgressBytesPerSec = s.egressRate(ctx, now)
	m.DockerHealthy = s.dockerHealthy(ctx)
	m.GPUCount, m.GPUUtilPercent = s.gpu(ctx)
	if m.DockerHealthy {
		m.JobsMemUsedMB = s.jobContainersMemMB(ctx)
	}
	return m, nil
}

func cpuTemperature(ctx context.Context) float64 {
	temps, err := sensors.TemperaturesWithContext(ctx)
	if err != nil && len(temps) == 0 {
		return 0
	}
	var best float64
	for _, t := range temps {
		k := strings.ToLower(t.SensorKey)
		if (strings.Contains(k, "core") || strings.Contains(k, "cpu") || strings.Contains(k, "k10temp") ||
			strings.Contains(k, "package") || strings.Contains(k, "tctl")) && t.Temperature > best {
			best = t.Temperature
		}
	}
	return best
}

func (s *SystemSampler) egressRate(ctx context.Context, now time.Time) float64 {
	c, err := gnet.IOCountersWithContext(ctx, false)
	if err != nil || len(c) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := c[0].BytesSent
	rate := 0.0
	if s.haveNet && now.After(s.lastNetAt) && cur >= s.lastNet {
		rate = float64(cur-s.lastNet) / now.Sub(s.lastNetAt).Seconds()
	}
	s.lastNet, s.lastNetAt, s.haveNet = cur, now, true
	return rate
}

func (s *SystemSampler) run(ctx context.Context, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, s.commandTime)
	defer cancel()
	out, err := exec.CommandContext(cctx, s.dockerBinary, args...).Output()
	return strings.TrimSpace(string(out)), err
}

func (s *SystemSampler) dockerHealthy(ctx context.Context) bool {
	s.mu.Lock()
	if time.Since(s.dockerAt) < s.probeTTL {
		ok := s.dockerOK
		s.mu.Unlock()
		return ok
	}
	s.mu.Unlock()
	_, err := s.run(ctx, "version", "--format", "{{.Server.Version}}")
	s.mu.Lock()
	s.dockerOK, s.dockerAt = err == nil, time.Now()
	s.mu.Unlock()
	return err == nil
}

func (s *SystemSampler) dockerRootDir(ctx context.Context) string {
	s.mu.Lock()
	if s.rootProbed {
		r := s.dockerRoot
		s.mu.Unlock()
		return r
	}
	s.mu.Unlock()
	out, err := s.run(ctx, "info", "--format", "{{.DockerRootDir}}")
	if err != nil {
		return "" // try again next sample
	}
	s.mu.Lock()
	s.dockerRoot, s.rootProbed = out, true
	s.mu.Unlock()
	return out
}

func (s *SystemSampler) gpu(ctx context.Context) (int, float64) {
	s.mu.Lock()
	if time.Since(s.gpuAt) < s.probeTTL {
		c, u := s.gpuCount, s.gpuUtil
		s.mu.Unlock()
		return c, u
	}
	s.mu.Unlock()
	count, util := 0, 0.0
	if _, err := exec.LookPath("nvidia-smi"); err == nil {
		cctx, cancel := context.WithTimeout(ctx, s.commandTime)
		out, err := exec.CommandContext(cctx, "nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits").Output()
		cancel()
		if err == nil {
			var sum float64
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if v, err := strconv.ParseFloat(strings.TrimSpace(line), 64); err == nil {
					count++
					sum += v
				}
			}
			if count > 0 {
				util = sum / float64(count)
			}
		}
	}
	s.mu.Lock()
	s.gpuCount, s.gpuUtil, s.gpuAt = count, util, time.Now()
	s.mu.Unlock()
	return count, util
}

// jobContainersMemMB is the memory used by this worker's job containers right now.
func (s *SystemSampler) jobContainersMemMB(ctx context.Context) int64 {
	s.mu.Lock()
	if time.Since(s.jobsMemAt) < s.jobsMemTTL {
		v := s.jobsMemMB
		s.mu.Unlock()
		return v
	}
	s.mu.Unlock()

	var total float64
	ids, err := s.run(ctx, "ps", "-q", "--filter", "label=computehive.worker.id="+s.workerID)
	if err == nil && ids != "" {
		args := append([]string{"stats", "--no-stream", "--format", "{{.MemUsage}}"}, strings.Fields(ids)...)
		if out, err := s.run(ctx, args...); err == nil {
			for _, line := range strings.Split(out, "\n") {
				used, _, _ := strings.Cut(line, "/")
				if mb, ok := ParseDockerSizeMB(used); ok {
					total += mb
				}
			}
		}
	}
	s.mu.Lock()
	s.jobsMemMB, s.jobsMemAt = int64(total), time.Now()
	s.mu.Unlock()
	return int64(total)
}

// ParseDockerSizeMB parses sizes as printed by `docker stats` ("12.5MiB", "1.2GiB", "512kB") into MiB.
func ParseDockerSizeMB(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, false
	}
	const mib = 1 << 20
	switch strings.ToLower(strings.TrimSpace(s[i:])) {
	case "b":
		return v / mib, true
	case "kb":
		return v * 1000 / mib, true
	case "kib":
		return v / 1024, true
	case "mb":
		return v * 1e6 / mib, true
	case "mib":
		return v, true
	case "gb":
		return v * 1e9 / mib, true
	case "gib":
		return v * 1024, true
	case "tb":
		return v * 1e12 / mib, true
	case "tib":
		return v * 1024 * 1024, true
	}
	return 0, false
}
