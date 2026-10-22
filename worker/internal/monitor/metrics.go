// Package monitor samples host and Docker resource usage and decides, with
// hysteresis, whether this worker may accept new jobs.
package monitor

import (
	"context"
	"time"
)

// Metrics is one resource sample. Percentages are 0-100.
type Metrics struct {
	At time.Time

	MemTotalMB  int64
	MemUsedMB   int64 // total minus available (page cache that can be reclaimed is not "used")
	MemPercent  float64
	SwapPercent float64

	CPUPercent float64
	Load1      float64
	Load5      float64
	Load15     float64

	WorkDiskFreeMB        int64   // free space on that filesystem, for disk-hungry jobs
	WorkDiskUsedPercent   float64 // filesystem holding job work directories
	DockerDiskUsedPercent float64 // filesystem holding Docker's data root (falls back to the work dir)

	CPUTempC             float64 // 0 when no sensor is available
	DockerHealthy        bool
	GPUCount             int
	GPUUtilPercent       float64
	NetEgressBytesPerSec float64

	// JobsMemUsedMB is what this worker's job containers are actually using right now.
	JobsMemUsedMB int64
}

// DiskPercent is the worse of the two disks the worker depends on.
func (m Metrics) DiskPercent() float64 { return max(m.WorkDiskUsedPercent, m.DockerDiskUsedPercent) }

// Sampler produces Metrics. Tests inject fakes; production uses SystemSampler.
type Sampler interface {
	Sample(ctx context.Context) (Metrics, error)
}

// SamplerFunc adapts a function to Sampler.
type SamplerFunc func(ctx context.Context) (Metrics, error)

func (f SamplerFunc) Sample(ctx context.Context) (Metrics, error) { return f(ctx) }
