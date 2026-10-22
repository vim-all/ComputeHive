// Package scheduler decides which worker should run which job. It is pure logic over a snapshot of
// the cluster (no I/O), so every decision is reproducible, explainable and unit-testable.
//
// Placement has two stages:
//
//  1. Hard filters. A worker that is stale, overloaded, draining, unreliable, lacks the requested
//     resources / OS / architecture / labels / isolation / network permission, has no memory headroom,
//     or is excluded for verification, is rejected, with a reason that is shown to the user.
//  2. A transparent weighted score over the survivors (each component 0..1, times its weight).
//
// The Lua claim script re-checks the safety-critical filters atomically, so a stale snapshot can
// make a placement suboptimal but never unsafe.
package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"coordinator/internal/queue"
)

// Weights are the score components. They should sum to about 1 so totals stay in 0..1.
type Weights struct {
	FreeRAM     float64 // RAM left after placing the job: spreads load, avoids hot-spotting
	FreeCPU     float64 // CPU currently idle
	Load        float64 // load average per core
	Reliability float64 // decayed success rate of recent jobs
	Locality    float64 // the job's artifact is already on the worker (no download)
	Uptime      float64 // how long the worker has been a stable member
	Preference  float64 // the contributor's own willingness to take work
	Fit         float64 // best-fit: small jobs prefer small workers, keeping big ones free (anti-fragmentation)
}

// DefaultWeights balance spreading (free RAM, CPU, load) against packing (fit).
func DefaultWeights() Weights {
	return Weights{FreeRAM: 0.22, FreeCPU: 0.14, Load: 0.14, Reliability: 0.20, Locality: 0.10, Uptime: 0.05, Preference: 0.05, Fit: 0.10}
}

// Map returns the weights by component name, for display.
func (w Weights) Map() map[string]float64 {
	return map[string]float64{"free_ram": w.FreeRAM, "free_cpu": w.FreeCPU, "load": w.Load, "reliability": w.Reliability,
		"locality": w.Locality, "uptime": w.Uptime, "preference": w.Preference, "fit": w.Fit}
}

// Config tunes placement.
type Config struct {
	Weights Weights
	// Patience: a job that has waited this long is given to ANY eligible idle worker that asks, even if a
	// better one exists, so a better-but-silent worker cannot starve it.
	Patience time.Duration

	LimitMemPct     float64 // memory headroom rule (same as the claim script)
	DefaultJobMemMB int64
	RequireUsage    bool
	UserLimit       int // max running jobs per user (0 = unlimited)

	// A worker whose decayed reliability is below MinReliability after at least MinSamples jobs is blacklisted.
	MinReliability float64
	MinSamples     float64

	UptimeFull time.Duration // uptime that earns the full uptime score
}

// DefaultConfig matches the coordinator's defaults.
func DefaultConfig() Config {
	return Config{Weights: DefaultWeights(), Patience: 5 * time.Second, LimitMemPct: 90, DefaultJobMemMB: 512, RequireUsage: true,
		MinReliability: 0.2, MinSamples: 5, UptimeFull: 24 * time.Hour}
}

// Candidate is one worker's evaluation for one job.
type Candidate struct {
	WorkerID string
	Eligible bool
	Busy     bool // eligible but has no free slot right now
	Reasons  []string
	Total    float64
	Parts    map[string]float64
}

// Decision is the full evaluation of a job against the cluster.
type Decision struct {
	JobID      string
	Chosen     string // best eligible idle worker ("" if none)
	Candidates []Candidate
	At         time.Time
}

func clamp01(v float64) float64 { return math.Max(0, math.Min(1, v)) }

func hasLabel(w queue.WorkerState, want string) bool {
	for _, l := range w.Labels {
		if l == want {
			return true
		}
	}
	return false
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// Filter returns why w cannot run j (empty if it can). userRunning is how many jobs j's user has running.
func Filter(w queue.WorkerState, j queue.PendingJob, userRunning int64, cfg Config) []string {
	var r []string
	sp := j.Spec
	add := func(f string, a ...any) { r = append(r, fmt.Sprintf(f, a...)) }

	if !w.Alive {
		add("heartbeat stale (%.1fs old)", float64(w.HeartbeatAgeMs)/1000)
	}
	if w.Draining {
		add("draining or paused by its contributor")
	}
	if w.Overloaded {
		why := w.OverloadReasons
		if why == "" {
			why = "reported by the worker"
		}
		add("overloaded (%s): the 90%% rule", why)
	}
	if cfg.RequireUsage && !w.HasUsage {
		add("has not reported resource usage")
	}
	if j.Exclude != "" && j.Exclude == w.ID {
		add("excluded: ran the other verification replica of this job")
	}
	if int(sp.CPUCores) > w.CPU {
		add("needs %d CPU, worker offers %d", sp.CPUCores, w.CPU)
	}
	if int(sp.MemoryMB) > w.MemMB && w.MemMB > 0 {
		add("needs %d MB RAM, worker offers %d", sp.MemoryMB, w.MemMB)
	}
	if int(sp.GPUCount) > w.GPU {
		add("needs %d GPU, worker has %d", sp.GPUCount, w.GPU)
	}
	if sp.MinDiskMB > 0 && w.DiskFreeMB < int64(sp.MinDiskMB) {
		add("needs %d MB free disk, worker has %d", sp.MinDiskMB, w.DiskFreeMB)
	}
	if sp.OS != "" && sp.OS != w.OS {
		add("needs os %s, worker is %s", sp.OS, orUnknown(w.OS))
	}
	if sp.Arch != "" && sp.Arch != w.Arch {
		add("needs arch %s, worker is %s", sp.Arch, orUnknown(w.Arch))
	}
	if sp.RequiredIsolation != "" && sp.RequiredIsolation != w.Isolation {
		add("needs %s isolation, worker provides %s", sp.RequiredIsolation, orUnknown(w.Isolation))
	}
	if len(sp.NetworkHosts) > 0 && !w.AllowsNet {
		add("job needs network access, this contributor does not allow network jobs")
	}
	for _, l := range sp.Labels {
		if !hasLabel(w, l) {
			add("missing required label %s", l)
		}
	}
	if w.MemTotalMB > 0 && cfg.LimitMemPct > 0 {
		req := int64(sp.MemoryMB)
		if req == 0 {
			req = cfg.DefaultJobMemMB
		}
		unramped := max(w.DeclaredRunningMemMB-w.JobsMemMB, 0)
		if proj := float64(w.MemUsedMB+unramped+req) * 100 / float64(w.MemTotalMB); proj >= cfg.LimitMemPct {
			add("no memory headroom: placing %d MB would take RAM to %.0f%% (limit %.0f%%)", req, proj, cfg.LimitMemPct)
		}
	}
	if w.ReliabilitySamples >= cfg.MinSamples && w.Reliability < cfg.MinReliability {
		add("blacklisted: recent reliability %.2f over %.0f jobs", w.Reliability, w.ReliabilitySamples)
	}
	if sp.User != "" && cfg.UserLimit > 0 && userRunning >= int64(cfg.UserLimit) {
		add("user %s already has %d jobs running (limit %d)", sp.User, userRunning, cfg.UserLimit)
	}
	return r
}

// Score weights each component for an eligible worker. nowMs is the current time in milliseconds.
func Score(w queue.WorkerState, j queue.PendingJob, nowMs int64, cfg Config) (float64, map[string]float64) {
	sp := j.Spec
	req := float64(sp.MemoryMB)
	if req == 0 {
		req = float64(cfg.DefaultJobMemMB)
	}
	total := math.Max(float64(w.MemTotalMB), float64(w.MemMB))
	unramped := math.Max(float64(w.DeclaredRunningMemMB-w.JobsMemMB), 0)

	freeRAM, fit := 0.5, 0.5
	if total > 0 {
		freeRAM = clamp01((total - float64(w.MemUsedMB) - unramped - req) / total)
		fit = clamp01(req / total)
	}
	load := 1.0
	if w.CPU > 0 {
		load = clamp01(1 - w.Load1/float64(w.CPU))
	}
	locality := 0.0
	if sp.ArtifactSHA256 != "" && w.Cached[sp.ArtifactSHA256] {
		locality = 1
	}
	uptime := 0.0
	if w.RegisteredMs > 0 && cfg.UptimeFull > 0 {
		uptime = clamp01(float64(nowMs-w.RegisteredMs) / float64(cfg.UptimeFull.Milliseconds()))
	}
	wt := cfg.Weights
	parts := map[string]float64{
		"free_ram":    wt.FreeRAM * freeRAM,
		"free_cpu":    wt.FreeCPU * clamp01(1-w.CPUPct/100),
		"load":        wt.Load * load,
		"reliability": wt.Reliability * clamp01(w.Reliability),
		"locality":    wt.Locality * locality,
		"uptime":      wt.Uptime * uptime,
		"preference":  wt.Preference * clamp01(float64(w.Preference)/100),
		"fit":         wt.Fit * fit,
	}
	// Fixed order: float addition is not associative, and map order is random, so summing the map
	// directly would make identical workers differ in the last bit and break deterministic tie-breaks.
	var sum float64
	for _, k := range partOrder {
		sum += parts[k]
	}
	return sum, parts
}

var partOrder = []string{"free_ram", "free_cpu", "load", "reliability", "locality", "uptime", "preference", "fit"}

// Evaluate filters and scores every worker for j.
func Evaluate(j queue.PendingJob, workers []queue.WorkerState, userRunning int64, nowMs int64, cfg Config) Decision {
	d := Decision{JobID: j.ID, At: time.UnixMilli(nowMs)}
	for _, w := range workers {
		c := Candidate{WorkerID: w.ID, Reasons: Filter(w, j, userRunning, cfg)}
		c.Eligible = len(c.Reasons) == 0
		c.Busy = len(w.Running) >= w.MaxJobs
		if c.Eligible {
			c.Total, c.Parts = Score(w, j, nowMs, cfg)
		}
		d.Candidates = append(d.Candidates, c)
	}
	// Eligible first by score, then rejected by id. Ties break on worker id so a decision is deterministic.
	sort.SliceStable(d.Candidates, func(a, b int) bool {
		x, y := d.Candidates[a], d.Candidates[b]
		if x.Eligible != y.Eligible {
			return x.Eligible
		}
		if x.Eligible && x.Total != y.Total {
			return x.Total > y.Total
		}
		return x.WorkerID < y.WorkerID
	})
	for _, c := range d.Candidates {
		if c.Eligible && !c.Busy {
			d.Chosen = c.WorkerID
			break
		}
	}
	return d
}

// Offer reports whether worker should be given the job now: it is the best idle eligible worker, or the
// job has waited past the patience window and the worker is eligible and idle.
func (d Decision) Offer(worker string, waited time.Duration, cfg Config) bool {
	if d.Chosen == worker {
		return true
	}
	if waited < cfg.Patience {
		return false
	}
	for _, c := range d.Candidates {
		if c.WorkerID == worker {
			return c.Eligible && !c.Busy
		}
	}
	return false
}

// Summary is a one-line human explanation.
func (d Decision) Summary() string {
	if d.Chosen == "" {
		eligible := 0
		for _, c := range d.Candidates {
			if c.Eligible {
				eligible++
			}
		}
		switch {
		case len(d.Candidates) == 0:
			return "no workers are registered"
		case eligible == 0:
			return "no worker can run this job: every worker was rejected (see reasons)"
		default:
			return fmt.Sprintf("%d worker(s) could run it but all are busy; it will go to the best one that frees up", eligible)
		}
	}
	for _, c := range d.Candidates {
		if c.WorkerID == d.Chosen {
			top, best := "", 0.0
			for k, v := range c.Parts {
				if v > best || (v == best && k < top) {
					top, best = k, v
				}
			}
			return fmt.Sprintf("%s scored highest (%.3f); its strongest component was %s", d.Chosen, c.Total, strings.ReplaceAll(top, "_", " "))
		}
	}
	return ""
}
