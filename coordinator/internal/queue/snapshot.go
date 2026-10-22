package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// reliabilityHalfLife is how fast a worker's history fades: an hour-old failure counts half as much.
const reliabilityHalfLife = time.Hour

func jobFromMap(m map[string]string) *Job {
	j := &Job{
		ID: m["id"], State: m["state"], Seq: atoi64(m["seq"]), Attempt: int(atoi64(m["attempt"])),
		MaxAttempts: int(atoi64(m["max_attempts"])), Worker: m["worker"], LastFailure: m["last_failure"],
		LastFailureMs: atoi64(m["last_failure_ms"]), CreatedMs: atoi64(m["created_ms"]),
		AssignedMs: atoi64(m["assigned_ms"]), StartedMs: atoi64(m["started_ms"]),
		FinishedMs: atoi64(m["finished_ms"]), NotBeforeMs: atoi64(m["not_before"]), Commits: int(atoi64(m["commits"])),
		Exclude: m["exclude"],
	}
	_ = json.Unmarshal([]byte(m["spec"]), &j.Spec)
	if m["verify"] == "1" {
		j.Verification = m["verified"]
		if j.Verification == "" {
			j.Verification = "pending"
		}
	}
	return j
}

// WorkerState is everything the scheduler knows about a worker at one instant.
type WorkerState struct {
	ID                                     string
	Alive, Overloaded, Draining, AllowsNet bool
	HasUsage                               bool
	CPU, GPU, MaxJobs, Preference, MemMB   int
	OS, Arch, Isolation, Version           string
	Labels                                 []string
	DiskFreeMB                             int64
	Load1, CPUPct, MemPct                  float64
	MemTotalMB, MemUsedMB, JobsMemMB       int64
	Running                                []string
	DeclaredRunningMemMB                   int64
	HeartbeatAgeMs, RegisteredMs           int64
	Reliability, ReliabilitySamples        float64
	Cached                                 map[string]bool
	OverloadReasons                        string
}

// Reliability turns a decayed (ok, fail) record into a 0-1 score. A Laplace-style prior of two
// successes and two failures keeps a brand-new worker at 0.5 instead of 0 or 1.
func reliability(ok, fail float64, ts, now int64) (score, samples float64) {
	if ts > 0 && now > ts {
		f := math.Pow(2, -float64(now-ts)/float64(reliabilityHalfLife.Milliseconds()))
		ok, fail = ok*f, fail*f
	}
	return (ok + 2) / (ok + fail + 4), ok + fail
}

func atof(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }

// WorkerStates reads every registered worker in one pipelined round trip.
func (q *Queue) WorkerStates(ctx context.Context) ([]WorkerState, error) {
	ids, err := q.rdb.ZRange(ctx, "ch:workers", 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	t, err := q.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis time: %w", err)
	}
	now := t.UnixMilli()

	pipe := q.rdb.Pipeline()
	type cmds struct {
		hash  *redis.MapStringStringCmd
		run   *redis.StringSliceCmd
		stat  *redis.MapStringStringCmd
		cache *redis.StringSliceCmd
	}
	cs := make([]cmds, len(ids))
	for i, id := range ids {
		cs[i] = cmds{
			hash:  pipe.HGetAll(ctx, "ch:worker:"+id),
			run:   pipe.SMembers(ctx, "ch:wrun:"+id),
			stat:  pipe.HGetAll(ctx, "ch:wstat:"+id),
			cache: pipe.SMembers(ctx, "ch:wcache:"+id),
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read workers: %w", err)
	}

	// Memory promised to each worker's running jobs (their declared size, or the default).
	declared := make(map[string]int64, len(ids))
	jobPipe := q.rdb.Pipeline()
	type runCmd struct {
		worker string
		cmd    *redis.StringCmd
	}
	var runCmds []runCmd
	for i, id := range ids {
		running, _ := cs[i].run.Result()
		for _, jid := range running {
			runCmds = append(runCmds, runCmd{id, jobPipe.HGet(ctx, "ch:job:"+jid, "req_mem")})
		}
	}
	if len(runCmds) > 0 {
		_, _ = jobPipe.Exec(ctx)
	}
	for _, rc := range runCmds {
		mb := atoi64(rc.cmd.Val())
		if mb == 0 {
			mb = int64(q.cfg.DefaultJobMemMB)
		}
		declared[rc.worker] += mb
	}

	out := make([]WorkerState, 0, len(ids))
	for i, id := range ids {
		m, _ := cs[i].hash.Result()
		running, _ := cs[i].run.Result()
		stat, _ := cs[i].stat.Result()
		cached, _ := cs[i].cache.Result()
		last := atoi64(m["last_hb_ms"])
		rel, samples := reliability(atof(stat["ok"]), atof(stat["fail"]), atoi64(stat["ts"]), now)
		var labels []string
		for _, l := range strings.Split(strings.Trim(m["labels"], ","), ",") {
			if l != "" {
				labels = append(labels, l)
			}
		}
		pref := int(atoi64(m["preference"]))
		if pref == 0 {
			pref = 50
		}
		ws := WorkerState{
			ID: id, Alive: now-last <= q.cfg.DeadAfter.Milliseconds(), Overloaded: m["overloaded"] == "1", Draining: m["draining"] == "1",
			AllowsNet: m["allows_net"] == "1", HasUsage: m["mem_pct"] != "",
			CPU: int(atoi64(m["cpu"])), GPU: int(atoi64(m["gpu"])), MemMB: int(atoi64(m["mem_mb"])), MaxJobs: max(int(atoi64(m["max_jobs"])), 1),
			Preference: pref, OS: m["os"], Arch: m["arch"], Isolation: m["isolation"], Version: m["version"], Labels: labels,
			DiskFreeMB: atoi64(m["disk_free_mb"]), Load1: atof(m["load1"]), CPUPct: atof(m["cpu_pct"]), MemPct: atof(m["mem_pct"]),
			MemTotalMB: atoi64(m["mem_total"]), MemUsedMB: atoi64(m["mem_used"]), JobsMemMB: atoi64(m["jobs_mem"]),
			Running: running, DeclaredRunningMemMB: declared[id], HeartbeatAgeMs: now - last, RegisteredMs: atoi64(m["registered_ms"]),
			Reliability: rel, ReliabilitySamples: samples, Cached: make(map[string]bool, len(cached)), OverloadReasons: m["over_reasons"],
		}
		for _, sha := range cached {
			ws.Cached[sha] = true
		}
		out = append(out, ws)
	}
	return out, nil
}

// PendingJob is a waiting job with how long it has waited.
type PendingJob struct {
	Job
	WaitMs int64
}

// PendingJobs returns the first limit jobs of the queue in FIFO order.
func (q *Queue) PendingJobs(ctx context.Context, limit int) ([]PendingJob, error) {
	ids, err := q.rdb.ZRange(ctx, "ch:pending", 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("list pending: %w", err)
	}
	t, err := q.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis time: %w", err)
	}
	now := t.UnixMilli()
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, "ch:job:"+id)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read pending jobs: %w", err)
	}
	out := make([]PendingJob, 0, len(ids))
	for _, c := range cmds {
		m, _ := c.Result()
		if len(m) == 0 {
			continue
		}
		j := jobFromMap(m)
		if j.State != StatePending && j.State != StateRequeued {
			continue
		}
		wait := now - j.CreatedMs
		if j.NotBeforeMs > now { // still backing off: it is not waiting for a worker yet
			wait = 0
		}
		out = append(out, PendingJob{Job: *j, WaitMs: wait})
	}
	return out, nil
}

// Now is Redis's clock in milliseconds, the same clock every script and snapshot uses.
func (q *Queue) Now(ctx context.Context) (int64, error) {
	t, err := q.rdb.Time(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("redis time: %w", err)
	}
	return t.UnixMilli(), nil
}

// RecordDecision stores the scheduler's explanation of why a job was placed where it was, on the job
// itself, so `explain` can show it after the fact. It never creates a job hash: a job that was
// finished or deleted in the meantime is left alone.
func (q *Queue) RecordDecision(ctx context.Context, jobID, decisionJSON string) error {
	const script = `if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('HSET', KEYS[1], 'decision', ARGV[1]) end return 1`
	return q.rdb.Eval(ctx, script, []string{"ch:job:" + jobID}, decisionJSON).Err()
}

// JobDecision returns the recorded placement decision of a job ("" if none was recorded).
func (q *Queue) JobDecision(ctx context.Context, jobID string) (string, error) {
	v, err := q.rdb.HGet(ctx, "ch:job:"+jobID, "decision").Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// UserRunning is how many jobs of user are currently running.
func (q *Queue) UserRunning(ctx context.Context, user string) (int64, error) {
	n, err := q.rdb.Get(ctx, "ch:urun:"+user).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}
