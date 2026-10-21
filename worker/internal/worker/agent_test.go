package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"github.com/vim-all/ComputeHive/worker/internal/monitor"
)

// ---- fakes ----

type fakeStore struct {
	mu         sync.Mutex
	pulls      atomic.Int64
	jobs       []*domain.Job
	heartbeats []domain.WorkerHeartbeat
	cancelIDs  []string // returned (once) by the next heartbeat
	results    []domain.JobResult
	running    []string
}

func (f *fakeStore) PullJob(context.Context) (*domain.Job, bool, error) {
	f.pulls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.jobs) == 0 {
		return nil, false, nil
	}
	j := f.jobs[0]
	f.jobs = f.jobs[1:]
	return j, true, nil
}

func (f *fakeStore) PublishHeartbeat(_ context.Context, hb domain.WorkerHeartbeat) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, hb)
	c := f.cancelIDs
	f.cancelIDs = nil
	return c, nil
}

func (f *fakeStore) ReportRunning(_ context.Context, id string) error {
	f.mu.Lock()
	f.running = append(f.running, id)
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) PublishJobResult(_ context.Context, r domain.JobResult) (string, error) {
	f.mu.Lock()
	f.results = append(f.results, r)
	f.mu.Unlock()
	return "ok", nil
}

func (f *fakeStore) Forget(string) {}

func (f *fakeStore) resultsCopy() []domain.JobResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.JobResult(nil), f.results...)
}

func (f *fakeStore) lastHeartbeat() (domain.WorkerHeartbeat, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.heartbeats) == 0 {
		return domain.WorkerHeartbeat{}, false
	}
	return f.heartbeats[len(f.heartbeats)-1], true
}

type fakeReporter struct{}

func (fakeReporter) Snapshot(context.Context) domain.ResourceSnapshot {
	return domain.ResourceSnapshot{CPUCores: 8, MemoryMB: 16000, DockerAvailable: true}
}

// blockingExecutor runs "forever" until its context ends, like a long job being stopped.
type blockingExecutor struct {
	started chan string
	runFor  time.Duration // if > 0, finish successfully after this long
}

func (e *blockingExecutor) Run(ctx context.Context, job domain.Job, _ artifact.Bundle) domain.JobResult {
	select {
	case e.started <- job.ID:
	default:
	}
	if e.runFor > 0 {
		select {
		case <-time.After(e.runFor):
			return domain.JobResult{JobID: job.ID, Status: domain.JobStatusSucceeded, FinishedAt: time.Now()}
		case <-ctx.Done():
		}
	} else {
		<-ctx.Done()
	}
	// What the real executor returns after killing the container.
	return domain.JobResult{JobID: job.ID, Status: domain.JobStatusFailed, ExitCode: -1, FinishedAt: time.Now()}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func healthy(ram float64) monitor.Metrics {
	return monitor.Metrics{MemPercent: ram, CPUPercent: 10, WorkDiskUsedPercent: 10, DockerDiskUsedPercent: 10, DockerHealthy: true, MemTotalMB: 16000, At: time.Now()}
}

type rig struct {
	agent *Agent
	store *fakeStore
	mon   *monitor.Monitor
	clk   *fakeClock
	exec  *blockingExecutor
}

func newRig(t *testing.T, jobs ...*domain.Job) *rig {
	t.Helper()
	cfg := config.Config{
		WorkerID: "w", HeartbeatInterval: 30 * time.Millisecond, PollInterval: 5 * time.Millisecond,
		PollTimeout: time.Second, DefaultJobTimeout: time.Minute, PressureGrace: time.Second,
	}
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	mcfg := monitor.DefaultConfig()
	mcfg.CPUWindow = 1
	mon := monitor.New(nil, mcfg, clk.now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	st := &fakeStore{jobs: jobs}
	ex := &blockingExecutor{started: make(chan string, 4)}
	a := NewAgent(cfg, st, nil, fakeReporter{}, ex, nil, mon, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &rig{agent: a, store: st, mon: mon, clk: clk, exec: ex}
}

func (r *rig) start(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.agent.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel
}

func eventually(t *testing.T, within time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pullsStayFlat asserts the worker makes no RequestJob calls for a while.
func pullsStayFlat(t *testing.T, r *rig, why string) {
	t.Helper()
	before := r.store.pulls.Load()
	time.Sleep(120 * time.Millisecond) // 24 poll intervals
	if got := r.store.pulls.Load(); got != before {
		t.Fatalf("%s: worker made %d RequestJob call(s) while it must not", why, got-before)
	}
}

// ---- tests ----

func TestWorkerStopsRequestingJobsAtThresholdAndResumesAfterHysteresis(t *testing.T) {
	r := newRig(t)
	r.mon.Observe(healthy(50))
	r.start(t)

	eventually(t, time.Second, func() bool { return r.store.pulls.Load() > 5 }, "worker polls while healthy")

	r.mon.Observe(healthy(90)) // exactly at the limit
	time.Sleep(30 * time.Millisecond)
	pullsStayFlat(t, r, "RAM at 90%")

	r.mon.Observe(healthy(85)) // inside the 80-90 band
	pullsStayFlat(t, r, "RAM back to 85% (hysteresis band)")

	r.mon.Observe(healthy(70)) // below 80%: recovery timer starts
	r.clk.advance(5 * time.Second)
	r.mon.Observe(healthy(70))
	pullsStayFlat(t, r, "RAM at 70% for only 5s of the required 10s")

	r.clk.advance(5 * time.Second)
	r.mon.Observe(healthy(70)) // 10s continuously below 80%
	before := r.store.pulls.Load()
	eventually(t, time.Second, func() bool { return r.store.pulls.Load() > before+3 }, "worker resumes polling after 10s below 80%")
}

func TestWorkerDoesNotPullBeforeItHasAnyMetrics(t *testing.T) {
	r := newRig(t) // monitor has seen no samples
	r.start(t)
	pullsStayFlat(t, r, "no samples yet")
}

func TestHeartbeatReportsUsageAndOverload(t *testing.T) {
	r := newRig(t)
	r.mon.Observe(healthy(40))
	r.start(t)
	eventually(t, time.Second, func() bool { hb, ok := r.store.lastHeartbeat(); return ok && hb.Usage != nil }, "heartbeat with usage")
	hb, _ := r.store.lastHeartbeat()
	if hb.Overloaded || hb.Usage.MemPercent != 40 || hb.MaxJobs != 1 {
		t.Fatalf("healthy heartbeat: %+v usage=%+v", hb, hb.Usage)
	}

	r.mon.Observe(healthy(93))
	eventually(t, time.Second, func() bool { hb, _ := r.store.lastHeartbeat(); return hb.Overloaded }, "heartbeat flags overload")
	hb, _ = r.store.lastHeartbeat()
	if len(hb.OverloadReasons) != 1 || hb.OverloadReasons[0] != "ram>=90%" || hb.Usage.MemPercent != 93 {
		t.Fatalf("overloaded heartbeat: reasons=%v usage=%+v", hb.OverloadReasons, hb.Usage)
	}
}

func TestPressureEvictsRunningJobAfterGraceAndRequeuesItWithoutBlame(t *testing.T) {
	r := newRig(t, &domain.Job{ID: "victim", ImageRef: "img", LeaseToken: "tok", Attempt: 1})
	r.mon.Observe(healthy(50))
	r.start(t)
	select {
	case <-r.exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	// The host goes critical while the job runs, but the grace period has not elapsed: the job keeps running.
	r.mon.Observe(healthy(95))
	r.clk.advance(500 * time.Millisecond)
	r.mon.Observe(healthy(95))
	time.Sleep(1300 * time.Millisecond) // > one pressure tick
	if n := len(r.store.resultsCopy()); n != 0 {
		t.Fatalf("job was evicted before the grace period (%d results)", n)
	}

	// Still critical after the grace period: evicted and handed back as "evicted" (not the job's fault).
	r.clk.advance(2 * time.Second)
	r.mon.Observe(healthy(96))
	eventually(t, 3*time.Second, func() bool { return len(r.store.resultsCopy()) == 1 }, "eviction result")
	res := r.store.resultsCopy()[0]
	if res.JobID != "victim" || res.FailureReason != ReasonEvicted || res.Status != domain.JobStatusFailed {
		t.Fatalf("eviction result: %+v", res)
	}
	if res.FailureReason == ReasonJobExit || res.FailureReason == ReasonInfra {
		t.Fatal("eviction must not be reported as the job's fault or an infra failure")
	}

	// It does not evict again, and it takes no new work while still overloaded.
	time.Sleep(1200 * time.Millisecond)
	if n := len(r.store.resultsCopy()); n != 1 {
		t.Fatalf("job evicted more than once: %d results", n)
	}
	pullsStayFlat(t, r, "still overloaded after eviction")
}

func TestCoordinatorRevokingTheLeaseAbortsTheJobAndDiscardsItsResult(t *testing.T) {
	r := newRig(t, &domain.Job{ID: "j", ImageRef: "img", LeaseToken: "tok", Attempt: 1})
	r.mon.Observe(healthy(50))
	r.start(t)
	select {
	case <-r.exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}
	r.store.mu.Lock()
	r.store.cancelIDs = []string{"j"} // the next heartbeat reply tells the worker it no longer owns "j"
	r.store.mu.Unlock()

	time.Sleep(400 * time.Millisecond)
	if n := len(r.store.resultsCopy()); n != 0 {
		t.Fatalf("a worker that lost its lease must not report: %+v", r.store.resultsCopy())
	}
	// ...and it is free to take new work.
	r.store.mu.Lock()
	r.store.jobs = append(r.store.jobs, &domain.Job{ID: "next", ImageRef: "img", LeaseToken: "t2", Attempt: 1})
	r.store.mu.Unlock()
	select {
	case id := <-r.exec.started:
		if id != "next" {
			t.Fatalf("started %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not recover after the abort")
	}
}

func TestShutdownHandsTheRunningJobBackAsEvicted(t *testing.T) {
	r := newRig(t, &domain.Job{ID: "j", ImageRef: "img", LeaseToken: "tok", Attempt: 1})
	r.mon.Observe(healthy(50))
	cancel := r.start(t)
	select {
	case <-r.exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}
	cancel() // SIGTERM
	eventually(t, 3*time.Second, func() bool { return len(r.store.resultsCopy()) == 1 }, "job handed back on shutdown")
	if res := r.store.resultsCopy()[0]; res.FailureReason != ReasonEvicted {
		t.Fatalf("shutdown hand-back: %+v", res)
	}
}

func TestFinishedJobReportsSuccess(t *testing.T) {
	r := newRig(t, &domain.Job{ID: "ok", ImageRef: "img", LeaseToken: "tok", Attempt: 1})
	r.exec.runFor = 20 * time.Millisecond
	r.mon.Observe(healthy(50))
	r.start(t)
	eventually(t, 2*time.Second, func() bool { return len(r.store.resultsCopy()) == 1 }, "result")
	if res := r.store.resultsCopy()[0]; res.Status != domain.JobStatusSucceeded || res.FailureReason != "" {
		t.Fatalf("result: %+v", res)
	}
}
