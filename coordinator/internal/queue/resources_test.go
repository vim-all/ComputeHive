package queue

import (
	"context"
	"testing"
	"time"
)

func withUsage(mem, cpu, disk float64) func(*WorkerReport) {
	return func(r *WorkerReport) {
		r.Usage = &Usage{MemTotalMB: 16000, MemUsedMB: int64(mem * 160), MemPct: mem, CPUPct: cpu, DiskPct: disk, JSON: "{}"}
	}
}

func tryClaim(t *testing.T, q *Queue, worker string) (*Claimed, string) {
	t.Helper()
	c, reason, err := q.Claim(context.Background(), worker)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return c, reason
}

// The worker says it is fine; the numbers it reports say otherwise. The coordinator believes the numbers.
func TestCoordinatorDerivesOverloadFromMetricsNotFromTheWorkersFlag(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueue(t, q, "j1")

	hb(t, q, "liar", withUsage(95, 10, 10)) // Overloaded flag left false
	if c, reason := tryClaim(t, q, "liar"); c != nil || reason != "overloaded" {
		t.Fatalf("RAM 95%% worker was assigned work: %v %q", c, reason)
	}
	ws, _ := q.ListWorkers(context.Background())
	if len(ws) != 1 || !ws[0].Overloaded || ws[0].OverloadReasons != "ram" {
		t.Fatalf("worker view: %+v", ws)
	}
}

func TestOverloadThresholdsAreInclusiveAndPerMetric(t *testing.T) {
	cases := []struct {
		name          string
		mem, cpu, dsk float64
		wantOverload  bool
		wantReason    string
	}{
		{"all healthy", 50, 50, 50, false, ""},
		{"ram just below limit", 89.9, 10, 10, false, ""},
		{"ram exactly at limit", 90.0, 10, 10, true, "ram"},
		{"cpu at limit", 10, 90, 10, true, "cpu"},
		{"disk at limit", 10, 10, 90, true, "disk"},
		{"ram and disk", 95, 10, 99, true, "ram,disk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue(t, testConfig())
			// 1 MB declared: isolates the threshold from the separate headroom rule (a 512 MB default job would not fit at 89.9%).
			enqueue(t, q, "j", func(s *Spec) { s.MemoryMB = 1 })
			hb(t, q, "w", withUsage(tc.mem, tc.cpu, tc.dsk))
			c, reason := tryClaim(t, q, "w")
			if tc.wantOverload && (c != nil || reason != "overloaded") {
				t.Fatalf("expected rejection as overloaded, got %v %q", c, reason)
			}
			if !tc.wantOverload && c == nil {
				t.Fatalf("healthy worker rejected: %q", reason)
			}
			if ws, _ := q.ListWorkers(context.Background()); ws[0].OverloadReasons != tc.wantReason {
				t.Fatalf("reasons %q, want %q", ws[0].OverloadReasons, tc.wantReason)
			}
		})
	}
}

// Enter at >= 90%; leave only after staying below 80% for ReleaseAfter. Between 80 and 90 nothing changes.
func TestHysteresisPreventsFlapping(t *testing.T) {
	cfg := testConfig()
	cfg.ReleaseAfter = 400 * time.Millisecond
	q := newQueue(t, cfg)
	enqueue(t, q, "j")

	// probe reports whether the worker would be given the job, and puts the job back so probing has no side effects.
	probe := func() bool {
		c, _ := tryClaim(t, q, "w")
		if c == nil {
			return false
		}
		if _, _, err := q.Fail(context.Background(), "j", c.Token, ReasonEvicted, false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		return true
	}

	hb(t, q, "w", withUsage(70, 10, 10))
	if !probe() {
		t.Fatal("healthy worker should receive work")
	}
	hb(t, q, "w", withUsage(91, 10, 10))
	if probe() {
		t.Fatal("must stop at >= 90%")
	}
	hb(t, q, "w", withUsage(85, 10, 10)) // between 80 and 90: still overloaded
	if probe() {
		t.Fatal("dropping to 85% must NOT resume (hysteresis band is 80-90)")
	}
	hb(t, q, "w", withUsage(70, 10, 10)) // below 80, but the timer has only just started
	if probe() {
		t.Fatal("dropping below 80% must not resume immediately")
	}
	time.Sleep(250 * time.Millisecond)
	hb(t, q, "w", withUsage(85, 10, 10)) // bounces back into the band: timer resets
	hb(t, q, "w", withUsage(70, 10, 10))
	time.Sleep(250 * time.Millisecond) // 250ms since the reset: not enough
	hb(t, q, "w", withUsage(70, 10, 10))
	if probe() {
		t.Fatal("a bounce into the 80-90 band must restart the recovery timer")
	}
	time.Sleep(450 * time.Millisecond)
	hb(t, q, "w", withUsage(70, 10, 10)) // continuously below 80% for >= ReleaseAfter
	if !probe() {
		t.Fatal("must resume after staying below 80% for ReleaseAfter")
	}
}

func TestWorkersWithoutMetricsAreRefusedWhenRequired(t *testing.T) {
	cfg := testConfig()
	cfg.RequireUsage = true
	q := newQueue(t, cfg)
	enqueue(t, q, "j")

	hb(t, q, "silent") // no usage at all
	if c, reason := tryClaim(t, q, "silent"); c != nil || reason != "no_metrics" {
		t.Fatalf("worker without metrics was assigned work: %v %q", c, reason)
	}
	hb(t, q, "silent", withUsage(40, 10, 10))
	if c, _ := tryClaim(t, q, "silent"); c == nil {
		t.Fatal("worker with healthy metrics should get work")
	}

	// Stopping the metrics must not let it replay the old healthy numbers.
	enqueue(t, q, "j2")
	hb(t, q, "silent", func(r *WorkerReport) { r.MaxJobs = 5 })
	if c, reason := tryClaim(t, q, "silent"); c != nil || reason != "no_metrics" {
		t.Fatalf("stale metrics were reused: %v %q", c, reason)
	}
}

func TestStaleWorkerGetsNothingEvenIfLastReportWasHealthy(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueue(t, q, "j")
	hb(t, q, "w", withUsage(10, 10, 10))
	time.Sleep(testConfig().DeadAfter + 100*time.Millisecond)
	if c, reason := tryClaim(t, q, "w"); c != nil || reason != "stale_heartbeat" {
		t.Fatalf("stale worker: %v %q", c, reason)
	}
}

// A job is not placed if its declared memory would push the host past the limit.
func TestMemoryHeadroomIsReservedForTheHostOS(t *testing.T) {
	q := newQueue(t, testConfig())
	// host: 16000 MB total, 10000 MB used (62.5%); limit 90% => 14400 MB.
	enqueue(t, q, "big", func(s *Spec) { s.MemoryMB = 5000 })   // 15000 MB projected = 93.75%: refused
	enqueue(t, q, "small", func(s *Spec) { s.MemoryMB = 2000 }) // 12000 MB projected = 75%: fits
	hb(t, q, "w", func(r *WorkerReport) {
		r.MemMB = 16000
		r.Usage = &Usage{MemTotalMB: 16000, MemUsedMB: 10000, MemPct: 62.5, CPUPct: 10, DiskPct: 10, JSON: "{}"}
	})
	c, _ := tryClaim(t, q, "w")
	if c == nil || c.JobID != "small" {
		t.Fatalf("expected the job that fits to be placed ahead of the one that does not, got %+v", c)
	}
	if st := state(t, q, "big").State; st != StatePending {
		t.Fatalf("job that does not fit must stay queued, is %s", st)
	}
}

func TestHeadroomCountsMemoryPromisedToRunningJobsNotYetUsed(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueue(t, q, "first", func(s *Spec) { s.MemoryMB = 4000 })
	enqueue(t, q, "second", func(s *Spec) { s.MemoryMB = 2000 })
	usage := func(jobsMem int64) func(*WorkerReport) {
		return func(r *WorkerReport) {
			r.MemMB, r.MaxJobs = 16000, 4
			r.Usage = &Usage{MemTotalMB: 16000, MemUsedMB: 10000, JobsMemMB: jobsMem, MemPct: 62.5, CPUPct: 10, DiskPct: 10, JSON: "{}"}
		}
	}
	hb(t, q, "w", usage(0))
	if c, _ := tryClaim(t, q, "w"); c == nil || c.JobID != "first" {
		t.Fatalf("first claim: %+v", c)
	}
	// "first" has declared 4000 MB but uses none yet. 10000 used + 4000 promised + 2000 = 16000 MB (100%): refuse.
	hb(t, q, "w", func(r *WorkerReport) { usage(0)(r); r.Running = []string{"first"} })
	c, reason := tryClaim(t, q, "w")
	if c != nil || reason != "no_headroom" {
		t.Fatalf("memory promised to a running job was double-booked: %+v %q", c, reason)
	}
	// Once the running job's memory shows up in host usage, the promise is no longer counted twice: 12000 + 2000 = 14000 (87.5%) fits.
	hb(t, q, "w", func(r *WorkerReport) {
		r.MemMB, r.MaxJobs, r.Running = 16000, 4, []string{"first"}
		r.Usage = &Usage{MemTotalMB: 16000, MemUsedMB: 12000, JobsMemMB: 4000, MemPct: 75, CPUPct: 10, DiskPct: 10, JSON: "{}"}
	})
	if c, reason := tryClaim(t, q, "w"); c == nil {
		t.Fatalf("job that fits was refused: %q", reason)
	}
}
