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

// A job that demands gVisor never lands on a plain-runc worker; unconstrained jobs go anywhere.
func TestRequiredIsolationIsHonouredByTheClaimScript(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueue(t, q, "needs-gvisor", func(s *Spec) { s.RequiredIsolation = "gvisor" })
	enqueue(t, q, "anywhere")

	hb(t, q, "runc-worker", func(r *WorkerReport) { r.Isolation = "hardened-runc" })
	c, _ := tryClaim(t, q, "runc-worker")
	if c == nil || c.JobID != "anywhere" {
		t.Fatalf("the plain worker should skip the gVisor-only job and take the other, got %+v", c)
	}
	if c2, _ := tryClaim(t, q, "runc-worker"); c2 != nil {
		t.Fatalf("a hardened-runc worker was given a job that requires gVisor: %+v", c2)
	}
	if st := state(t, q, "needs-gvisor").State; st != StatePending {
		t.Fatalf("job should still be waiting for a gVisor worker, is %s", st)
	}

	hb(t, q, "gvisor-worker", func(r *WorkerReport) { r.Isolation = "gvisor" })
	c3, _ := tryClaim(t, q, "gvisor-worker")
	if c3 == nil || c3.JobID != "needs-gvisor" {
		t.Fatalf("the gVisor worker should get the job, got %+v", c3)
	}
	ws, _ := q.ListWorkers(context.Background())
	seen := map[string]string{}
	for _, w := range ws {
		seen[w.ID] = w.Isolation
	}
	if seen["gvisor-worker"] != "gvisor" || seen["runc-worker"] != "hardened-runc" {
		t.Fatalf("isolation not surfaced in the worker listing: %v", seen)
	}
}

// Jobs that ask for network access only go to workers whose contributor allows network jobs.
func TestNetworkJobsOnlyGoToWorkersThatAllowThem(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueue(t, q, "wants-net", func(s *Spec) { s.NetworkHosts = []string{"pypi.org"} })
	enqueue(t, q, "offline")

	hb(t, q, "private", func(r *WorkerReport) { r.AllowsNet = false })
	c, _ := tryClaim(t, q, "private")
	if c == nil || c.JobID != "offline" {
		t.Fatalf("a worker that refuses network jobs must skip them, got %+v", c)
	}
	if st := state(t, q, "wants-net").State; st != StatePending {
		t.Fatalf("network job should still be waiting, is %s", st)
	}
	hb(t, q, "open", func(r *WorkerReport) { r.AllowsNet = true })
	c2, _ := tryClaim(t, q, "open")
	if c2 == nil || c2.JobID != "wants-net" {
		t.Fatalf("the network-enabled worker should get the network job, got %+v", c2)
	}
}

// ---- redundant execution (verify) ----

func enqueuePair(t *testing.T, q *Queue, id string) {
	t.Helper()
	if _, created, err := q.EnqueuePair(context.Background(), Spec{ID: id, ContainerImage: "img", CreatedAtUnix: time.Now().Unix()}, 0); err != nil || !created {
		t.Fatalf("enqueue pair: %v %v", created, err)
	}
}

func finishWith(t *testing.T, q *Queue, c *Claimed, digest string) {
	t.Helper()
	d, err := q.Complete(context.Background(), c.JobID, c.Token, true, Result{JobID: c.JobID, OutputDigest: digest})
	if err != nil || d != DispOK {
		t.Fatalf("complete %s: %v %v", c.JobID, d, err)
	}
}

func TestVerifiedJobRunsOnTwoDifferentWorkersAndMatchingOutputSucceeds(t *testing.T) {
	cfg := testConfig()
	cfg.VerifyTimeout = time.Minute
	q := newQueue(t, cfg)
	enqueuePair(t, q, "v")
	hb(t, q, "w1")
	hb(t, q, "w2")

	a := mustClaim(t, q, "w1")
	finishWith(t, q, a, "sha256-of-correct-output")
	if j := state(t, q, a.JobID); j.State != StateVerifying || j.Verification != "pending" {
		t.Fatalf("first replica must wait for its sibling: %+v", j)
	}
	// The sibling replica is still PENDING and w1 is free again, but w1 must never be given it:
	// otherwise one worker could produce both results and forge the agreement. This exercises the
	// exclusion rule itself (capacity is not the reason).
	if n, _ := q.QueueDepth(context.Background()); n != 1 {
		t.Fatalf("the sibling should still be waiting, queue depth %d", n)
	}
	if c, _ := tryClaim(t, q, "w1"); c != nil {
		t.Fatalf("worker w1 was given the sibling replica %s", c.JobID)
	}
	// ...while another worker takes it immediately, proving it was available all along.
	b := mustClaim(t, q, "w2")
	if b.JobID == a.JobID {
		t.Fatalf("the same replica was assigned twice")
	}
	ctx := context.Background()
	if res, err := q.GetResult(ctx, a.JobID); err != nil || res == nil {
		t.Fatalf("result stored but withheld by the API layer: %v %v", res, err)
	}
	finishWith(t, q, b, "sha256-of-correct-output")

	for _, id := range []string{a.JobID, b.JobID} {
		if j := state(t, q, id); j.State != StateSucceeded || j.Verification != "match" || j.Commits != 1 {
			t.Fatalf("after agreement %s: %+v", id, j)
		}
	}
	counts, _ := q.Counts(ctx)
	if counts[StateVerifying] != 0 || counts[StateSucceeded] != 2 {
		t.Fatalf("counts %v", counts)
	}
}

func TestDisagreeingOutputsFailBothReplicas(t *testing.T) {
	cfg := testConfig()
	cfg.VerifyTimeout = time.Minute
	q := newQueue(t, cfg)
	enqueuePair(t, q, "v")
	hb(t, q, "honest")
	hb(t, q, "forger")
	a, b := mustClaim(t, q, "honest"), mustClaim(t, q, "forger")
	finishWith(t, q, a, "digest-honest")
	finishWith(t, q, b, "digest-forged")
	for _, id := range []string{a.JobID, b.JobID} {
		j := state(t, q, id)
		if j.State != StateFailed || j.Verification != "mismatch" || j.LastFailure != "verification_mismatch" {
			t.Fatalf("%s must fail on a mismatch: %+v", id, j)
		}
	}
}

func TestAReplicaWithoutADigestCanNeverBeVerified(t *testing.T) {
	cfg := testConfig()
	cfg.VerifyTimeout = time.Minute
	q := newQueue(t, cfg)
	enqueuePair(t, q, "v")
	hb(t, q, "w1")
	hb(t, q, "w2")
	a, b := mustClaim(t, q, "w1"), mustClaim(t, q, "w2")
	finishWith(t, q, a, "")
	finishWith(t, q, b, "")
	if j := state(t, q, a.JobID); j.State != StateFailed || j.Verification != "mismatch" {
		t.Fatalf("two empty digests must not count as agreement: %+v", j)
	}
}

func TestAVerifiedJobOnAOneWorkerClusterTimesOutInsteadOfWaitingForever(t *testing.T) {
	cfg := testConfig()
	cfg.VerifyTimeout = 300 * time.Millisecond
	cfg.DeadAfter = time.Minute
	q := newQueue(t, cfg)
	enqueuePair(t, q, "v")
	hb(t, q, "only")
	a := mustClaim(t, q, "only")
	finishWith(t, q, a, "d")
	if c, _ := tryClaim(t, q, "only"); c != nil {
		t.Fatal("the only worker was given the second replica")
	}
	time.Sleep(450 * time.Millisecond)
	if n, err := q.ReapVerifyTimeouts(context.Background()); err != nil || n != 1 {
		t.Fatalf("reap: %d %v", n, err)
	}
	if j := state(t, q, a.JobID); j.State != StateFailed || j.Verification != "timeout" || j.LastFailure != "verification_timeout" {
		t.Fatalf("after the timeout: %+v", j)
	}
}

func TestIfTheOtherReplicaFailsTheWaitingOneFailsToo(t *testing.T) {
	cfg := testConfig()
	cfg.VerifyTimeout = time.Minute
	q := newQueue(t, cfg)
	enqueuePair(t, q, "v")
	hb(t, q, "w1")
	hb(t, q, "w2")
	a, b := mustClaim(t, q, "w1"), mustClaim(t, q, "w2")
	finishWith(t, q, a, "d")
	// the sibling's own failure (non-zero exit) is terminal
	if d, err := q.Complete(context.Background(), b.JobID, b.Token, false, Result{JobID: b.JobID}); err != nil || d != DispOK {
		t.Fatal(d, err)
	}
	if j := state(t, q, a.JobID); j.State != StateFailed || j.Verification != "unavailable" {
		t.Fatalf("the waiting replica must not be left in VERIFYING or reported as success: %+v", j)
	}
}

func TestCancellingAVerifiedJobCancelsBothReplicasAndPairEnqueueIsIdempotent(t *testing.T) {
	q := newQueue(t, testConfig())
	enqueuePair(t, q, "v")
	seq1 := state(t, q, "v").Seq
	if seq, created, _ := q.EnqueuePair(context.Background(), Spec{ID: "v", ContainerImage: "img"}, 0); created || seq != seq1 {
		t.Fatalf("re-enqueue created=%v seq %d vs %d", created, seq, seq1)
	}
	if n, _ := q.QueueDepth(context.Background()); n != 2 {
		t.Fatalf("a pair is two queue entries, got %d", n)
	}
	if d, _ := q.Cancel(context.Background(), "v"); d != DispOK {
		t.Fatal(d)
	}
	for _, id := range []string{"v", ShadowID("v")} {
		if st := state(t, q, id).State; st != StateCanceled {
			t.Fatalf("%s is %s after cancelling the verified job", id, st)
		}
	}
	if n, _ := q.QueueDepth(context.Background()); n != 0 {
		t.Fatalf("cancelled replicas still queued: %d", n)
	}
}
