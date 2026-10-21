package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"coordinator/internal/testutil"

	"github.com/redis/go-redis/v9"
)

func testConfig() Config {
	return Config{
		MaxAttempts:    3,
		BackoffBase:    10 * time.Millisecond,
		BackoffMax:     50 * time.Millisecond,
		DeadAfter:      700 * time.Millisecond,
		ForgetAfter:    time.Hour,
		LeaseGrace:     100 * time.Millisecond,
		DefaultTimeout: time.Minute,
		ClaimScan:      100,
		ReapInterval:   50 * time.Millisecond,
		OrphanGrace:    time.Second,
	}
}

func newQueue(t *testing.T, cfg Config) *Queue {
	t.Helper()
	r := testutil.Start(t)
	c := r.Client()
	t.Cleanup(func() { c.Close() })
	return New(c, cfg)
}

func hb(t *testing.T, q *Queue, id string, mutate ...func(*WorkerReport)) []string {
	t.Helper()
	r := WorkerReport{ID: id, CPU: 8, MemMB: 16384, GPU: 0, MaxJobs: 1}
	for _, m := range mutate {
		m(&r)
	}
	cancel, err := q.Heartbeat(context.Background(), r)
	if err != nil {
		t.Fatalf("heartbeat %s: %v", id, err)
	}
	return cancel
}

func enqueue(t *testing.T, q *Queue, id string, mutate ...func(*Spec)) {
	t.Helper()
	s := Spec{ID: id, ContainerImage: "img", CreatedAtUnix: time.Now().Unix()}
	for _, m := range mutate {
		m(&s)
	}
	if _, created, err := q.Enqueue(context.Background(), s, 0); err != nil || !created {
		t.Fatalf("enqueue %s: created=%v err=%v", id, created, err)
	}
}

func mustClaim(t *testing.T, q *Queue, worker string) *Claimed {
	t.Helper()
	c, reason, err := q.Claim(context.Background(), worker)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if c == nil {
		t.Fatalf("claim by %s returned nothing (reason %q)", worker, reason)
	}
	return c
}

func state(t *testing.T, q *Queue, id string) *Job {
	t.Helper()
	j, err := q.Get(context.Background(), id)
	if err != nil || j == nil {
		t.Fatalf("get %s: %v %v", id, j, err)
	}
	return j
}

func TestFIFOOrder(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		enqueue(t, q, fmt.Sprintf("job-%d", i))
	}
	hb(t, q, "w1")
	for i := 0; i < 5; i++ {
		c := mustClaim(t, q, "w1")
		if want := fmt.Sprintf("job-%d", i); c.JobID != want {
			t.Fatalf("claim %d got %s, want %s", i, c.JobID, want)
		}
		if d, err := q.Complete(ctx, c.JobID, c.Token, true, Result{JobID: c.JobID}); err != nil || d != DispOK {
			t.Fatalf("complete: %v %v", d, err)
		}
	}
	if c, reason, _ := q.Claim(ctx, "w1"); c != nil || reason != "empty" {
		t.Fatalf("expected empty queue, got %v %q", c, reason)
	}
}

func TestEnqueueIsIdempotent(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	enqueue(t, q, "dup")
	seq1 := state(t, q, "dup").Seq
	seq2, created, err := q.Enqueue(ctx, Spec{ID: "dup", ContainerImage: "other"}, 0)
	if err != nil || created || seq2 != seq1 {
		t.Fatalf("re-enqueue: seq %d->%d created=%v err=%v", seq1, seq2, created, err)
	}
	if n, _ := q.QueueDepth(ctx); n != 1 {
		t.Fatalf("queue depth %d, want 1", n)
	}
}

// The headline guarantee: under heavy concurrency every job is assigned exactly once.
func TestConcurrentClaimNeverDoubleAssigns(t *testing.T) {
	cfg := testConfig()
	cfg.DeadAfter = time.Minute // no reaping noise in this test
	q := newQueue(t, cfg)
	ctx := context.Background()

	const jobs, workers = 3000, 48
	for i := 0; i < jobs; i++ {
		enqueue(t, q, fmt.Sprintf("j%05d", i))
	}
	for w := 0; w < workers; w++ {
		hb(t, q, fmt.Sprintf("w%d", w), func(r *WorkerReport) { r.MaxJobs = jobs })
	}

	var (
		mu       sync.Mutex
		claimed  = make(map[string]string, jobs) // jobID -> worker
		tokens   = make(map[string]bool, jobs)
		doubles  atomic.Int64
		total    atomic.Int64
		wg       sync.WaitGroup
		startGun = make(chan struct{})
	)
	for w := 0; w < workers; w++ {
		wid := fmt.Sprintf("w%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGun
			for {
				c, reason, err := q.Claim(ctx, wid)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if c == nil {
					if reason != "empty" {
						t.Errorf("unexpected rejection %q", reason)
					}
					return
				}
				total.Add(1)
				mu.Lock()
				if prev, dup := claimed[c.JobID]; dup {
					doubles.Add(1)
					t.Errorf("job %s assigned to both %s and %s", c.JobID, prev, wid)
				}
				claimed[c.JobID] = wid
				if tokens[c.Token] {
					t.Errorf("lease token reused: %s", c.Token)
				}
				tokens[c.Token] = true
				mu.Unlock()
			}
		}()
	}
	close(startGun)
	wg.Wait()

	if doubles.Load() != 0 {
		t.Fatalf("%d double assignments", doubles.Load())
	}
	if total.Load() != jobs || len(claimed) != jobs {
		t.Fatalf("claims=%d unique jobs=%d, want %d each", total.Load(), len(claimed), jobs)
	}
	counts, _ := q.Counts(ctx)
	if counts[StateAssigned] != jobs || counts[StatePending] != 0 {
		t.Fatalf("counts after run: %v", counts)
	}
	t.Logf("%d jobs claimed by %d concurrent workers: 0 double-assignments", jobs, workers)
}

func TestRequeueKeepsFIFOPositionAndCountsAttempt(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	for _, id := range []string{"A", "B", "C"} {
		enqueue(t, q, id)
	}
	hb(t, q, "dying")
	a := mustClaim(t, q, "dying")
	if a.JobID != "A" || a.Attempt != 1 {
		t.Fatalf("first claim %+v", a)
	}

	// "dying" stops heartbeating; a healthy worker keeps going.
	time.Sleep(testConfig().DeadAfter + 100*time.Millisecond)
	jobs, workers, err := q.ReapDeadWorkers(ctx)
	if err != nil || jobs != 1 || workers != 1 {
		t.Fatalf("reap: jobs=%d workers=%d err=%v", jobs, workers, err)
	}
	if j := state(t, q, "A"); j.State != StateRequeued || j.LastFailure != "worker_dead" {
		t.Fatalf("A after reap: %+v", j)
	}

	time.Sleep(80 * time.Millisecond) // backoff
	hb(t, q, "healthy")
	got := mustClaim(t, q, "healthy")
	if got.JobID != "A" {
		t.Fatalf("requeued job lost its place: healthy worker got %s, want A", got.JobID)
	}
	if got.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", got.Attempt)
	}
	if got.Token == a.Token {
		t.Fatal("new attempt must have a new lease token")
	}
}

func TestLateResultFromDeadWorkerIsDiscarded(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	enqueue(t, q, "A")
	hb(t, q, "zombie")
	old := mustClaim(t, q, "zombie")

	time.Sleep(testConfig().DeadAfter + 100*time.Millisecond)
	if _, _, err := q.ReapDeadWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	hb(t, q, "good")
	cur := mustClaim(t, q, "good")

	// The partitioned worker finishes and reports late.
	if d, err := q.Complete(ctx, "A", old.Token, true, Result{JobID: "A", WorkerID: "zombie", StdoutExcerpt: "zombie"}); err != nil || d != DispStale {
		t.Fatalf("late result: %v %v, want stale", d, err)
	}
	if st := state(t, q, "A").State; st != StateAssigned {
		t.Fatalf("late result changed state to %s", st)
	}
	if d, err := q.MarkRunning(ctx, "A", old.Token); err != nil || d != DispStale {
		t.Fatalf("late mark-running: %v %v", d, err)
	}

	if d, _ := q.Complete(ctx, "A", cur.Token, true, Result{JobID: "A", WorkerID: "good", StdoutExcerpt: "good"}); d != DispOK {
		t.Fatalf("legit completion: %v", d)
	}
	// Duplicates and even a stale straggler after completion change nothing.
	if d, _ := q.Complete(ctx, "A", cur.Token, true, Result{JobID: "A", StdoutExcerpt: "again"}); d != DispDuplicate {
		t.Fatalf("duplicate completion: %v", d)
	}
	if d, _ := q.Complete(ctx, "A", old.Token, false, Result{JobID: "A", StdoutExcerpt: "late"}); d != DispDuplicate {
		t.Fatalf("straggler after completion: %v", d)
	}
	res, _ := q.GetResult(ctx, "A")
	if res == nil || res.WorkerID != "good" || res.StdoutExcerpt != "good" {
		t.Fatalf("committed result corrupted: %+v", res)
	}
	if st := state(t, q, "A").State; st != StateSucceeded {
		t.Fatalf("final state %s", st)
	}
}

func TestMaxAttemptsParksJobAsDeadAndReviveRestoresIt(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = 2
	q := newQueue(t, cfg)
	ctx := context.Background()
	enqueue(t, q, "poison")
	hb(t, q, "w")

	for attempt := 1; attempt <= 2; attempt++ {
		time.Sleep(80 * time.Millisecond) // let backoff elapse
		c := mustClaim(t, q, "w")
		if c.Attempt != attempt {
			t.Fatalf("attempt %d, want %d", c.Attempt, attempt)
		}
		d, newState, err := q.Fail(ctx, "poison", c.Token, ReasonInfra, true)
		if err != nil || d != DispOK {
			t.Fatalf("fail: %v %v", d, err)
		}
		want := StateRequeued
		if attempt == 2 {
			want = StateDead
		}
		if newState != want {
			t.Fatalf("after attempt %d state = %s, want %s", attempt, newState, want)
		}
	}
	dead, _ := q.DeadJobs(ctx)
	if len(dead) != 1 || dead[0] != "poison" {
		t.Fatalf("dead set %v", dead)
	}
	if c, _, _ := q.Claim(ctx, "w"); c != nil {
		t.Fatal("dead job must not be claimable")
	}
	if ok, err := q.Revive(ctx, "poison"); err != nil || !ok {
		t.Fatalf("revive: %v %v", ok, err)
	}
	if c := mustClaim(t, q, "w"); c.Attempt != 1 {
		t.Fatalf("revived job attempt %d, want 1", c.Attempt)
	}
}

func TestEvictionDoesNotBurnAnAttempt(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttempts = 1
	q := newQueue(t, cfg)
	ctx := context.Background()
	enqueue(t, q, "j")
	hb(t, q, "w")
	for i := 0; i < 5; i++ {
		c := mustClaim(t, q, "w")
		if c.Attempt != 1 {
			t.Fatalf("eviction %d: attempt = %d, want 1 (evictions are not the job's fault)", i, c.Attempt)
		}
		if _, st, err := q.Fail(ctx, "j", c.Token, ReasonEvicted, false); err != nil || st != StateRequeued {
			t.Fatalf("evict: %s %v", st, err)
		}
	}
	if st := state(t, q, "j").State; st != StateRequeued {
		t.Fatalf("state %s", st)
	}
}

func TestCancel(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	enqueue(t, q, "queued")
	enqueue(t, q, "running")
	hb(t, q, "w")
	// claim order is FIFO, so claim "queued" then cancel the one still pending.
	first := mustClaim(t, q, "w")
	if first.JobID != "queued" {
		t.Fatalf("got %s", first.JobID)
	}
	if d, _ := q.Cancel(ctx, "running"); d != DispOK {
		t.Fatalf("cancel pending: %v", d)
	}
	if n, _ := q.QueueDepth(ctx); n != 0 {
		t.Fatalf("cancelled job still queued (depth %d)", n)
	}
	// Cancel an assigned job: the worker is told on its next heartbeat.
	if d, _ := q.Cancel(ctx, "queued"); d != DispOK {
		t.Fatalf("cancel assigned: %v", d)
	}
	cancel := hb(t, q, "w", func(r *WorkerReport) { r.Running = []string{"queued"} })
	if len(cancel) != 1 || cancel[0] != "queued" {
		t.Fatalf("worker was not told to abort: %v", cancel)
	}
	if d, _ := q.Complete(ctx, "queued", first.Token, true, Result{}); d != DispDuplicate {
		t.Fatalf("result for cancelled job: %v", d)
	}
	if d, _ := q.Cancel(ctx, "queued"); d != DispDuplicate {
		t.Fatalf("double cancel: %v", d)
	}
	if d, _ := q.Cancel(ctx, "nope"); d != DispUnknown {
		t.Fatalf("cancel unknown: %v", d)
	}
}

func TestWorkerEligibility(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	enqueue(t, q, "needs-gpu", func(s *Spec) { s.GPUCount = 1 })

	if c, reason, _ := q.Claim(ctx, "ghost"); c != nil || reason != "unknown_worker" {
		t.Fatalf("unregistered worker: %v %q", c, reason)
	}

	hb(t, q, "cpu-only")
	if c, reason, _ := q.Claim(ctx, "cpu-only"); c != nil || reason != "empty" {
		t.Fatalf("GPU job must not go to a CPU-only worker: %v %q", c, reason)
	}

	hb(t, q, "busy-box", func(r *WorkerReport) { r.GPU = 1; r.Overloaded = true })
	if c, reason, _ := q.Claim(ctx, "busy-box"); c != nil || reason != "overloaded" {
		t.Fatalf("overloaded worker must get nothing: %v %q", c, reason)
	}
	hb(t, q, "busy-box", func(r *WorkerReport) { r.GPU = 1; r.Draining = true })
	if c, reason, _ := q.Claim(ctx, "busy-box"); c != nil || reason != "draining" {
		t.Fatalf("draining worker must get nothing: %v %q", c, reason)
	}

	hb(t, q, "gpu-ok", func(r *WorkerReport) { r.GPU = 1 })
	c := mustClaim(t, q, "gpu-ok")
	if c.JobID != "needs-gpu" {
		t.Fatalf("got %s", c.JobID)
	}
	enqueue(t, q, "second")
	if c, reason, _ := q.Claim(ctx, "gpu-ok"); c != nil || reason != "at_capacity" {
		t.Fatalf("worker over max_jobs: %v %q", c, reason)
	}

	time.Sleep(testConfig().DeadAfter + 100*time.Millisecond)
	if c, reason, _ := q.Claim(ctx, "cpu-only"); c != nil || reason != "stale_heartbeat" {
		t.Fatalf("stale worker must get nothing: %v %q", c, reason)
	}
}

func TestLeaseExpiryRequeuesHungJob(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseGrace = 50 * time.Millisecond
	q := newQueue(t, cfg)
	ctx := context.Background()
	enqueue(t, q, "hung", func(s *Spec) { s.MaxRuntimeSeconds = 1 })
	hb(t, q, "w")
	c := mustClaim(t, q, "w")

	if n, _ := q.ReapExpiredLeases(ctx); n != 0 {
		t.Fatalf("reaped %d before the deadline", n)
	}
	time.Sleep(1100 * time.Millisecond)
	hb(t, q, "w", func(r *WorkerReport) { r.Running = []string{"hung"} }) // alive, still "running" the job, never finishing
	if n, err := q.ReapExpiredLeases(ctx); err != nil || n != 1 {
		t.Fatalf("reap expired: %d %v", n, err)
	}
	j := state(t, q, "hung")
	if j.State != StateRequeued || j.LastFailure != "lease_expired" {
		t.Fatalf("after expiry: %+v", j)
	}
	if d, _ := q.Complete(ctx, "hung", c.Token, true, Result{}); d != DispStale {
		t.Fatalf("hung worker's late result: %v", d)
	}
}

func TestHeartbeatTellsWorkerToAbortJobsItNoLongerOwns(t *testing.T) {
	q := newQueue(t, testConfig())
	ctx := context.Background()
	enqueue(t, q, "A")
	hb(t, q, "w1")
	c := mustClaim(t, q, "w1")

	if cancel := hb(t, q, "w1", func(r *WorkerReport) { r.Running = []string{"A"} }); len(cancel) != 0 {
		t.Fatalf("owner told to abort: %v", cancel)
	}
	// Job is taken away (failed attempt requeued), then the worker reports it again.
	if _, _, err := q.Fail(ctx, "A", c.Token, ReasonInfra, true); err != nil {
		t.Fatal(err)
	}
	cancel := hb(t, q, "w1", func(r *WorkerReport) { r.Running = []string{"A"} })
	if len(cancel) != 1 || cancel[0] != "A" {
		t.Fatalf("zombie not told to abort: %v", cancel)
	}
}

// The reply to a claim can be lost (coordinator or Redis dies after the atomic claim, before the
// worker hears about it). The job is then leased to a live worker that does not know it has it;
// heartbeat reconciliation must give it back, without burning an attempt.
func TestLostClaimReplyIsReconciledByHeartbeat(t *testing.T) {
	cfg := testConfig()
	cfg.OrphanGrace = 300 * time.Millisecond
	cfg.DeadAfter = time.Minute // the worker stays alive: only reconciliation can save the job
	q := newQueue(t, cfg)
	enqueue(t, q, "orphan")
	hb(t, q, "w")
	c := mustClaim(t, q, "w") // ...and the worker never receives this reply
	if c.Attempt != 1 {
		t.Fatalf("attempt %d", c.Attempt)
	}

	// Inside the grace period the lease might just be in flight: leave it alone.
	hb(t, q, "w")
	if st := state(t, q, "orphan").State; st != StateAssigned {
		t.Fatalf("a fresh unreported lease was taken back after %s (state %s)", "<grace", st)
	}

	time.Sleep(400 * time.Millisecond)
	hb(t, q, "w") // reports nothing running
	j := state(t, q, "orphan")
	if j.State != StateRequeued || j.LastFailure != "assignment_lost" {
		t.Fatalf("orphaned lease not reconciled: %+v", j)
	}
	time.Sleep(50 * time.Millisecond)
	again := mustClaim(t, q, "w")
	if again.Attempt != 1 {
		t.Fatalf("a claim that never reached the worker burned an attempt (attempt %d)", again.Attempt)
	}

	// A job the worker does report is never taken back, however old.
	time.Sleep(400 * time.Millisecond)
	hb(t, q, "w", func(r *WorkerReport) { r.Running = []string{"orphan"} })
	if st := state(t, q, "orphan").State; st != StateAssigned {
		t.Fatalf("reported job was taken back (state %s)", st)
	}
}

// Failure detection and completion racing each other must never leave a job in two states.
func TestCompleteVersusReapRace(t *testing.T) {
	cfg := testConfig()
	cfg.DeadAfter = time.Minute // setup must never go stale, however slow the machine: we age the worker explicitly below
	cfg.BackoffBase = time.Millisecond
	cfg.BackoffMax = time.Millisecond
	q := newQueue(t, cfg)
	ctx := context.Background()

	const n = 150
	for i := 0; i < n; i++ {
		enqueue(t, q, fmt.Sprintf("r%03d", i))
	}
	var held []string
	claims := make([]*Claimed, 0, n)
	for i := 0; i < n; i++ {
		hb(t, q, "slow", func(r *WorkerReport) { r.MaxJobs = n; r.Running = held }) // stay alive, reporting what it holds
		c := mustClaim(t, q, "slow")
		claims = append(claims, c)
		held = append(held, c.JobID)
	}
	// "slow" goes silent: backdate its last heartbeat instead of sleeping, so the test does not depend on wall-clock timing.
	if err := q.Redis().ZAdd(ctx, "ch:workers", redis.Z{Score: float64(time.Now().Add(-10 * time.Minute).UnixMilli()), Member: "slow"}).Err(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var ok, stale, dup atomic.Int64
	wg.Add(2)
	go func() { // the dead worker wakes up and reports everything at once
		defer wg.Done()
		for _, c := range claims {
			d, err := q.Complete(ctx, c.JobID, c.Token, true, Result{JobID: c.JobID, WorkerID: "slow"})
			if err != nil {
				t.Error(err)
				return
			}
			switch d {
			case DispOK:
				ok.Add(1)
			case DispStale:
				stale.Add(1)
			case DispDuplicate:
				dup.Add(1)
			}
		}
	}()
	go func() { // while the reaper declares it dead, part-way through that stream
		defer wg.Done()
		time.Sleep(15 * time.Millisecond)
		if _, _, err := q.ReapDeadWorkers(ctx); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()

	counts, _ := q.Counts(ctx)
	var sum int64
	for _, c := range counts {
		sum += c
	}
	if sum != n {
		t.Fatalf("state counts sum to %d, want %d: %v", sum, n, counts)
	}
	if counts[StateSucceeded] != ok.Load() {
		t.Fatalf("%d accepted completions but %d jobs SUCCEEDED", ok.Load(), counts[StateSucceeded])
	}
	if counts[StateSucceeded]+counts[StateRequeued] != n {
		t.Fatalf("every job must be either committed or requeued: %v", counts)
	}
	t.Logf("race outcome: %d committed before the reap, %d discarded as stale, %d requeued", ok.Load(), stale.Load(), counts[StateRequeued])
}
