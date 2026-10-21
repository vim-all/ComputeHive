package chaos

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"coordinator/internal/queue"
	"coordinator/internal/testutil"
	pb "coordinator/pkg/pb"
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func observer(t testing.TB, r *testutil.Redis, p profile) *queue.Queue {
	t.Helper()
	c := r.Client()
	t.Cleanup(func() { c.Close() })
	return queue.New(c, p.queueConfig())
}

// jitter gives each job a deterministic run time in [lo, hi).
func jitter(lo, hi time.Duration) func(string) time.Duration {
	return func(id string) time.Duration {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		return lo + time.Duration(h.Sum32())%(hi-lo)
	}
}

func fixed(d time.Duration) func(string) time.Duration {
	return func(string) time.Duration { return d }
}

// requireClean asserts no job is left half-assigned anywhere in Redis.
func requireClean(t *testing.T, r *testutil.Redis, q *queue.Queue, total int64) {
	t.Helper()
	ctx := context.Background()
	rdb := q.Redis()
	if n, _ := rdb.ZCard(ctx, "ch:pending").Result(); n != 0 {
		t.Errorf("%d jobs still in the pending set", n)
	}
	if n, _ := rdb.ZCard(ctx, "ch:leases").Result(); n != 0 {
		t.Errorf("%d leases still outstanding", n)
	}
	keys, _ := rdb.Keys(ctx, "ch:wrun:*").Result()
	for _, k := range keys {
		if n, _ := rdb.SCard(ctx, k).Result(); n != 0 {
			t.Errorf("%s still holds %d jobs", k, n)
		}
	}
	counts, err := q.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, c := range counts {
		sum += c
	}
	if sum != total {
		t.Errorf("state counts sum to %d, want %d: %v", sum, total, counts)
	}
}

// Worker killed mid-job -> job requeued in < 15s -> completes on another worker, exactly once.
func TestWorkerKilledMidJobIsRequeuedUnder15sAndCompletesElsewhere(t *testing.T) {
	r := testutil.Start(t)
	c := startCoordinator(t, r, freeAddr(t), specProfile)
	client := clientFor(t, c.addr)
	q := observer(t, r, specProfile)
	led := newLedger()
	ctx := context.Background()

	iterations := envInt("CHAOS_ITER", 3)
	var requeueSecs, reassignSecs, completeSecs []float64
	for i := 0; i < iterations; i++ {
		victimID, survivorID := fmt.Sprintf("victim-%d", i), fmt.Sprintf("survivor-%d", i)
		victim := startWorker(t, c.addr, victimID, specProfile, led, fixed(time.Minute)) // would run "forever"
		waitFor(t, 10*time.Second, func() bool { ok, _ := q.WorkerKnown(ctx, victimID); return ok }, func() string { return "victim never registered" })

		jobID := submit(t, client, 1, nil)[0]
		waitFor(t, 10*time.Second, func() bool { return victim.Current() == jobID }, func() string { return "victim never picked up the job" })

		survivor := startWorker(t, c.addr, survivorID, specProfile, led, fixed(300*time.Millisecond))
		time.Sleep(time.Duration(rand.Intn(int(specProfile.hb.Milliseconds()))) * time.Millisecond) // kill at a random phase of the heartbeat cycle
		killAt := time.Now()
		victim.Kill()

		var requeuedAt, reassignedAt time.Time
		waitFor(t, 40*time.Second, func() bool {
			j, err := q.Get(ctx, jobID)
			if err != nil || j == nil {
				return false
			}
			if requeuedAt.IsZero() && j.LastFailureMs > 0 {
				requeuedAt = time.UnixMilli(j.LastFailureMs)
			}
			if reassignedAt.IsZero() && j.Attempt == 2 && j.Worker == survivorID {
				reassignedAt = time.Now()
			}
			return j.State == queue.StateSucceeded
		}, func() string { j, _ := q.Get(ctx, jobID); return fmt.Sprintf("job never completed: %+v", j) })
		doneAt := time.Now()

		j, _ := q.Get(ctx, jobID)
		if j.LastFailure != "worker_dead" || j.Attempt != 2 {
			t.Fatalf("iteration %d: job %+v, want requeued once for worker_dead", i, j)
		}
		if j.Commits != 1 {
			t.Fatalf("iteration %d: job committed %d times, want exactly 1", i, j.Commits)
		}
		res, _ := q.GetResult(ctx, jobID)
		if res == nil || res.WorkerID != survivorID {
			t.Fatalf("iteration %d: committed result %+v", i, res)
		}

		rq := requeuedAt.Sub(killAt).Seconds()
		if rq >= 15 {
			t.Fatalf("iteration %d: time-to-requeue %.2fs, want < 15s", i, rq)
		}
		if reassignedAt.IsZero() {
			reassignedAt = doneAt
		}
		requeueSecs = append(requeueSecs, rq)
		reassignSecs = append(reassignSecs, reassignedAt.Sub(killAt).Seconds())
		completeSecs = append(completeSecs, doneAt.Sub(killAt).Seconds())
		t.Logf("iteration %d: kill -> requeued %.2fs, -> reassigned %.2fs, -> completed %.2fs (limit 15s)", i, rq, reassignSecs[i], completeSecs[i])
		survivor.Kill()
	}
	sort.Float64s(requeueSecs)
	report("requeue_seconds_max", requeueSecs[len(requeueSecs)-1])
	report("requeue_seconds_p50", requeueSecs[len(requeueSecs)/2])
	for _, v := range completeSecs {
		report("kill_to_complete_seconds", v)
	}
	for _, v := range requeueSecs {
		report("kill_to_requeue_seconds", v)
	}
	requireClean(t, r, q, int64(iterations))
}

// Coordinator crash + restart mid-run: nothing lost, nothing committed twice.
func TestCoordinatorCrashAndRestartLosesNothing(t *testing.T) {
	r := testutil.Start(t)
	addr := freeAddr(t)
	c := startCoordinator(t, r, addr, fastProfile)
	client := clientFor(t, addr)
	q := observer(t, r, fastProfile)
	led := newLedger()

	const total = 300
	ids := submit(t, client, total, nil)
	var workers []*fakeWorker
	for i := 0; i < 4; i++ {
		workers = append(workers, startWorker(t, addr, fmt.Sprintf("w%d", i), fastProfile, led, jitter(20*time.Millisecond, 60*time.Millisecond)))
	}
	committed := func() int { return int(succeeded(q)) }
	waitFor(t, 30*time.Second, func() bool { return committed() >= 80 }, func() string { return "workers made no progress" })
	at := committed()

	c.Kill() // coordinator dies mid-run
	time.Sleep(1500 * time.Millisecond)
	c2 := startCoordinator(t, r, addr, fastProfile) // a fresh process recovers purely from Redis
	_ = c2

	waitFor(t, 90*time.Second, func() bool { return committed() == total }, func() string {
		counts, _ := q.Counts(context.Background())
		return fmt.Sprintf("committed %d/%d, counts %v", committed(), total, counts)
	})
	auditCommits(t, q, ids)
	counts, _ := q.Counts(context.Background())
	if counts[queue.StateSucceeded] != total {
		t.Fatalf("counts after recovery: %v", counts)
	}
	requireClean(t, r, q, total)
	t.Logf("coordinator killed after %d/%d commits, down 1.5s, restarted: %d/%d jobs SUCCEEDED, each committed exactly once", at, total, counts[queue.StateSucceeded], total)
	_ = workers
}

// Redis crash (SIGKILL) + restart with AOF: acknowledged state survives.
func TestRedisRestartKeepsState(t *testing.T) {
	r := testutil.Start(t)
	addr := freeAddr(t)
	startCoordinator(t, r, addr, fastProfile)
	client := clientFor(t, addr)
	q := observer(t, r, fastProfile)
	led := newLedger()
	ctx := context.Background()

	const total = 60
	ids := submit(t, client, total, nil)
	startWorker(t, addr, "w0", fastProfile, led, fixed(30*time.Millisecond))
	startWorker(t, addr, "w1", fastProfile, led, fixed(30*time.Millisecond))

	waitFor(t, 30*time.Second, func() bool {
		c, _ := q.Counts(ctx)
		return c[queue.StateSucceeded] >= 15
	}, func() string { return "no progress before restart" })

	// Snapshot everything already acknowledged as done.
	before := map[string]string{}
	for _, id := range ids {
		if j, _ := q.Get(ctx, id); j != nil && j.State == queue.StateSucceeded {
			res, _ := q.GetResult(ctx, id)
			if res == nil {
				t.Fatalf("job %s SUCCEEDED without a result", id)
			}
			before[id] = res.WorkerID
		}
	}

	restartStart := time.Now()
	r.KillAndRestart(t)
	t.Logf("redis kill+restart took %s", time.Since(restartStart).Round(time.Millisecond))

	for id, worker := range before {
		j, err := q.Get(ctx, id)
		if err != nil || j == nil || j.State != queue.StateSucceeded {
			t.Fatalf("job %s lost its SUCCEEDED state across the restart: %+v %v", id, j, err)
		}
		if res, _ := q.GetResult(ctx, id); res == nil || res.WorkerID != worker {
			t.Fatalf("job %s result changed across the restart: %+v", id, res)
		}
	}
	for _, id := range ids {
		if j, _ := q.Get(ctx, id); j == nil {
			t.Fatalf("job %s disappeared across the restart", id)
		}
	}

	resumeStart := time.Now()
	waitFor(t, 90*time.Second, func() bool {
		c, _ := q.Counts(ctx)
		return c[queue.StateSucceeded] == total
	}, func() string { c, _ := q.Counts(ctx); return fmt.Sprintf("counts after restart: %v", c) })
	t.Logf("remaining jobs finished %s after the restart", time.Since(resumeStart).Round(time.Millisecond))
	auditCommits(t, q, ids)
	requireClean(t, r, q, total)
	t.Logf("redis SIGKILLed with %d jobs already done; all %d jobs present, %d results intact, all finished exactly once", len(before), total, len(before))
}

// A partitioned worker keeps running but cannot heartbeat: its result must never be committed over the live attempt.
func TestPartitionedWorkerNeverCommitsOverRequeuedJob(t *testing.T) {
	t.Run("finishes after the job completed elsewhere", func(t *testing.T) {
		r := testutil.Start(t)
		c := startCoordinator(t, r, freeAddr(t), fastProfile)
		client := clientFor(t, c.addr)
		q := observer(t, r, fastProfile)
		led := newLedger()
		ctx := context.Background()

		zombie := startWorker(t, c.addr, "zombie", fastProfile, led, fixed(3*time.Second))
		waitFor(t, 5*time.Second, func() bool { ok, _ := q.WorkerKnown(ctx, "zombie"); return ok }, func() string { return "no register" })
		jobID := submit(t, client, 1, nil)[0]
		waitFor(t, 5*time.Second, func() bool { return zombie.Current() == jobID }, func() string { return "zombie never took the job" })

		zombie.partition.Store(true) // network partition: it keeps working but is silent
		startWorker(t, c.addr, "healthy", fastProfile, led, fixed(100*time.Millisecond))

		waitFor(t, 20*time.Second, func() bool { j, _ := q.Get(ctx, jobID); return j != nil && j.State == queue.StateSucceeded }, func() string { return "healthy worker never finished" })
		// Wait for the zombie to wake up and report late.
		waitFor(t, 10*time.Second, func() bool { return led.dup.Load()+led.stale.Load() >= 1 }, func() string { return "zombie never reported" })

		if j, _ := q.Get(ctx, jobID); j.Commits != 1 {
			t.Fatalf("job committed %d times, want exactly 1", j.Commits)
		}
		res, _ := q.GetResult(ctx, jobID)
		if res == nil || res.WorkerID != "healthy" || res.StdoutExcerpt != "by healthy" {
			t.Fatalf("committed result was overwritten: %+v", res)
		}
	})

	t.Run("finishes while the job is waiting to be reassigned", func(t *testing.T) {
		r := testutil.Start(t)
		c := startCoordinator(t, r, freeAddr(t), fastProfile)
		client := clientFor(t, c.addr)
		q := observer(t, r, fastProfile)
		led := newLedger()
		ctx := context.Background()

		zombie := startWorker(t, c.addr, "zombie", fastProfile, led, fixed(1800*time.Millisecond))
		waitFor(t, 5*time.Second, func() bool { ok, _ := q.WorkerKnown(ctx, "zombie"); return ok }, func() string { return "no register" })
		jobID := submit(t, client, 1, nil)[0]
		waitFor(t, 5*time.Second, func() bool { return zombie.Current() == jobID }, func() string { return "zombie never took the job" })
		zombie.partition.Store(true)

		// The job is requeued (worker declared dead) and sits unassigned: nobody else exists yet.
		waitFor(t, 10*time.Second, func() bool { j, _ := q.Get(ctx, jobID); return j != nil && j.State == queue.StateRequeued }, func() string { return "job never requeued" })
		waitFor(t, 10*time.Second, func() bool { return led.stale.Load() >= 1 }, func() string { return "zombie's late result was not rejected as stale" })
		if j, _ := q.Get(ctx, jobID); j.State != queue.StateRequeued {
			t.Fatalf("a stale result changed the job state to %s", j.State)
		}
		if j, _ := q.Get(ctx, jobID); j.Commits != 0 {
			t.Fatalf("stale result was committed (%d commits)", j.Commits)
		}

		startWorker(t, c.addr, "healthy", fastProfile, led, fixed(50*time.Millisecond))
		waitFor(t, 20*time.Second, func() bool { j, _ := q.Get(ctx, jobID); return j != nil && j.State == queue.StateSucceeded }, func() string { return "never finished" })
		if j, _ := q.Get(ctx, jobID); j.Commits != 1 {
			t.Fatalf("job committed %d times, want exactly 1", j.Commits)
		}
		if res, _ := q.GetResult(ctx, jobID); res == nil || res.WorkerID != "healthy" {
			t.Fatalf("committed result %+v, want worker healthy", res)
		}
	})

	t.Run("partition heals and the coordinator tells the worker to abort", func(t *testing.T) {
		r := testutil.Start(t)
		c := startCoordinator(t, r, freeAddr(t), fastProfile)
		client := clientFor(t, c.addr)
		q := observer(t, r, fastProfile)
		led := newLedger()
		ctx := context.Background()

		zombie := startWorker(t, c.addr, "zombie", fastProfile, led, fixed(10*time.Second))
		waitFor(t, 5*time.Second, func() bool { ok, _ := q.WorkerKnown(ctx, "zombie"); return ok }, func() string { return "no register" })
		jobID := submit(t, client, 1, nil)[0]
		waitFor(t, 5*time.Second, func() bool { return zombie.Current() == jobID }, func() string { return "zombie never took the job" })
		zombie.partition.Store(true)
		waitFor(t, 10*time.Second, func() bool { j, _ := q.Get(ctx, jobID); return j != nil && j.State == queue.StateRequeued }, func() string { return "never requeued" })

		zombie.partition.Store(false) // network heals; the next heartbeat reports the job
		waitFor(t, 5*time.Second, func() bool { return zombie.aborted.Load() >= 1 }, func() string { return "zombie was not told to abort" })
		if j, _ := q.Get(ctx, jobID); j.Commits != 0 {
			t.Fatalf("aborted zombie committed (%d commits)", j.Commits)
		}
	})
}

// 1,000 jobs while workers are being SIGKILLed: every job reaches a terminal state exactly once.
func TestThousandJobsWithRandomWorkerKills(t *testing.T) {
	jobs := envInt("CHAOS_JOBS", 1000)
	r := testutil.Start(t)
	addr := freeAddr(t)
	c := startCoordinator(t, r, addr, fastProfile)
	_ = c
	client := clientFor(t, addr)
	q := observer(t, r, fastProfile)
	led := newLedger()
	ctx := context.Background()

	ids := submit(t, client, jobs, nil)

	const poolSize = 8
	var mu sync.Mutex
	pool := make([]*fakeWorker, 0, poolSize)
	gen := 0
	spawn := func() {
		w := startWorker(t, addr, fmt.Sprintf("w-%d", gen), fastProfile, led, jitter(20*time.Millisecond, 80*time.Millisecond))
		gen++
		pool = append(pool, w)
	}
	for i := 0; i < poolSize; i++ {
		spawn()
	}

	kills := 0
	stop := make(chan struct{})
	var killer sync.WaitGroup
	killer.Add(1)
	go func() {
		defer killer.Done()
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				mu.Lock()
				i := rand.Intn(len(pool))
				victim := pool[i]
				pool = append(pool[:i], pool[i+1:]...)
				victim.Kill() // SIGKILL
				kills++
				spawn()
				mu.Unlock()
			}
		}
	}()

	start := time.Now()
	waitFor(t, 5*time.Minute, func() bool {
		counts, _ := q.Counts(ctx)
		terminal := counts[queue.StateSucceeded] + counts[queue.StateFailed] + counts[queue.StateDead] + counts[queue.StateCanceled]
		return terminal == int64(jobs)
	}, func() string { c, _ := q.Counts(ctx); return fmt.Sprintf("counts: %v", c) })
	elapsed := time.Since(start)
	close(stop)
	killer.Wait()

	counts, _ := q.Counts(ctx)
	if counts[queue.StateSucceeded] != int64(jobs) {
		t.Fatalf("not every job succeeded: %v", counts)
	}
	auditCommits(t, q, ids)
	retried := 0
	for _, id := range ids {
		if j, _ := q.Get(ctx, id); j != nil && j.Attempt > 1 {
			retried++
		}
	}
	if kills < 5 || retried == 0 {
		t.Fatalf("test did not exercise failures (kills=%d, jobs retried=%d)", kills, retried)
	}
	requireClean(t, r, q, int64(jobs))
	report("zero_loss_jobs", float64(jobs))
	report("zero_loss_kills", float64(kills))
	report("zero_loss_lost", 0)
	report("zero_loss_retried_jobs", float64(retried))
	t.Logf("%d jobs, %d worker kills, %d jobs re-run after a kill, %d stale results discarded, %d duplicates ignored: %d SUCCEEDED, 0 lost, each committed exactly once (%.1fs)",
		jobs, kills, retried, led.stale.Load(), led.dup.Load(), counts[queue.StateSucceeded], elapsed.Seconds())
	_ = pb.Status_STATUS_QUEUED
}
