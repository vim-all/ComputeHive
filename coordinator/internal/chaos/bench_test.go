package chaos

// Benchmarks. They run only with BENCH=1 (`make bench`), because they take minutes and measure the machine as
// much as the code. Every line they print starts with "BENCH " and is machine-readable; scripts/bench.sh collects
// them. Nothing here is an estimate: each number comes from a run on the real coordinator (gRPC server, scheduler,
// Lua scripts) over a real Redis in Docker, with in-process fake workers that do no work (so the numbers measure the
// control plane, not Docker). Real container timings are measured separately by scripts/bench.sh on the live cluster.
//
// The coordinator is built with the PRODUCTION scheduling settings (250 ms snapshot cache, 5 s patience), unlike the
// chaos tests, which run with none, because the point is to measure what a user gets.

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"coordinator/internal/config"
	grpcserver "coordinator/internal/grpc"
	"coordinator/internal/queue"
	"coordinator/internal/testutil"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func benchOnly(t *testing.T) {
	t.Helper()
	if os.Getenv("BENCH") == "" {
		t.Skip("benchmarks run with BENCH=1 (make bench)")
	}
}

func benchLine(format string, a ...any) { fmt.Printf("BENCH "+format+"\n", a...) }

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p/100*float64(len(sorted))+0.5) - 1
	return sorted[max(0, min(len(sorted)-1, i))]
}

func envList(key string, def []int) []int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	var out []int
	for _, s := range strings.Split(raw, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

// startProdCoordinator is startCoordinator with the production scheduling settings (or the overrides in mod).
func startProdCoordinator(t testing.TB, r *testutil.Redis, p profile, mod func(*config.Config)) *coordinator {
	t.Helper()
	rdb := r.Client()
	q := queue.New(rdb, p.queueConfig())
	cfg := config.Config{HeartbeatInterval: p.hb, WorkerDeadAfter: p.deadAfter, ReapInterval: p.reap,
		SchedPatience: 5 * time.Second, SchedSnapshotTTL: 250 * time.Millisecond}
	// Experiments only: BENCH_SNAPSHOT_TTL_MS / BENCH_PATIENCE_MS override the production values.
	if v, err := strconv.Atoi(os.Getenv("BENCH_SNAPSHOT_TTL_MS")); err == nil {
		cfg.SchedSnapshotTTL = time.Duration(v) * time.Millisecond
	}
	if v, err := strconv.Atoi(os.Getenv("BENCH_PATIENCE_MS")); err == nil {
		cfg.SchedPatience = time.Duration(v) * time.Millisecond
	}
	if mod != nil {
		mod(&cfg)
	}
	svc := grpcserver.NewServer(q, cfg, quietLog())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &coordinator{addr: l.Addr().String(), cancel: cancel, done: make(chan struct{}), q: q}
	go q.RunReaper(ctx, quietLog(), nil)
	go func() {
		defer close(c.done)
		_ = grpcserver.Serve(ctx, l, svc, nil)
		_ = rdb.Close()
	}()
	t.Cleanup(c.Kill)
	return c
}

// TestBenchThroughput: how many jobs per second the control plane assigns and commits as the number of workers grows.
// Each worker pulls a job and reports it done immediately, so this measures the coordinator, scheduler and Redis.
func TestBenchThroughput(t *testing.T) {
	benchOnly(t)
	jobs := envInt("BENCH_JOBS", 3000)
	reps := envInt("BENCH_REPS", 3)
	r := testutil.Start(t)
	ctx := context.Background()
	for _, n := range envList("BENCH_WORKERS", []int{1, 2, 4, 8, 16, 32, 64}) {
		var rates []float64
		for rep := 0; rep < reps; rep++ {
			_ = r.Client().FlushAll(ctx).Err()
			c := startProdCoordinator(t, r, fastProfile, nil)
			client := clientFor(t, c.addr)
			q := observer(t, r, fastProfile)
			led := newLedger()

			workers := make([]*fakeWorker, 0, n)
			for i := 0; i < n; i++ {
				workers = append(workers, startWorker(t, c.addr, fmt.Sprintf("bw-%d-%d-%d", n, rep, i), fastProfile, led, fixed(0)))
			}
			waitFor(t, 30*time.Second, func() bool {
				for i := range workers {
					if ok, _ := q.WorkerKnown(ctx, fmt.Sprintf("bw-%d-%d-%d", n, rep, i)); !ok {
						return false
					}
				}
				return true
			}, func() string { return "workers never registered" })

			ids := submit(t, client, jobs, nil)
			_ = r.Client().ConfigResetStat(ctx).Err() // count only the commands spent assigning and committing
			start := time.Now()
			waitFor(t, 10*time.Minute, func() bool {
				counts, _ := q.Counts(ctx)
				return counts[queue.StateSucceeded] == int64(jobs)
			}, func() string { cc, _ := q.Counts(ctx); return fmt.Sprintf("counts: %v", cc) })
			elapsed := time.Since(start)
			auditCommits(t, q, ids) // a throughput number is worthless if jobs were lost or double-committed
			rate := float64(jobs) / elapsed.Seconds()
			rates = append(rates, rate)
			cmds, scripts := redisCommandCounts(ctx, r)
			benchLine("throughput workers=%d rep=%d jobs=%d seconds=%.3f jobs_per_second=%.1f redis_commands_per_job=%.1f redis_script_calls_per_job=%.2f",
				n, rep, jobs, elapsed.Seconds(), rate, float64(cmds)/float64(jobs), float64(scripts)/float64(jobs))
			for _, w := range workers {
				w.Kill()
			}
			c.Kill()
		}
		sort.Float64s(rates)
		benchLine("throughput_summary workers=%d jobs=%d reps=%d min=%.1f median=%.1f max=%.1f", n, jobs, reps, rates[0], rates[len(rates)/2], rates[len(rates)-1])
	}
}

// TestBenchAssignLatency: how long a submitted job waits for a worker when the cluster is idle, measured from the
// timestamps Redis itself recorded (created -> assigned), with the workers polling every 15 ms.
func TestBenchAssignLatency(t *testing.T) {
	benchOnly(t)
	jobs := envInt("BENCH_LAT_JOBS", 300)
	r := testutil.Start(t)
	ctx := context.Background()
	for _, n := range envList("BENCH_LAT_WORKERS", []int{1, 8, 32}) {
		_ = r.Client().FlushAll(ctx).Err()
		c := startProdCoordinator(t, r, fastProfile, nil)
		client := clientFor(t, c.addr)
		q := observer(t, r, fastProfile)
		led := newLedger()
		for i := 0; i < n; i++ {
			startWorker(t, c.addr, fmt.Sprintf("lw-%d-%d", n, i), fastProfile, led, fixed(0))
		}
		time.Sleep(time.Second) // registered and idle

		var ids []string
		for i := 0; i < jobs; i++ {
			ids = append(ids, submit(t, client, 1, nil)...)
			time.Sleep(25 * time.Millisecond) // an arrival every 25 ms: the cluster is never saturated
		}
		waitFor(t, 2*time.Minute, func() bool {
			counts, _ := q.Counts(ctx)
			return counts[queue.StateSucceeded] == int64(jobs)
		}, func() string { cc, _ := q.Counts(ctx); return fmt.Sprintf("counts: %v", cc) })

		var waits []float64
		for i, id := range ids {
			j, _ := q.Get(ctx, id)
			if j != nil && j.AssignedMs > 0 && j.CreatedMs > 0 {
				if j.AssignedMs-j.CreatedMs > 1000 {
					t.Logf("slow assignment: job #%d of %d waited %d ms", i, len(ids), j.AssignedMs-j.CreatedMs)
				}
				waits = append(waits, float64(j.AssignedMs-j.CreatedMs))
			}
		}
		sort.Float64s(waits)
		benchLine("assign_latency workers=%d jobs=%d p50_ms=%.0f p95_ms=%.0f p99_ms=%.0f max_ms=%.0f", n, len(waits),
			percentile(waits, 50), percentile(waits, 95), percentile(waits, 99), waits[len(waits)-1])
		c.Kill()
	}
}

// TestBenchRequestJobRPC: the cost of the coordinator answering a worker's RequestJob (snapshot, scoring, atomic
// claim), measured around the RPC from the worker's side, for calls that find nothing and calls that assign a job.
// The snapshot cache is off here: every call does the full cluster read, which is the worst case for decision time.
func TestBenchRequestJobRPC(t *testing.T) {
	benchOnly(t)
	calls := envInt("BENCH_RPC_CALLS", 1000)
	r := testutil.Start(t)
	ctx := context.Background()
	for _, n := range envList("BENCH_RPC_WORKERS", []int{1, 8, 64}) {
		_ = r.Client().FlushAll(ctx).Err()
		c := startProdCoordinator(t, r, fastProfile, func(cfg *config.Config) { cfg.SchedPatience = 0; cfg.SchedSnapshotTTL = 0 })
		client := clientFor(t, c.addr)
		conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		wc := pb.NewWorkerServiceClient(conn)
		register := func(id string) {
			if _, err := wc.RegisterWorker(ctx, &pb.RegisterWorkerRequest{WorkerId: id, MaxJobs: 1, AvailableResources: &pb.ResourceSpec{CpuCores: 8, MemoryMb: 16384}}); err != nil {
				t.Fatal(err)
			}
		}
		// n-1 idle workers make the scheduler evaluate a realistic pool; the probe is the nth.
		for i := 1; i < n; i++ {
			register(fmt.Sprintf("rw-%d-%d", n, i))
		}
		const probe = "rw-probe"
		register(probe)
		hbCtx, stopHB := context.WithCancel(ctx)
		defer stopHB()
		go func() { // a live worker heartbeats; without it the coordinator rightly treats the probe as unavailable
			for hbCtx.Err() == nil {
				_, _ = wc.Heartbeat(hbCtx, &pb.HeartbeatRequest{WorkerId: &pb.WorkerID{Value: probe}, MaxJobs: 1,
					AvailableResources: &pb.ResourceSpec{CpuCores: 8, MemoryMb: 16384}})
				time.Sleep(500 * time.Millisecond)
			}
		}()
		ask := func() (*pb.RequestJobResponse, float64) {
			t0 := time.Now()
			resp, err := wc.RequestJob(ctx, &pb.RequestJobRequest{WorkerId: &pb.WorkerID{Value: probe}})
			if err != nil {
				t.Fatal(err)
			}
			return resp, float64(time.Since(t0).Microseconds()) / 1000
		}

		var empty []float64
		for i := 0; i < calls; i++ {
			resp, d := ask()
			if resp.GetHasJob() {
				t.Fatal("the queue is empty: no job expected")
			}
			empty = append(empty, d)
		}
		sort.Float64s(empty)
		benchLine("requestjob_rpc_empty pool=%d calls=%d p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f", n, len(empty), percentile(empty, 50), percentile(empty, 95), percentile(empty, 99))

		submit(t, client, calls, nil)
		var assign []float64
		for i := 0; i < calls; i++ {
			resp, d := ask()
			if !resp.GetHasJob() {
				t.Fatalf("call %d: expected a job, got %v", i, resp)
			}
			assign = append(assign, d)
			if _, err := wc.SubmitResult(ctx, &pb.SubmitResultRequest{WorkerId: &pb.WorkerID{Value: probe}, LeaseToken: resp.GetJob().GetLeaseToken(),
				Result: &pb.Result{JobId: resp.GetJob().GetJobId(), Status: pb.Status_STATUS_SUCCEEDED}}); err != nil {
				t.Fatal(err)
			}
		}
		sort.Float64s(assign)
		benchLine("requestjob_rpc_assign pool=%d calls=%d p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f", n, len(assign), percentile(assign, 50), percentile(assign, 95), percentile(assign, 99))
		c.Kill()
	}
}

// redisCommandCounts returns how many commands (and how many of them were Lua script calls) Redis has executed since
// the last CONFIG RESETSTAT. Redis runs commands one at a time, so commands per job bounds the throughput it can give.
func redisCommandCounts(ctx context.Context, r *testutil.Redis) (total, scripts int64) {
	info, err := r.Client().Info(ctx, "commandstats").Result()
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(info, "\n") {
		name, rest, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || !strings.HasPrefix(name, "cmdstat_") {
			continue
		}
		for _, kv := range strings.Split(rest, ",") {
			if v, ok := strings.CutPrefix(kv, "calls="); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				if name == "cmdstat_info" || name == "cmdstat_config|resetstat" {
					break // the measurement itself
				}
				total += n
				if name == "cmdstat_evalsha" || name == "cmdstat_eval" {
					scripts += n
				}
			}
		}
	}
	return total, scripts
}
