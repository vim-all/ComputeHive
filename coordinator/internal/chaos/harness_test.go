// Package chaos holds fault-injection tests: real Redis (Docker), the real gRPC
// coordinator in-process, and fake workers speaking the real WorkerService
// protocol that tests can kill, partition, or run slowly.
package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
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

// timing profiles
type profile struct {
	hb          time.Duration // worker heartbeat interval
	deadAfter   time.Duration
	reap        time.Duration
	backoffBase time.Duration
	maxAttempts int
}

// specProfile is the production default: 2s heartbeats, dead after 6s, reaper every 1s.
var specProfile = profile{hb: 2 * time.Second, deadAfter: 6 * time.Second, reap: time.Second, backoffBase: time.Second, maxAttempts: 3}

// fastProfile compresses time so high-volume tests finish quickly.
var fastProfile = profile{hb: 100 * time.Millisecond, deadAfter: 1 * time.Second, reap: 100 * time.Millisecond, backoffBase: 20 * time.Millisecond, maxAttempts: 10}

func (p profile) queueConfig() queue.Config {
	c := queue.DefaultConfig()
	c.DeadAfter, c.ReapInterval, c.BackoffBase, c.BackoffMax, c.MaxAttempts = p.deadAfter, p.reap, p.backoffBase, p.backoffBase*8, p.maxAttempts
	c.OrphanGrace = 5 * p.hb
	return c
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// coordinator is an in-process coordinator on a fixed address, so it can be
// killed and restarted without workers needing a new address.
type coordinator struct {
	addr   string
	cancel context.CancelFunc
	done   chan struct{}
	q      *queue.Queue
}

func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startCoordinator(t testing.TB, r *testutil.Redis, addr string, p profile) *coordinator {
	t.Helper()
	rdb := r.Client()
	q := queue.New(rdb, p.queueConfig())
	cfg := config.Config{HeartbeatInterval: p.hb, WorkerDeadAfter: p.deadAfter, ReapInterval: p.reap}
	svc := grpcserver.NewServer(q, cfg, quietLog())

	var l net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port may linger briefly after a previous instance
		if l, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &coordinator{addr: addr, cancel: cancel, done: make(chan struct{}), q: q}
	go q.RunReaper(ctx, quietLog(), nil)
	go func() {
		defer close(c.done)
		_ = grpcserver.Serve(ctx, l, svc, nil)
		_ = rdb.Close()
	}()
	t.Cleanup(c.Kill)
	return c
}

// Kill stops the coordinator (idempotent). It holds no state, so this is a crash as far as the cluster can tell.
func (c *coordinator) Kill() {
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
	}
}

// ledger counts, from the workers' side, how the coordinator answered result submissions.
// It is diagnostic only: exactly-once is audited from the server's own state (auditCommits),
// because a worker killed after the coordinator committed never sees the reply.
type ledger struct {
	ok, stale, dup atomic.Int64
}

func newLedger() *ledger { return &ledger{} }

func (l *ledger) record(_, _, disposition string) {
	switch disposition {
	case "ok":
		l.ok.Add(1)
	case "stale":
		l.stale.Add(1)
	case "duplicate":
		l.dup.Add(1)
	}
}

// fakeWorker speaks WorkerService like the real worker: register, heartbeat with its
// running jobs, poll for work, run, submit with its lease token.
type fakeWorker struct {
	id     string
	addr   string
	p      profile
	led    *ledger
	runFor func(jobID string) time.Duration

	cancel     context.CancelFunc // kills everything (SIGKILL)
	partition  atomic.Bool        // stop heartbeating but keep working (network partition)
	finished   atomic.Int64
	current    atomic.Value // string
	aborted    atomic.Int64 // jobs aborted because the coordinator revoked the lease
	hbFailures atomic.Int64
	wg         sync.WaitGroup
}

func (w *fakeWorker) dial() (pb.WorkerServiceClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(w.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewWorkerServiceClient(conn), conn
}

func startWorker(t testing.TB, addr, id string, p profile, led *ledger, runFor func(string) time.Duration) *fakeWorker {
	w := &fakeWorker{id: id, addr: addr, p: p, led: led, runFor: runFor}
	w.current.Store("")
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	client, conn := w.dial()

	var abortMu sync.Mutex
	var abortJob func()

	w.wg.Add(2)
	go func() { // heartbeat loop
		defer w.wg.Done()
		tick := time.NewTicker(p.hb)
		defer tick.Stop()
		for {
			if !w.partition.Load() {
				req := &pb.HeartbeatRequest{WorkerId: &pb.WorkerID{Value: id},
					AvailableResources: &pb.ResourceSpec{CpuCores: 8, MemoryMb: 16384}, MaxJobs: 1}
				if cur := w.current.Load().(string); cur != "" {
					req.RunningJobs = []*pb.JobID{{Value: cur}}
				}
				hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
				resp, err := client.Heartbeat(hctx, req)
				hcancel()
				if err != nil {
					w.hbFailures.Add(1)
					_, _ = client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{WorkerId: id, MaxJobs: 1,
						AvailableResources: &pb.ResourceSpec{CpuCores: 8, MemoryMb: 16384}})
				} else {
					for _, jid := range resp.GetCancelJobIds() {
						if jid == w.current.Load().(string) {
							abortMu.Lock()
							if abortJob != nil {
								abortJob()
							}
							abortMu.Unlock()
						}
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	go func() { // work loop
		defer w.wg.Done()
		defer conn.Close()
		for ctx.Err() == nil {
			rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
			resp, err := client.RequestJob(rctx, &pb.RequestJobRequest{WorkerId: &pb.WorkerID{Value: id}})
			rcancel()
			if err != nil || !resp.GetHasJob() {
				if !sleep(ctx, 15*time.Millisecond) {
					return
				}
				continue
			}
			job := resp.GetJob()
			jid := job.GetJobId().GetValue()
			w.current.Store(jid)
			_, _ = client.SubmitResult(ctx, &pb.SubmitResultRequest{WorkerId: &pb.WorkerID{Value: id}, LeaseToken: job.GetLeaseToken(),
				Result: &pb.Result{JobId: job.GetJobId(), Status: pb.Status_STATUS_RUNNING}})

			jobCtx, jobCancel := context.WithCancel(ctx)
			abortMu.Lock()
			abortJob = func() { w.aborted.Add(1); jobCancel() }
			abortMu.Unlock()
			ran := sleep(jobCtx, w.runFor(jid))
			jobCancel()
			w.current.Store("")
			if !ran {
				if ctx.Err() != nil {
					return // killed mid-job: no result is ever sent
				}
				continue // lease revoked: discard
			}

			var disp string
			for attempt := 0; attempt < 40 && ctx.Err() == nil; attempt++ { // ride out a coordinator restart
				sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
				sresp, serr := client.SubmitResult(sctx, &pb.SubmitResultRequest{WorkerId: &pb.WorkerID{Value: id}, LeaseToken: job.GetLeaseToken(),
					Result: &pb.Result{JobId: job.GetJobId(), Status: pb.Status_STATUS_SUCCEEDED, StdoutExcerpt: "by " + id}})
				scancel()
				if serr == nil {
					disp = sresp.GetDisposition()
					break
				}
				sleep(ctx, 100*time.Millisecond)
			}
			if disp != "" {
				w.led.record(jid, id, disp)
				w.finished.Add(1)
			}
		}
	}()
	t.Cleanup(w.Kill)
	return w
}

func sleep(ctx context.Context, d time.Duration) bool {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-tm.C:
		return true
	}
}

// Kill is SIGKILL: no result, no goodbye, heartbeats just stop.
func (w *fakeWorker) Kill() {
	w.cancel()
	w.wg.Wait()
}

func (w *fakeWorker) Current() string { return w.current.Load().(string) }

// helpers shared by tests

// auditCommits is the authoritative exactly-once check, made from the server's own
// bookkeeping (a worker's ledger can miss a commit whose reply was lost to a kill):
// every job must be SUCCEEDED with its result committed exactly once.
func auditCommits(t testing.TB, q *queue.Queue, ids []string) {
	t.Helper()
	bad := 0
	for _, id := range ids {
		j, err := q.Get(context.Background(), id)
		if err != nil || j == nil {
			t.Errorf("job %s: %v %v", id, j, err)
			bad++
			continue
		}
		if j.State != queue.StateSucceeded || j.Commits != 1 {
			t.Errorf("job %s: state=%s commits=%d, want SUCCEEDED with exactly 1 commit", id, j.State, j.Commits)
			bad++
		}
		if bad > 20 { // enough to diagnose
			break
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d jobs violated exactly-once", bad, len(ids))
	}
}

func succeeded(q *queue.Queue) int64 {
	c, _ := q.Counts(context.Background())
	return c[queue.StateSucceeded]
}

func submit(t testing.TB, c pb.ClientServiceClient, n int, mutate func(i int, r *pb.SubmitJobRequest)) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		req := &pb.SubmitJobRequest{ContainerImage: "img", Command: []string{"x"}, RequiredResources: &pb.ResourceSpec{CpuCores: 1, MemoryMb: 64}}
		if mutate != nil {
			mutate(i, req)
		}
		var resp *pb.SubmitJobResponse
		var err error
		for try := 0; try < 100; try++ { // ride out a coordinator restart
			resp, err = c.SubmitJob(context.Background(), req)
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		ids = append(ids, resp.GetJobId().GetValue())
	}
	return ids
}

func clientFor(t testing.TB, addr string) pb.ClientServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewClientServiceClient(conn)
}

// waitFor polls cond until true or the timeout, failing the test with msg().
func waitFor(t testing.TB, timeout time.Duration, cond func() bool, msg func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s: %s", timeout, msg())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// report appends "name value" to $CHAOS_REPORT (used by the benchmark phase).
func report(name string, value float64) {
	path := os.Getenv("CHAOS_REPORT")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s %.3f\n", name, value)
}
