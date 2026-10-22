package grpcserver

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"coordinator/internal/auth"
	"coordinator/internal/config"
	"coordinator/internal/queue"
	"coordinator/internal/testutil"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cluster is a coordinator on a real Redis plus a client and a worker connection, with scheduling
// settings the test chooses.
type cluster struct {
	w   pb.WorkerServiceClient
	c   pb.ClientServiceClient
	q   *queue.Queue
	svc *Server           // the coordinator service, for tests that need to reach the placer
	tok map[string]string // lease token of each job a fake worker was given
}

func startCluster(t *testing.T, mod func(*config.Config)) *cluster {
	t.Helper()
	r := testutil.Start(t)
	rdb := r.Client()
	t.Cleanup(func() { rdb.Close() })
	qcfg := queue.DefaultConfig()
	qcfg.DeadAfter, qcfg.ReapInterval, qcfg.BackoffBase, qcfg.BackoffMax = 10*time.Second, 100*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	qcfg.RequireUsage = true

	cfg := config.Config{HeartbeatInterval: 100 * time.Millisecond, WorkerDeadAfter: 10 * time.Second, RequireUsage: true,
		LimitRAMPct: 90, SchedPatience: time.Hour, SchedSnapshotTTL: 0}
	if mod != nil {
		mod(&cfg)
	}
	qcfg.UserLimit = cfg.UserLimit
	q := queue.New(rdb, qcfg)
	acfg := auth.DefaultConfig()
	acfg.Disabled = true
	svc := NewServer(q, cfg, quiet()).WithAuth(auth.New(rdb, acfg, quiet()))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = Serve(ctx, l, svc, nil); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &cluster{w: pb.NewWorkerServiceClient(conn), c: pb.NewClientServiceClient(conn), q: q, svc: svc, tok: map[string]string{}}
}

// box describes a fake worker's hardware and current load.
type box struct {
	cpu, gpu     int32
	memMB        int32
	memUsedMB    int64
	cpuPct       float64
	osName, arch string
	labels       []string
	isolation    string
	overloaded   bool
	diskFreeMB   int64
	cached       []string
}

func (b box) usage() *pb.ResourceUsage {
	return &pb.ResourceUsage{MemTotalMb: int64(b.memMB), MemUsedMb: b.memUsedMB, MemUsedPercent: float64(b.memUsedMB) * 100 / float64(b.memMB),
		CpuPercent: b.cpuPct, WorkDiskFreeMb: b.diskFreeMB, DockerHealthy: true}
}

func (b *box) defaults() {
	if b.osName == "" {
		b.osName = "linux"
	}
	if b.arch == "" {
		b.arch = "amd64"
	}
	if b.diskFreeMB == 0 {
		b.diskFreeMB = 100_000
	}
}

func (cl *cluster) join(t *testing.T, id string, b box) {
	t.Helper()
	b.defaults()
	if _, err := cl.w.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{WorkerId: id, MaxJobs: 1, Isolation: b.isolation,
		AvailableResources: &pb.ResourceSpec{CpuCores: b.cpu, MemoryMb: b.memMB, GpuCount: b.gpu}, Os: b.osName, Arch: b.arch, Labels: b.labels}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	cl.beat(t, id, b)
}

func (cl *cluster) beat(t *testing.T, id string, b box) {
	t.Helper()
	b.defaults()
	if _, err := cl.w.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: wid(id), MaxJobs: 1, Overloaded: b.overloaded, Isolation: b.isolation,
		AvailableResources: &pb.ResourceSpec{CpuCores: b.cpu, MemoryMb: b.memMB, GpuCount: b.gpu}, Usage: b.usage(),
		Os: b.osName, Arch: b.arch, Labels: b.labels, CachedArtifacts: b.cached}); err != nil {
		t.Fatalf("heartbeat %s: %v", id, err)
	}
}

func (cl *cluster) submit(t *testing.T, mod func(*pb.SubmitJobRequest)) string {
	t.Helper()
	req := &pb.SubmitJobRequest{ContainerImage: "alpine:3", Command: []string{"true"}, RequiredResources: &pb.ResourceSpec{CpuCores: 1, MemoryMb: 256}}
	if mod != nil {
		mod(req)
	}
	resp, err := cl.c.SubmitJob(context.Background(), req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return resp.GetJobId().GetValue()
}

// ask is a worker asking for work; it returns the job id it was given ("" if none).
func (cl *cluster) ask(t *testing.T, id string) string {
	t.Helper()
	resp, err := cl.w.RequestJob(context.Background(), &pb.RequestJobRequest{WorkerId: wid(id)})
	if err != nil {
		t.Fatalf("request job %s: %v", id, err)
	}
	if !resp.GetHasJob() {
		return ""
	}
	job := resp.GetJob().GetJobId().GetValue()
	cl.tok[job] = resp.GetJob().GetLeaseToken()
	return job
}

// finish completes a job the worker holds so the worker becomes idle again.
func (cl *cluster) finish(t *testing.T, worker, job string) {
	t.Helper()
	j, err := cl.q.Get(context.Background(), job)
	if err != nil || j == nil {
		t.Fatalf("get %s: %v", job, err)
	}
	if _, err := cl.q.Complete(context.Background(), job, cl.tok[job], true, queue.Result{JobID: job, WorkerID: worker, Attempt: j.Attempt, Status: "SUCCEEDED"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func (cl *cluster) explain(t *testing.T, job string) *pb.ExplainJobResponse {
	t.Helper()
	r, err := cl.c.ExplainJob(context.Background(), &pb.ExplainJobRequest{JobId: &pb.JobID{Value: job}})
	if err != nil {
		t.Fatalf("explain %s: %v", job, err)
	}
	return r
}

func evalFor(r *pb.ExplainJobResponse, worker string) *pb.WorkerEvaluation {
	for _, e := range r.GetEvaluations() {
		if e.GetWorkerId().GetValue() == worker {
			return e
		}
	}
	return nil
}

func TestHeterogeneousClusterPlacesEachJobOnItsBestFit(t *testing.T) {
	cl := startCluster(t, nil)
	cl.join(t, "gpu-box", box{cpu: 16, gpu: 1, memMB: 32768, memUsedMB: 4000, cpuPct: 10})
	cl.join(t, "big-cpu", box{cpu: 16, memMB: 32768, memUsedMB: 4000, cpuPct: 10})
	cl.join(t, "small", box{cpu: 2, memMB: 2048, memUsedMB: 400, cpuPct: 10})
	cl.join(t, "busy", box{cpu: 16, memMB: 32768, memUsedMB: 20000, cpuPct: 70})

	// A GPU job can only go to the GPU box, no matter who asks first.
	gpuJob := cl.submit(t, func(r *pb.SubmitJobRequest) { r.RequiredResources.GpuCount = 1 })
	for _, w := range []string{"small", "busy", "big-cpu"} {
		if got := cl.ask(t, w); got != "" {
			t.Fatalf("%s was given %s, but it has no GPU", w, got)
		}
	}
	if got := cl.ask(t, "gpu-box"); got != gpuJob {
		t.Fatalf("gpu-box got %q, want the GPU job %s", got, gpuJob)
	}
	ex := cl.explain(t, gpuJob)
	if !ex.GetRecorded() || ex.GetAssignedWorkerId() != "gpu-box" {
		t.Fatalf("explain: recorded=%v assigned=%q", ex.GetRecorded(), ex.GetAssignedWorkerId())
	}
	for _, w := range []string{"small", "busy", "big-cpu"} {
		e := evalFor(ex, w)
		if e == nil || e.GetEligible() || !strings.Contains(strings.Join(e.GetReasons(), ";"), "GPU") {
			t.Fatalf("explain should say %s was rejected for lacking a GPU: %+v", w, e)
		}
	}
	if e := evalFor(ex, "gpu-box"); e == nil || !e.GetChosen() || e.GetScore() <= 0 || len(e.GetParts()) == 0 {
		t.Fatalf("explain should show a score breakdown for the chosen worker: %+v", e)
	}

	// An ordinary job is held for the best-fit worker (the lightly loaded big one), not the first to ask.
	job := cl.submit(t, nil)
	for _, w := range []string{"busy", "small"} {
		if got := cl.ask(t, w); got != "" {
			t.Fatalf("%s grabbed the job although a better-fit idle worker exists (got %s)", w, got)
		}
	}
	if got := cl.ask(t, "big-cpu"); got != job {
		t.Fatalf("big-cpu got %q, want %s", got, job)
	}
}

func TestOverloadedWorkersAreNeverChosenEvenWhenTheyAreTheOnlyOnesAsking(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0 }) // patience elapsed: anyone eligible may take it
	cl.join(t, "hot", box{cpu: 8, memMB: 16384, memUsedMB: 15000, cpuPct: 99, overloaded: true})
	job := cl.submit(t, nil)
	for i := 0; i < 5; i++ {
		if got := cl.ask(t, "hot"); got != "" {
			t.Fatalf("an overloaded worker was given %s", got)
		}
	}
	ex := cl.explain(t, job)
	if e := evalFor(ex, "hot"); e == nil || e.GetEligible() || !strings.Contains(strings.Join(e.GetReasons(), ";"), "90%") {
		t.Fatalf("explain should cite the 90%% rule: %+v", e)
	}
	// Once it has recovered (the hysteresis holds it for a while) it can have the job.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cl.beat(t, "hot", box{cpu: 8, memMB: 16384, memUsedMB: 2000, cpuPct: 5})
		if got := cl.ask(t, "hot"); got == job {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the worker never received the job after recovering")
}

func TestPatienceReleasesAnAgedJobToAnyEligibleWorker(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 600 * time.Millisecond })
	best := box{cpu: 8, memMB: 16384, memUsedMB: 1000, cpuPct: 2}   // never asks: a silent better worker
	other := box{cpu: 8, memMB: 16384, memUsedMB: 9000, cpuPct: 40} // keeps asking
	cl.join(t, "best", best)
	cl.join(t, "other", other)
	job := cl.submit(t, nil)
	if got := cl.ask(t, "other"); got != "" {
		t.Fatalf("job handed over before the patience window: %s", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cl.beat(t, "best", best)
		cl.beat(t, "other", other)
		if got := cl.ask(t, "other"); got == job {
			if ex := cl.explain(t, job); !strings.Contains(ex.GetSummary(), "patience") {
				t.Fatalf("summary should say patience released it: %q", ex.GetSummary())
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the job starved: patience never released it to the worker that was asking")
}

func TestRequirementsOSArchLabelsDiskIsolationAreEnforced(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0 })
	cl.join(t, "mac", box{cpu: 8, memMB: 16384, memUsedMB: 2000, osName: "darwin", arch: "arm64", labels: []string{"fast-disk"}, diskFreeMB: 500})
	cl.join(t, "linux", box{cpu: 8, memMB: 16384, memUsedMB: 2000, osName: "linux", arch: "amd64", isolation: "hardened-runc", diskFreeMB: 50_000})

	cases := []struct {
		name string
		mod  func(*pb.SubmitJobRequest)
		want string
	}{
		{"arm64 job", func(r *pb.SubmitJobRequest) { r.Arch = "arm64" }, "mac"},
		{"darwin job", func(r *pb.SubmitJobRequest) { r.Os = "darwin" }, "mac"},
		{"labelled job", func(r *pb.SubmitJobRequest) { r.Labels = []string{"fast-disk"} }, "mac"},
		{"disk-hungry job", func(r *pb.SubmitJobRequest) { r.MinDiskMb = 10_000 }, "linux"},
		{"gvisor-only job", func(r *pb.SubmitJobRequest) { r.RequiredIsolation = "gvisor" }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := cl.submit(t, tc.mod)
			var got string
			for _, w := range []string{"mac", "linux"} {
				if g := cl.ask(t, w); g != "" {
					got = w
					if g != job {
						t.Fatalf("%s was given %s, want %s", w, g, job)
					}
					cl.finish(t, w, job) // free the worker for the next case
					break
				}
			}
			if got != tc.want {
				t.Fatalf("job ran on %q, want %q", got, tc.want)
			}
			if tc.want == "" {
				if ex := cl.explain(t, job); !strings.Contains(ex.GetSummary(), "no worker can run") {
					t.Fatalf("explain should say nobody can run it: %q", ex.GetSummary())
				}
				_, _ = cl.c.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: &pb.JobID{Value: job}})
			}
		})
	}
}

func TestPerUserLimitHoldsExtraJobsWhileOtherUsersProceed(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0; c.UserLimit = 1 })
	for _, id := range []string{"w1", "w2", "w3"} {
		cl.join(t, id, box{cpu: 8, memMB: 16384, memUsedMB: 2000})
	}
	a1 := cl.submit(t, func(r *pb.SubmitJobRequest) { r.User = "alice" })
	a2 := cl.submit(t, func(r *pb.SubmitJobRequest) { r.User = "alice" })
	b1 := cl.submit(t, func(r *pb.SubmitJobRequest) { r.User = "bob" })

	if got := cl.ask(t, "w1"); got != a1 {
		t.Fatalf("w1 got %q, want alice's first job %s", got, a1)
	}
	if got := cl.ask(t, "w2"); got != b1 { // alice is at her limit; bob's later job is not stuck behind hers
		t.Fatalf("w2 got %q, want bob's job %s (alice is at her limit of 1)", got, b1)
	}
	if got := cl.ask(t, "w3"); got != "" {
		t.Fatalf("w3 got %q: alice exceeded her limit", got)
	}
	ex := cl.explain(t, a2)
	if e := evalFor(ex, "w3"); e == nil || !strings.Contains(strings.Join(e.GetReasons(), ";"), "alice already has 1 jobs running") {
		t.Fatalf("explain should cite the user limit: %+v", e)
	}
	cl.finish(t, "w1", a1)
	if got := cl.ask(t, "w3"); got != a2 {
		t.Fatalf("after alice's job finished w3 got %q, want %s", got, a2)
	}
}

func TestListWorkersReportsSchedulingFacts(t *testing.T) {
	cl := startCluster(t, nil)
	cl.join(t, "arm", box{cpu: 4, memMB: 8192, memUsedMB: 1000, osName: "linux", arch: "arm64", labels: []string{"rack-1"}})
	resp, err := cl.c.ListWorkers(context.Background(), &pb.ListWorkersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	w := resp.GetWorkers()[0]
	if w.GetArch() != "arm64" || w.GetOs() != "linux" || len(w.GetLabels()) != 1 || w.GetLabels()[0] != "rack-1" || w.GetReliability() <= 0 {
		t.Fatalf("worker info: %+v", w)
	}
}

func TestSubmitValidatesSchedulingFields(t *testing.T) {
	cl := startCluster(t, nil)
	bad := map[string]func(*pb.SubmitJobRequest){
		"label with comma": func(r *pb.SubmitJobRequest) { r.Labels = []string{"a,b"} },
		"label with space": func(r *pb.SubmitJobRequest) { r.Labels = []string{"a b"} },
		"too many labels": func(r *pb.SubmitJobRequest) {
			for i := 0; i < 40; i++ {
				r.Labels = append(r.Labels, "l"+string(rune('a'+i%26)))
			}
		},
		"weird os":      func(r *pb.SubmitJobRequest) { r.Os = "Linux; rm -rf" },
		"negative disk": func(r *pb.SubmitJobRequest) { r.MinDiskMb = -1 },
	}
	for name, mod := range bad {
		req := &pb.SubmitJobRequest{ContainerImage: "alpine:3", Command: []string{"true"}, RequiredResources: &pb.ResourceSpec{CpuCores: 1}}
		mod(req)
		if _, err := cl.c.SubmitJob(context.Background(), req); code(err).String() != "InvalidArgument" {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
}

func TestWorkerAnnouncementsAreSanitised(t *testing.T) {
	cl := startCluster(t, nil)
	cl.join(t, "liar", box{cpu: 4, memMB: 8192, memUsedMB: 1000, labels: []string{"ok-label", "bad,label", "also bad"},
		cached: []string{strings.Repeat("a", 64), "not-a-digest"}})
	resp, _ := cl.c.ListWorkers(context.Background(), &pb.ListWorkersRequest{})
	if got := resp.GetWorkers()[0].GetLabels(); len(got) != 1 || got[0] != "ok-label" {
		t.Fatalf("labels %v: malformed labels must be dropped", got)
	}
	states, _ := cl.q.WorkerStates(context.Background())
	if len(states[0].Cached) != 1 || !states[0].Cached[strings.Repeat("a", 64)] {
		t.Fatalf("cached artifacts %v: only well-formed digests may be kept", states[0].Cached)
	}
}

// With a long snapshot TTL the placer decides from its cached view. After one worker claims a job, the cache
// must reflect it at once: the next worker is offered the next job, and nobody is ever given the same job twice.
func TestSnapshotCacheReflectsClaimsImmediately(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedSnapshotTTL = time.Hour })
	b := box{cpu: 4, memMB: 8192}
	cl.join(t, "wa", b)
	cl.join(t, "wb", b)
	j1 := cl.submit(t, nil)
	j2 := cl.submit(t, nil)

	first := cl.ask(t, "wa")
	second := cl.ask(t, "wb")
	if first == "" || second == "" {
		t.Fatalf("both workers should have been given a job from the cached view: wa=%q wb=%q", first, second)
	}
	if first == second {
		t.Fatalf("job %s was assigned to two workers", first)
	}
	if got := map[string]bool{first: true, second: true}; !got[j1] || !got[j2] {
		t.Fatalf("assigned %q and %q, want %q and %q", first, second, j1, j2)
	}
	if third := cl.ask(t, "wa"); third != "" {
		t.Fatalf("worker wa is at capacity and nothing is pending, got %q", third)
	}
}

// A worker that finished its job and asks again must get the next job at once, even though the cached view
// (long TTL) still shows it running the first one.
func TestSnapshotCacheSeesCompletions(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedSnapshotTTL = time.Hour })
	cl.join(t, "wa", box{cpu: 4, memMB: 8192})
	j1 := cl.submit(t, nil)
	if got := cl.ask(t, "wa"); got != j1 {
		t.Fatalf("first ask = %q, want %q", got, j1)
	}
	cl.finish(t, "wa", j1)
	j2 := cl.submit(t, nil)
	if got := cl.ask(t, "wa"); got != j2 {
		t.Fatalf("after finishing %s the worker should be given %s immediately, got %q", j1, j2, got)
	}
}
