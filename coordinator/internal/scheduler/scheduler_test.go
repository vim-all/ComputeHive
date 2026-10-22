package scheduler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"coordinator/internal/queue"
)

const nowMs = 1_000_000_000

// worker builds a healthy, idle 8-CPU / 16 GB worker; tests then tweak it.
func worker(id string, mod ...func(*queue.WorkerState)) queue.WorkerState {
	w := queue.WorkerState{
		ID: id, Alive: true, HasUsage: true, CPU: 8, MemMB: 16384, MaxJobs: 2, Preference: 50,
		OS: "linux", Arch: "amd64", Isolation: "runc", MemTotalMB: 16384, MemUsedMB: 2048,
		DiskFreeMB: 50_000, Reliability: 0.5, Cached: map[string]bool{}, RegisteredMs: nowMs - 3_600_000,
	}
	for _, m := range mod {
		m(&w)
	}
	return w
}

func job(mod ...func(*queue.Spec)) queue.PendingJob {
	j := queue.PendingJob{Job: queue.Job{ID: "j1", Spec: queue.Spec{CPUCores: 1, MemoryMB: 512}}}
	for _, m := range mod {
		m(&j.Spec)
	}
	return j
}

func TestFilterRejectionsNameTheirReason(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UserLimit = 2
	cases := []struct {
		name string
		w    queue.WorkerState
		j    queue.PendingJob
		user int64
		want string // substring of a rejection reason; "" means eligible
	}{
		{"healthy worker is eligible", worker("w"), job(), 0, ""},
		{"stale heartbeat", worker("w", func(w *queue.WorkerState) { w.Alive, w.HeartbeatAgeMs = false, 9000 }), job(), 0, "heartbeat stale"},
		{"overloaded", worker("w", func(w *queue.WorkerState) { w.Overloaded, w.OverloadReasons = true, "mem 93%" }), job(), 0, "90% rule"},
		{"draining", worker("w", func(w *queue.WorkerState) { w.Draining = true }), job(), 0, "paused"},
		{"no usage reported", worker("w", func(w *queue.WorkerState) { w.HasUsage = false }), job(), 0, "not reported resource usage"},
		{"too few CPUs", worker("w", func(w *queue.WorkerState) { w.CPU = 1 }), job(func(s *queue.Spec) { s.CPUCores = 4 }), 0, "needs 4 CPU"},
		{"too little RAM", worker("w", func(w *queue.WorkerState) { w.MemMB = 1024 }), job(func(s *queue.Spec) { s.MemoryMB = 4096 }), 0, "needs 4096 MB RAM"},
		{"GPU job on CPU-only worker", worker("w"), job(func(s *queue.Spec) { s.GPUCount = 1 }), 0, "needs 1 GPU"},
		{"GPU job on GPU worker", worker("w", func(w *queue.WorkerState) { w.GPU = 1 }), job(func(s *queue.Spec) { s.GPUCount = 1 }), 0, ""},
		{"not enough disk", worker("w", func(w *queue.WorkerState) { w.DiskFreeMB = 100 }), job(func(s *queue.Spec) { s.MinDiskMB = 1000 }), 0, "free disk"},
		{"wrong os", worker("w"), job(func(s *queue.Spec) { s.OS = "darwin" }), 0, "needs os darwin"},
		{"wrong arch", worker("w"), job(func(s *queue.Spec) { s.Arch = "arm64" }), 0, "needs arch arm64"},
		{"isolation", worker("w"), job(func(s *queue.Spec) { s.RequiredIsolation = "gvisor" }), 0, "gvisor isolation"},
		{"network not allowed", worker("w"), job(func(s *queue.Spec) { s.NetworkHosts = []string{"pypi.org"} }), 0, "network"},
		{"network allowed", worker("w", func(w *queue.WorkerState) { w.AllowsNet = true }), job(func(s *queue.Spec) { s.NetworkHosts = []string{"pypi.org"} }), 0, ""},
		{"label missing", worker("w"), job(func(s *queue.Spec) { s.Labels = []string{"fast-disk"} }), 0, "missing required label fast-disk"},
		{"label present", worker("w", func(w *queue.WorkerState) { w.Labels = []string{"fast-disk"} }), job(func(s *queue.Spec) { s.Labels = []string{"fast-disk"} }), 0, ""},
		{"memory headroom would cross 90%", worker("w", func(w *queue.WorkerState) { w.MemUsedMB = 14000 }), job(func(s *queue.Spec) { s.MemoryMB = 1000 }), 0, "no memory headroom"},
		{"unreliable worker blacklisted", worker("w", func(w *queue.WorkerState) { w.Reliability, w.ReliabilitySamples = 0.1, 20 }), job(), 0, "blacklisted"},
		{"new worker with a bad score is not blacklisted yet", worker("w", func(w *queue.WorkerState) { w.Reliability, w.ReliabilitySamples = 0.1, 1 }), job(), 0, ""},
		{"user at limit", worker("w"), job(func(s *queue.Spec) { s.User = "alice" }), 2, "already has 2 jobs"},
		{"user under limit", worker("w"), job(func(s *queue.Spec) { s.User = "alice" }), 1, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(Filter(tc.w, tc.j, tc.user, cfg), "; ")
			if tc.want == "" && got != "" {
				t.Fatalf("expected eligible, rejected: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("reasons %q do not mention %q", got, tc.want)
			}
		})
	}
}

func TestExcludedWorkerRejectedForVerificationReplica(t *testing.T) {
	j := job()
	j.Exclude = "w"
	if r := Filter(worker("w"), j, 0, DefaultConfig()); len(r) == 0 || !strings.Contains(r[0], "verification") {
		t.Fatalf("reasons: %v", r)
	}
}

func TestScoreComponentsMoveInTheRightDirection(t *testing.T) {
	cfg := DefaultConfig()
	j := job(func(s *queue.Spec) { s.ArtifactSHA256 = "abc" })
	// A mid-load reference; every variant changes exactly one thing.
	mid := func(w *queue.WorkerState) {
		w.CPUPct, w.Load1, w.MemUsedMB, w.Reliability, w.Preference = 40, 4, 8000, 0.5, 50
	}
	ref, _ := Score(worker("w", mid), j, nowMs, cfg)
	variant := func(f func(*queue.WorkerState)) float64 {
		s, _ := Score(worker("w", mid, f), j, nowMs, cfg)
		return s
	}
	better := map[string]func(*queue.WorkerState){
		"more free RAM":    func(w *queue.WorkerState) { w.MemUsedMB = 1000 },
		"idle CPU":         func(w *queue.WorkerState) { w.CPUPct = 0 },
		"lower load":       func(w *queue.WorkerState) { w.Load1 = 0 },
		"reliable":         func(w *queue.WorkerState) { w.Reliability = 0.95 },
		"has the artifact": func(w *queue.WorkerState) { w.Cached = map[string]bool{"abc": true} },
		"longer uptime":    func(w *queue.WorkerState) { w.RegisteredMs = nowMs - 48*3_600_000 },
		"keen contributor": func(w *queue.WorkerState) { w.Preference = 100 },
	}
	worse := map[string]func(*queue.WorkerState){
		"less free RAM": func(w *queue.WorkerState) { w.MemUsedMB = 12000 },
		"busy CPU":      func(w *queue.WorkerState) { w.CPUPct = 90 },
		"loaded":        func(w *queue.WorkerState) { w.Load1 = 8 },
		"flaky":         func(w *queue.WorkerState) { w.Reliability = 0.2 },
	}
	for name, f := range better {
		if s := variant(f); s <= ref {
			t.Errorf("%s: score %.4f did not beat the reference %.4f", name, s, ref)
		}
	}
	for name, f := range worse {
		if s := variant(f); s >= ref {
			t.Errorf("%s: score %.4f did not fall below the reference %.4f", name, s, ref)
		}
	}
}

func TestScoreBreakdownSumsToTotal(t *testing.T) {
	total, parts := Score(worker("w"), job(), nowMs, DefaultConfig())
	var sum float64
	for _, k := range partOrder {
		sum += parts[k]
	}
	if len(parts) != len(partOrder) {
		t.Fatalf("parts %v do not match partOrder", parts)
	}
	if d := sum - total; d > 1e-9 || d < -1e-9 {
		t.Fatalf("parts sum %.6f != total %.6f", sum, total)
	}
	if total <= 0 || total > 1.0001 {
		t.Fatalf("total %.4f outside (0,1]", total)
	}
}

func TestBestFitPrefersTheEmptierWorkerAndIsDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	ws := []queue.WorkerState{
		worker("busy", func(w *queue.WorkerState) { w.MemUsedMB, w.CPUPct, w.Load1 = 11000, 60, 5 }),
		worker("idle", func(w *queue.WorkerState) { w.MemUsedMB, w.CPUPct, w.Load1 = 1000, 5, 0.2 }),
		worker("mid", func(w *queue.WorkerState) { w.MemUsedMB, w.CPUPct, w.Load1 = 6000, 30, 2 }),
	}
	for i := 0; i < 20; i++ { // input order must not matter
		in := append([]queue.WorkerState(nil), ws...)
		in[0], in[i%3] = in[i%3], in[0]
		if d := Evaluate(job(), in, 0, nowMs, cfg); d.Chosen != "idle" {
			t.Fatalf("round %d chose %s, want idle", i, d.Chosen)
		}
	}
}

func TestIdenticalWorkersTieBreakOnID(t *testing.T) {
	d := Evaluate(job(), []queue.WorkerState{worker("b"), worker("a"), worker("c")}, 0, nowMs, DefaultConfig())
	if d.Chosen != "a" {
		t.Fatalf("chose %s, want a", d.Chosen)
	}
}

func TestNeverChoosesOverloadedOrIneligibleWorkers(t *testing.T) {
	ws := []queue.WorkerState{
		worker("hot", func(w *queue.WorkerState) { w.Overloaded = true; w.MemUsedMB = 0 }), // otherwise the best score
		worker("gpuless"),
	}
	d := Evaluate(job(func(s *queue.Spec) { s.GPUCount = 1 }), ws, 0, nowMs, DefaultConfig())
	if d.Chosen != "" {
		t.Fatalf("chose %s although nobody is eligible", d.Chosen)
	}
	if !strings.Contains(d.Summary(), "every worker was rejected") {
		t.Fatalf("summary: %s", d.Summary())
	}
	for _, c := range d.Candidates {
		if c.Eligible || len(c.Reasons) == 0 {
			t.Fatalf("%s: eligible=%v reasons=%v", c.WorkerID, c.Eligible, c.Reasons)
		}
	}
}

func TestBusyWorkersAreEligibleButNotChosen(t *testing.T) {
	busy := worker("busy", func(w *queue.WorkerState) { w.Running = []string{"x", "y"} })
	d := Evaluate(job(), []queue.WorkerState{busy}, 0, nowMs, DefaultConfig())
	if d.Chosen != "" || !d.Candidates[0].Eligible || !d.Candidates[0].Busy {
		t.Fatalf("%+v", d)
	}
	if !strings.Contains(d.Summary(), "all are busy") {
		t.Fatalf("summary: %s", d.Summary())
	}
}

func TestOfferGivesTheJobToTheBestWorkerOnly(t *testing.T) {
	cfg := DefaultConfig()
	d := Evaluate(job(), []queue.WorkerState{
		worker("best", func(w *queue.WorkerState) { w.MemUsedMB = 500 }),
		worker("ok", func(w *queue.WorkerState) { w.MemUsedMB = 6000 }),
	}, 0, nowMs, cfg)
	if !d.Offer("best", 0, cfg) {
		t.Fatal("best worker was not offered the job")
	}
	if d.Offer("ok", time.Second, cfg) {
		t.Fatal("a lesser worker was offered a fresh job while a better one is idle")
	}
	if d.Offer("unknown", time.Hour, cfg) {
		t.Fatal("a worker the scheduler does not know was offered a job")
	}
}

func TestPatienceLetsAnyEligibleIdleWorkerTakeAnAgedJob(t *testing.T) {
	cfg := DefaultConfig()
	d := Evaluate(job(), []queue.WorkerState{
		worker("best", func(w *queue.WorkerState) { w.MemUsedMB = 500 }),
		worker("ok", func(w *queue.WorkerState) { w.MemUsedMB = 6000 }),
		worker("hot", func(w *queue.WorkerState) { w.Overloaded = true }),
	}, 0, nowMs, cfg)
	if !d.Offer("ok", cfg.Patience, cfg) {
		t.Fatal("aged job was not released to an eligible idle worker (starvation)")
	}
	if d.Offer("hot", time.Hour, cfg) {
		t.Fatal("patience must never override the 90% rule")
	}
}

func TestLocalityOutweighsASmallLoadDifference(t *testing.T) {
	cfg := DefaultConfig()
	j := job(func(s *queue.Spec) { s.ArtifactSHA256 = "deadbeef" })
	cached := worker("cached", func(w *queue.WorkerState) { w.MemUsedMB, w.Cached = 3000, map[string]bool{"deadbeef": true} })
	empty := worker("empty", func(w *queue.WorkerState) { w.MemUsedMB = 2500 })
	if d := Evaluate(j, []queue.WorkerState{empty, cached}, 0, nowMs, cfg); d.Chosen != "cached" {
		t.Fatalf("chose %s, want the worker that already holds the artifact", d.Chosen)
	}
}

func TestSmallJobsPreferSmallWorkersWhenOtherwiseEqual(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Weights = Weights{Fit: 1} // isolate anti-fragmentation
	small := worker("small", func(w *queue.WorkerState) { w.MemMB, w.MemTotalMB = 2048, 2048; w.MemUsedMB = 200 })
	big := worker("big")
	if d := Evaluate(job(), []queue.WorkerState{big, small}, 0, nowMs, cfg); d.Chosen != "small" {
		t.Fatalf("chose %s: a 512 MB job should not consume the big worker", d.Chosen)
	}
}

// BenchmarkEvaluate measures the cost of one placement decision (filter + score + rank) against a
// cluster of n workers. It is pure CPU work: no Redis round trip is included.
func BenchmarkEvaluate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		ws := make([]queue.WorkerState, n)
		for i := range ws {
			id := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26))
			ws[i] = worker(id, func(w *queue.WorkerState) {
				w.MemUsedMB, w.CPUPct, w.Load1 = int64(1000+i*7%9000), float64(i%80), float64(i%6)
			})
		}
		j, cfg := job(), DefaultConfig()
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Evaluate(j, ws, 0, nowMs, cfg)
			}
		})
	}
}
