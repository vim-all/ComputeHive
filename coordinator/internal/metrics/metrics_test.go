package metrics_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"coordinator/internal/httpapi"
	"coordinator/internal/metrics"
	"coordinator/internal/queue"
	"coordinator/internal/testutil"
)

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape: %d %s", rec.Code, rec.Body.String())
	}
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

// has reports whether the exposition contains a line starting with prefix and ending with the value.
func has(body, prefix, value string) bool {
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, prefix) && strings.HasSuffix(strings.TrimSpace(l), " "+value) {
			return true
		}
	}
	return false
}

func newQueue(t *testing.T) *queue.Queue {
	t.Helper()
	r := testutil.Start(t)
	rdb := r.Client()
	t.Cleanup(func() { rdb.Close() })
	cfg := queue.DefaultConfig()
	cfg.DeadAfter = 30 * time.Second
	return queue.New(rdb, cfg)
}

func TestScrapeReportsTheRealClusterStateFromRedis(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, _, err := q.Enqueue(ctx, queue.Spec{ID: fmt.Sprintf("job-%d", i), ContainerImage: "alpine", Command: []string{"true"}}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.Heartbeat(ctx, queue.WorkerReport{ID: "w1", CPU: 4, MemMB: 8192, MaxJobs: 1,
		Usage: &queue.Usage{MemTotalMB: 8192, MemUsedMB: 4096, MemPct: 50, CPUPct: 12, DiskPct: 33, JSON: "{}"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // so the oldest job has a non-zero age

	body := scrape(t, metrics.New(q).Handler())
	for prefix, want := range map[string]string{
		`computehive_jobs{state="PENDING"}`:                 "3",
		`computehive_queue_depth`:                           "3",
		`computehive_redis_up`:                              "1",
		`computehive_workers{state="alive"}`:                "1",
		`computehive_workers{state="overloaded"}`:           "0",
		`computehive_worker_ram_used_percent{worker="w1"}`:  "50",
		`computehive_worker_cpu_used_percent{worker="w1"}`:  "12",
		`computehive_worker_disk_used_percent{worker="w1"}`: "33",
		`computehive_worker_running_jobs{worker="w1"}`:      "0",
		`computehive_worker_overloaded{worker="w1"}`:        "0",
	} {
		if !has(body, prefix, want) {
			t.Errorf("missing %s %s in scrape", prefix, want)
		}
	}
	if has(body, "computehive_oldest_pending_age_seconds", "0") {
		t.Error("the oldest pending job has waited over a second but the age reads 0")
	}
	if !strings.Contains(body, "computehive_worker_heartbeat_age_seconds{worker=\"w1\"}") || !strings.Contains(body, "computehive_worker_reliability{worker=\"w1\"}") {
		t.Error("per-worker heartbeat lag / reliability series missing")
	}
}

func TestOverloadedWorkerIsExportedAsOverloaded(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	if _, err := q.Heartbeat(ctx, queue.WorkerReport{ID: "hot", CPU: 4, MemMB: 8192, MaxJobs: 1,
		Usage: &queue.Usage{MemTotalMB: 8192, MemUsedMB: 7800, MemPct: 95, CPUPct: 10, JSON: "{}"}}); err != nil {
		t.Fatal(err)
	}
	body := scrape(t, metrics.New(q).Handler())
	if !has(body, `computehive_worker_overloaded{worker="hot"}`, "1") || !has(body, `computehive_workers{state="overloaded"}`, "1") {
		t.Fatalf("a worker at 95%% RAM must be exported as overloaded:\n%s", body)
	}
}

func TestEventSeriesRecordWhatHappened(t *testing.T) {
	m := metrics.New(nil)
	m.JobSubmitted()
	m.JobAssigned(1500*time.Millisecond, 2*time.Millisecond)
	m.NoJob(time.Millisecond)
	m.JobCompleted("STATUS_SUCCEEDED", "", 0, 3*time.Second)
	m.JobCompleted("STATUS_FAILED", "job_exit", 137, 0)
	m.JobRequeued("worker_dead", 2)
	m.JobRequeued("evicted", 1)
	m.JobRequeued("evicted", 0) // ignored
	m.WorkersLost(1)
	m.RPC("SubmitJob", "OK", 4*time.Millisecond)
	m.RPC("SubmitJob", "Unauthenticated", time.Millisecond)

	body := scrape(t, m.Handler())
	for prefix, want := range map[string]string{
		`computehive_jobs_submitted_total`:                                            "1",
		`computehive_job_completions_total{reason="none",status="STATUS_SUCCEEDED"}`:  "1",
		`computehive_job_completions_total{reason="job_exit",status="STATUS_FAILED"}`: "1",
		`computehive_job_requeues_total{reason="worker_dead"}`:                        "2",
		`computehive_job_requeues_total{reason="evicted"}`:                            "1",
		`computehive_jobs_killed_total{kind="kill"}`:                                  "1",
		`computehive_jobs_killed_total{kind="evicted"}`:                               "1",
		`computehive_workers_lost_total`:                                              "1",
		`computehive_scheduling_requests_total{outcome="assigned"}`:                   "1",
		`computehive_scheduling_requests_total{outcome="none"}`:                       "1",
		`computehive_job_time_in_queue_seconds_count`:                                 "1",
		`computehive_job_runtime_seconds_count`:                                       "1",
		`computehive_scheduling_decision_seconds_count`:                               "2",
		`computehive_grpc_requests_total{code="OK",method="SubmitJob"}`:               "1",
		`computehive_grpc_requests_total{code="Unauthenticated",method="SubmitJob"}`:  "1",
	} {
		if !has(body, prefix, want) {
			t.Errorf("missing %s %s", prefix, want)
		}
	}
}

func TestOverloadEventsCountTransitionsNotHeartbeats(t *testing.T) {
	m := metrics.New(nil)
	for _, over := range []bool{false, true, true, true, false, true, true} {
		m.WorkerOverloaded("w", over)
	}
	if body := scrape(t, m.Handler()); !has(body, `computehive_worker_overload_events_total{worker="w"}`, "2") {
		t.Fatalf("seven heartbeats with two entries into overload must count 2:\n%s", body)
	}
}

func TestNilMetricsAreHarmless(t *testing.T) {
	var m *metrics.Metrics
	m.JobSubmitted()
	m.JobAssigned(time.Second, time.Millisecond)
	m.NoJob(time.Millisecond)
	m.JobCompleted("s", "r", 1, time.Second)
	m.JobRequeued("x", 1)
	m.WorkersLost(1)
	m.WorkerOverloaded("w", true)
	m.RPC("m", "OK", time.Millisecond)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 404 {
		t.Fatal("a nil Metrics must not serve anything")
	}
}

type tokenAuthz struct{}

func (tokenAuthz) HTTPWriteAllowed(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer api"
}
func (tokenAuthz) HTTPReadAllowed(*http.Request) bool { return true } // a worker credential would pass this
func (tokenAuthz) AllowAnon(string) bool              { return true }
func (tokenAuthz) WorkerFromRequest(*http.Request) (string, bool) {
	return "", false
}

func TestMetricsEndpointRequiresTheAPIToken(t *testing.T) {
	m := metrics.New(nil)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "next") })
	srv := httptest.NewServer(httpapi.WithMetrics(next, m.Handler(), tokenAuthz{}))
	defer srv.Close()

	get := func(path, auth string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get("/metrics", ""); code != http.StatusUnauthorized {
		t.Errorf("anonymous scrape: %d", code)
	}
	if code, _ := get("/metrics", "Bearer wrong"); code != http.StatusUnauthorized {
		t.Errorf("wrong token: %d", code)
	}
	if code, body := get("/metrics", "Bearer api"); code != 200 || !strings.Contains(body, "go_goroutines") {
		t.Errorf("authorised scrape: %d", code)
	}
	if code, body := get("/anything-else", ""); code != 200 || body != "next" {
		t.Errorf("other paths must reach the normal handler: %d %q", code, body)
	}
}
