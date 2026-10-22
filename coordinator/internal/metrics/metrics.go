// Package metrics exposes the coordinator's state and behaviour in Prometheus format.
//
// Two kinds of series:
//   - Cluster gauges (queue depth, jobs by state, per-worker RAM/CPU/disk, heartbeat age...) are read from
//     Redis at scrape time, so they are always the truth and never drift from the data they describe.
//   - Event series (time in queue, runtime, completions, requeues, scheduling latency, RPCs...) are recorded
//     as things happen, because a state snapshot cannot tell you how long something took.
//
// Every method on *Metrics is safe to call on a nil receiver, so code paths and tests that do not
// care about metrics need no plumbing.
package metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"coordinator/internal/queue"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Source is what the scrape-time collector reads. *queue.Queue implements it.
type Source interface {
	Counts(ctx context.Context) (map[string]int64, error)
	QueueDepth(ctx context.Context) (int64, error)
	OldestPendingAge(ctx context.Context) (time.Duration, error)
	WorkerStates(ctx context.Context) ([]queue.WorkerState, error)
}

// maxWorkerSeries bounds per-worker label cardinality. Beyond this the aggregate series still count everyone.
const maxWorkerSeries = 200

// Metrics holds every series the coordinator records.
type Metrics struct {
	reg *prometheus.Registry

	submitted     prometheus.Counter
	completions   *prometheus.CounterVec
	requeues      *prometheus.CounterVec
	workersLost   prometheus.Counter
	queueWait     prometheus.Histogram
	runtime       prometheus.Histogram
	schedLatency  prometheus.Histogram
	schedOutcomes *prometheus.CounterVec
	overloads     *prometheus.CounterVec
	killed        *prometheus.CounterVec
	rpcs          *prometheus.CounterVec
	rpcDuration   *prometheus.HistogramVec

	mu         sync.Mutex
	wasOverlod map[string]bool
}

// New registers all series and the scrape-time cluster collector over src.
func New(src Source) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg, wasOverlod: map[string]bool{}}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "computehive", Name: name, Help: help})
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "computehive", Name: name, Help: help}, labels)
	}
	// Job durations run from milliseconds to hours; scheduling decisions are sub-millisecond to seconds.
	jobBuckets := []float64{.1, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}
	latBuckets := []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}

	m.submitted = counter("jobs_submitted_total", "Jobs accepted by SubmitJob.")
	m.completions = counterVec("job_completions_total", "Committed job results by final status and failure reason.", "status", "reason")
	m.requeues = counterVec("job_requeues_total", "Jobs put back in the queue, by why.", "reason")
	m.workersLost = counter("workers_lost_total", "Workers declared dead because their heartbeats stopped.")
	m.queueWait = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "computehive", Name: "job_time_in_queue_seconds",
		Help: "How long a job waited before being assigned to a worker.", Buckets: jobBuckets})
	m.runtime = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "computehive", Name: "job_runtime_seconds",
		Help: "Wall-clock time from assignment to a committed result.", Buckets: jobBuckets})
	m.schedLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "computehive", Name: "scheduling_decision_seconds",
		Help: "Time the coordinator spent answering a worker's RequestJob (snapshot, scoring, atomic claim).", Buckets: latBuckets})
	m.schedOutcomes = counterVec("scheduling_requests_total", "RequestJob outcomes: assigned, or none (queue empty, or the job is held for a better-fit worker).", "outcome")
	m.overloads = counterVec("worker_overload_events_total", "Times a worker crossed into the overloaded state (the 90% rule).", "worker")
	m.killed = counterVec("jobs_killed_total", "Jobs that ended by signal or were evicted. kill = exit code 137/143 (OOM, disk or PID quota, timeout kill); evicted = the host was overloaded.", "kind")
	m.rpcs = counterVec("grpc_requests_total", "gRPC calls by method and status code.", "method", "code")
	m.rpcDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "computehive", Name: "grpc_request_duration_seconds",
		Help: "gRPC handler latency by method.", Buckets: latBuckets}, []string{"method"})

	reg.MustRegister(m.submitted, m.completions, m.requeues, m.workersLost, m.queueWait, m.runtime, m.schedLatency,
		m.schedOutcomes, m.overloads, m.killed, m.rpcs, m.rpcDuration,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if src != nil {
		reg.MustRegister(&clusterCollector{src: src})
	}
	return m
}

// Handler serves the registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

func (m *Metrics) JobSubmitted() {
	if m != nil {
		m.submitted.Inc()
	}
}

// JobAssigned records a successful placement, how long the job waited for it, and how long the decision took.
func (m *Metrics) JobAssigned(waited, decision time.Duration) {
	if m == nil {
		return
	}
	m.queueWait.Observe(waited.Seconds())
	m.schedLatency.Observe(decision.Seconds())
	m.schedOutcomes.WithLabelValues("assigned").Inc()
}

// NoJob records a RequestJob that handed nothing out.
func (m *Metrics) NoJob(decision time.Duration) {
	if m == nil {
		return
	}
	m.schedLatency.Observe(decision.Seconds())
	m.schedOutcomes.WithLabelValues("none").Inc()
}

// JobCompleted records a committed result. runtime <= 0 means unknown.
func (m *Metrics) JobCompleted(status, reason string, exitCode int32, runtime time.Duration) {
	if m == nil {
		return
	}
	m.completions.WithLabelValues(status, orNone(reason)).Inc()
	if runtime > 0 {
		m.runtime.Observe(runtime.Seconds())
	}
	if exitCode == 137 || exitCode == 143 {
		m.killed.WithLabelValues("kill").Inc()
	}
}

// JobRequeued records jobs put back in the queue ("worker_dead", "lease_expired", "infra", "evicted").
func (m *Metrics) JobRequeued(reason string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.requeues.WithLabelValues(reason).Add(float64(n))
	if reason == "evicted" {
		m.killed.WithLabelValues("evicted").Add(float64(n))
	}
}

func (m *Metrics) WorkersLost(n int) {
	if m != nil && n > 0 {
		m.workersLost.Add(float64(n))
	}
}

// WorkerOverloaded counts a worker's transition into overload (not every heartbeat while it stays there).
// The previous state is kept in memory, so a coordinator restart can count one extra transition.
func (m *Metrics) WorkerOverloaded(worker string, overloaded bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	was := m.wasOverlod[worker]
	m.wasOverlod[worker] = overloaded
	if len(m.wasOverlod) > 10*maxWorkerSeries { // never grow without bound
		m.wasOverlod = map[string]bool{worker: overloaded}
	}
	m.mu.Unlock()
	if overloaded && !was {
		m.overloads.WithLabelValues(worker).Inc()
	}
}

// RPC records one gRPC call.
func (m *Metrics) RPC(method, code string, d time.Duration) {
	if m == nil {
		return
	}
	m.rpcs.WithLabelValues(method, code).Inc()
	m.rpcDuration.WithLabelValues(method).Observe(d.Seconds())
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// clusterCollector reads Redis at scrape time.
type clusterCollector struct{ src Source }

var (
	dJobs      = prometheus.NewDesc("computehive_jobs", "Jobs currently in each state.", []string{"state"}, nil)
	dQueue     = prometheus.NewDesc("computehive_queue_depth", "Jobs waiting for a worker (pending + requeued).", nil, nil)
	dOldest    = prometheus.NewDesc("computehive_oldest_pending_age_seconds", "Age of the job at the head of the queue; a large value with idle workers means the queue is stuck.", nil, nil)
	dWorkers   = prometheus.NewDesc("computehive_workers", "Registered workers by state (alive, dead, overloaded, paused).", []string{"state"}, nil)
	dRedisUp   = prometheus.NewDesc("computehive_redis_up", "1 if Redis answered the scrape.", nil, nil)
	dRAM       = prometheus.NewDesc("computehive_worker_ram_used_percent", "Worker RAM used, percent.", []string{"worker"}, nil)
	dCPU       = prometheus.NewDesc("computehive_worker_cpu_used_percent", "Worker CPU used, percent.", []string{"worker"}, nil)
	dDisk      = prometheus.NewDesc("computehive_worker_disk_used_percent", "Worker disk used (the worse of the work and Docker disks), percent.", []string{"worker"}, nil)
	dOverload  = prometheus.NewDesc("computehive_worker_overloaded", "1 if the worker is currently overloaded (90% rule).", []string{"worker"}, nil)
	dRunning   = prometheus.NewDesc("computehive_worker_running_jobs", "Jobs the worker is running.", []string{"worker"}, nil)
	dHeartbeat = prometheus.NewDesc("computehive_worker_heartbeat_age_seconds", "Seconds since the worker's last heartbeat (heartbeat lag).", []string{"worker"}, nil)
	dReliable  = prometheus.NewDesc("computehive_worker_reliability", "Decayed success rate of the worker's recent jobs (0-1).", []string{"worker"}, nil)
)

func (c *clusterCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{dJobs, dQueue, dOldest, dWorkers, dRedisUp, dRAM, dCPU, dDisk, dOverload, dRunning, dHeartbeat, dReliable} {
		ch <- d
	}
}

func (c *clusterCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	g := func(d *prometheus.Desc, v float64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
	}

	counts, err := c.src.Counts(ctx)
	if err != nil {
		g(dRedisUp, 0)
		return
	}
	g(dRedisUp, 1)
	for state, n := range counts {
		g(dJobs, float64(n), state)
	}
	if depth, err := c.src.QueueDepth(ctx); err == nil {
		g(dQueue, float64(depth))
	}
	if age, err := c.src.OldestPendingAge(ctx); err == nil {
		g(dOldest, age.Seconds())
	}
	ws, err := c.src.WorkerStates(ctx)
	if err != nil {
		return
	}
	var alive, overloaded, paused int
	for i, w := range ws {
		if w.Alive {
			alive++
			if w.Overloaded {
				overloaded++
			}
			if w.Draining {
				paused++
			}
		}
		if i >= maxWorkerSeries {
			continue
		}
		g(dRAM, w.MemPct, w.ID)
		g(dCPU, w.CPUPct, w.ID)
		g(dDisk, w.DiskPct, w.ID)
		g(dRunning, float64(len(w.Running)), w.ID)
		g(dHeartbeat, float64(w.HeartbeatAgeMs)/1000, w.ID)
		g(dReliable, w.Reliability, w.ID)
		g(dOverload, boolFloat(w.Overloaded), w.ID)
	}
	g(dWorkers, float64(alive), "alive")
	g(dWorkers, float64(len(ws)-alive), "dead")
	g(dWorkers, float64(overloaded), "overloaded")
	g(dWorkers, float64(paused), "paused")
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
