// Package queue is the coordinator's durable job queue and worker registry.
// All state lives in Redis; every transition is one atomic Lua script (scripts.go),
// so the coordinator itself is stateless and can crash, restart or run as several
// replicas without losing or duplicating jobs.
//
// Job states: PENDING -> ASSIGNED -> RUNNING -> SUCCEEDED | FAILED, with
// REQUEUED (a failed attempt waiting under its original FIFO position, after
// backoff), DEAD (max attempts exhausted) and CANCELED.
package queue

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Job states.
const (
	StatePending   = "PENDING"
	StateAssigned  = "ASSIGNED"
	StateRunning   = "RUNNING"
	StateSucceeded = "SUCCEEDED"
	StateFailed    = "FAILED"
	StateRequeued  = "REQUEUED"
	StateDead      = "DEAD"
	StateCanceled  = "CANCELED"
	// StateVerifying: the job ran and its result is held back until its verification replica agrees.
	StateVerifying = "VERIFYING"
)

// AllStates lists every state, for metrics and tests.
var AllStates = []string{StatePending, StateAssigned, StateRunning, StateSucceeded, StateFailed, StateRequeued, StateDead, StateCanceled, StateVerifying}

// Disposition is the outcome of a state-changing call.
type Disposition string

const (
	DispOK        Disposition = "ok"
	DispDuplicate Disposition = "duplicate" // job already terminal: idempotent no-op
	DispStale     Disposition = "stale"     // lease superseded: discarded
	DispUnknown   Disposition = "unknown"   // no such job
)

// Failure reasons carried in results.
const (
	ReasonJobExit = "job_exit" // the job's own non-zero exit: terminal
	ReasonTimeout = "timeout"  // job exceeded its runtime: terminal
	ReasonInfra   = "infra"    // worker-side problem: retried
	ReasonEvicted = "evicted"  // worker shed load: retried, attempt not burned
)

// Config tunes failure detection and retries.
type Config struct {
	MaxAttempts    int           // default attempts per job
	BackoffBase    time.Duration // first retry delay; doubles per attempt
	BackoffMax     time.Duration
	DeadAfter      time.Duration // heartbeat silence after which a worker is dead
	ForgetAfter    time.Duration // dead workers are removed from the registry after this long
	LeaseGrace     time.Duration // added to a job's runtime limit to form its lease deadline
	DefaultTimeout time.Duration // lease basis for jobs that declare no timeout
	ClaimScan      int           // how many queue-head jobs a claim considers
	ReapInterval   time.Duration
	// VerifyTimeout bounds how long a replica waits in VERIFYING for its sibling (default 10m).
	VerifyTimeout time.Duration
	// UserLimit caps how many jobs one user may have running at once (0 = unlimited). It keeps one
	// submitter's flood from occupying the whole cluster.
	UserLimit int
	// OrphanGrace is how long a lease may go unreported by its worker before the job
	// is taken back (a lost claim reply). It must exceed the worker's heartbeat interval
	// plus the time a worker takes to register a freshly received job.
	OrphanGrace time.Duration

	// Resource-safety rules enforced on the coordinator side (percentages of host capacity).
	LimitMemPct     float64       // no assignment while RAM used >= this (default 90)
	LimitCPUPct     float64       // ... CPU
	LimitDiskPct    float64       // ... disk
	ReleasePct      float64       // an overloaded worker recovers only below this on every metric (default 80)...
	ReleaseAfter    time.Duration // ...continuously for this long
	RequireUsage    bool          // workers that report no metrics get no jobs
	DefaultJobMemMB int           // memory assumed for jobs that declare none
}

// DefaultConfig: with 2s heartbeats, a worker is declared dead after 6s and the
// reaper runs every 1s, so a killed worker's jobs are requeued within about 7s.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:    3,
		BackoffBase:    time.Second,
		BackoffMax:     30 * time.Second,
		DeadAfter:      6 * time.Second,
		ForgetAfter:    time.Hour,
		LeaseGrace:     30 * time.Second,
		DefaultTimeout: 30 * time.Minute,
		ClaimScan:      100,
		ReapInterval:   time.Second,
		OrphanGrace:    10 * time.Second,
		VerifyTimeout:  10 * time.Minute,

		LimitMemPct:     90,
		LimitCPUPct:     90,
		LimitDiskPct:    90,
		ReleasePct:      80,
		ReleaseAfter:    10 * time.Second,
		DefaultJobMemMB: 512,
	}
}

// Validate reports unusable settings.
func (c Config) Validate() error {
	switch {
	case c.MaxAttempts < 1:
		return errors.New("max attempts must be >= 1")
	case c.DeadAfter <= 0:
		return errors.New("worker dead-after must be > 0")
	case c.ReapInterval <= 0:
		return errors.New("reap interval must be > 0")
	case c.BackoffBase < 0 || c.BackoffMax < c.BackoffBase:
		return errors.New("backoff max must be >= backoff base >= 0")
	case c.ClaimScan < 1:
		return errors.New("claim scan must be >= 1")
	case c.VerifyTimeout < 0:
		return errors.New("verify timeout must be >= 0")
	case c.OrphanGrace <= 0:
		return errors.New("orphan grace must be > 0")
	case c.LimitMemPct <= 0 || c.LimitMemPct > 100 || c.LimitCPUPct <= 0 || c.LimitCPUPct > 100 || c.LimitDiskPct <= 0 || c.LimitDiskPct > 100:
		return errors.New("resource limits must be in (0, 100]")
	case c.ReleasePct <= 0 || c.ReleasePct >= min(c.LimitMemPct, c.LimitCPUPct, c.LimitDiskPct):
		return errors.New("release percentage must be positive and below every limit (hysteresis)")
	case c.ReleaseAfter < 0:
		return errors.New("release-after must be >= 0")
	}
	return nil
}

// Spec is the immutable description of a job, stored as JSON in the job hash.
type Spec struct {
	ID                string            `json:"id"`
	ContainerImage    string            `json:"container_image"`
	Command           []string          `json:"command,omitempty"`
	CPUCores          int32             `json:"cpu_cores,omitempty"`
	MemoryMB          int32             `json:"memory_mb,omitempty"`
	GPUCount          int32             `json:"gpu_count,omitempty"`
	Environment       map[string]string `json:"environment,omitempty"`
	MaxRuntimeSeconds int32             `json:"max_runtime_seconds,omitempty"`
	ArtifactURL       string            `json:"artifact_url,omitempty"`
	ArtifactSHA256    string            `json:"artifact_sha256,omitempty"`
	User              string            `json:"user,omitempty"`
	Verify            bool              `json:"verify,omitempty"`
	RequiredIsolation string            `json:"required_isolation,omitempty"`
	NetworkHosts      []string          `json:"network_hosts,omitempty"`
	MinDiskMB         int32             `json:"min_disk_mb,omitempty"`
	OS                string            `json:"os,omitempty"`
	Arch              string            `json:"arch,omitempty"`
	Labels            []string          `json:"labels,omitempty"`
	CreatedAtUnix     int64             `json:"created_at_unix"`
}

// OutputArtifact describes one uploaded output file.
type OutputArtifact struct {
	RelativePath string `json:"relative_path,omitempty"`
	ObjectKey    string `json:"object_key,omitempty"`
	URI          string `json:"uri,omitempty"`
	APIURL       string `json:"api_url,omitempty"`
	PublicURL    string `json:"public_url,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
	SizeBytes    int64  `json:"size_bytes,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
}

// Result is the committed outcome of a job.
type Result struct {
	JobID           string           `json:"job_id"`
	WorkerID        string           `json:"worker_id"`
	Attempt         int              `json:"attempt"`
	Status          string           `json:"status"`
	ExitCode        int32            `json:"exit_code"`
	FailureReason   string           `json:"failure_reason,omitempty"`
	StdoutExcerpt   string           `json:"stdout_excerpt,omitempty"`
	StderrExcerpt   string           `json:"stderr_excerpt,omitempty"`
	OutputURI       string           `json:"output_uri,omitempty"`
	OutputAPIURL    string           `json:"output_api_url,omitempty"`
	OutputPublicURL string           `json:"output_public_url,omitempty"`
	OutputSizeBytes int64            `json:"output_size_bytes,omitempty"`
	OutputDigest    string           `json:"output_digest,omitempty"`
	OutputArtifacts []OutputArtifact `json:"output_artifacts,omitempty"`
	FinishedAtUnix  int64            `json:"finished_at_unix,omitempty"`
}

// Queue is the Redis-backed queue.
type Queue struct {
	rdb *redis.Client
	cfg Config
}

// NewRedisClient connects to Redis and verifies it answers.
func NewRedisClient(ctx context.Context, addr, password string, db int, useTLS bool) (*redis.Client, error) {
	opts := &redis.Options{Addr: addr, Password: password, DB: db}
	if useTLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}
	return c, nil
}

// New returns a Queue over rdb.
func New(rdb *redis.Client, cfg Config) *Queue { return &Queue{rdb: rdb, cfg: cfg} }

// Config returns the effective configuration.
func (q *Queue) Config() Config { return q.cfg }

// Redis returns the underlying client (readiness probes, tests).
func (q *Queue) Redis() *redis.Client { return q.rdb }

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err)) // not recoverable
	}
	return hex.EncodeToString(b[:])
}

func boolArg(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func asStrings(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected script reply %T", v)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		switch t := e.(type) {
		case string:
			out[i] = t
		case int64:
			out[i] = strconv.FormatInt(t, 10)
		default:
			out[i] = fmt.Sprint(t)
		}
	}
	return out, nil
}

func disposition(s string) Disposition {
	switch s {
	case "OK":
		return DispOK
	case "DUP":
		return DispDuplicate
	case "STALE":
		return DispStale
	default:
		return DispUnknown
	}
}

// Enqueue adds a job at the tail of the FIFO. Re-enqueueing an existing ID is a
// no-op that returns the original sequence number (created=false).
// enqueueBlock is the 17 script arguments describing one job.
func (q *Queue) enqueueBlock(sp Spec, maxAttempts int, verify bool) ([]any, error) {
	payload, err := json.Marshal(sp)
	if err != nil {
		return nil, fmt.Errorf("marshal job spec: %w", err)
	}
	return []any{sp.ID, maxAttempts, sp.CPUCores, sp.MemoryMB, sp.GPUCount, sp.MaxRuntimeSeconds,
		q.cfg.BackoffBase.Milliseconds(), q.cfg.BackoffMax.Milliseconds(), sp.User, boolArg(verify), payload,
		sp.RequiredIsolation, boolArg(len(sp.NetworkHosts) > 0),
		sp.MinDiskMB, sp.OS, sp.Arch, labelsKey(sp.Labels)}, nil
}

// labelsKey renders labels as ",k=v,k2=v2," for the claim script's substring matching.
func labelsKey(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return strings.Join(labels, ",")
}

func (q *Queue) Enqueue(ctx context.Context, spec Spec, maxAttempts int) (seq int64, created bool, err error) {
	if spec.ID == "" {
		return 0, false, errors.New("job id is required")
	}
	if maxAttempts <= 0 {
		maxAttempts = q.cfg.MaxAttempts
	}
	args, err := q.enqueueBlock(spec, maxAttempts, spec.Verify)
	if err != nil {
		return 0, false, err
	}
	res, err := enqueueScript.Run(ctx, q.rdb, nil, args...).Result()
	if err != nil {
		return 0, false, fmt.Errorf("enqueue %s: %w", spec.ID, err)
	}
	out, err := asStrings(res)
	if err != nil || len(out) != 2 {
		return 0, false, fmt.Errorf("enqueue %s: bad reply %v", spec.ID, res)
	}
	seq, _ = strconv.ParseInt(out[1], 10, 64)
	return seq, out[0] == "OK", nil
}

// ShadowID is the id of the verification replica of job id.
func ShadowID(id string) string { return id + "~v" }

// EnqueuePair adds a job and its verification replica atomically. The replica has the same spec
// under ShadowID(spec.ID) and is never run on the same worker as the primary.
func (q *Queue) EnqueuePair(ctx context.Context, spec Spec, maxAttempts int) (seq int64, created bool, err error) {
	if spec.ID == "" {
		return 0, false, errors.New("job id is required")
	}
	if maxAttempts <= 0 {
		maxAttempts = q.cfg.MaxAttempts
	}
	spec.Verify = true
	shadow := spec
	shadow.ID = ShadowID(spec.ID)
	args := []any{}
	for _, sp := range []Spec{spec, shadow} {
		block, err := q.enqueueBlock(sp, maxAttempts, true)
		if err != nil {
			return 0, false, err
		}
		args = append(args, block...)
	}
	res, err := enqueuePairScript.Run(ctx, q.rdb, nil, args...).Result()
	if err != nil {
		return 0, false, fmt.Errorf("enqueue verified pair %s: %w", spec.ID, err)
	}
	out, err := asStrings(res)
	if err != nil || len(out) != 2 {
		return 0, false, fmt.Errorf("enqueue verified pair %s: bad reply %v", spec.ID, res)
	}
	seq, _ = strconv.ParseInt(out[1], 10, 64)
	return seq, out[0] == "OK", nil
}

// ReapVerifyTimeouts fails replicas that waited longer than VerifyTimeout for their sibling.
func (q *Queue) ReapVerifyTimeouts(ctx context.Context) (int, error) {
	n, err := reapVerifyScript.Run(ctx, q.rdb, nil, q.cfg.VerifyTimeout.Milliseconds(), 100).Int()
	if err != nil {
		return 0, fmt.Errorf("reap verification timeouts: %w", err)
	}
	return n, nil
}

// Claimed is a job assigned to a worker under a fresh lease token.
type Claimed struct {
	JobID   string
	Attempt int
	Token   string
	Spec    Spec
}

// Claim atomically moves the lowest-sequence eligible pending job to ASSIGNED for
// workerID. It returns (nil, reason, nil) when nothing was assigned; reason says
// why (empty queue, worker overloaded, stale heartbeat, ...).
func (q *Queue) Claim(ctx context.Context, workerID string) (*Claimed, string, error) {
	return q.claim(ctx, workerID, "")
}

// ClaimJob is Claim restricted to one specific pending job, used by the scheduler once it has decided
// which worker should run which job. The same atomic safety checks apply.
func (q *Queue) ClaimJob(ctx context.Context, workerID, jobID string) (*Claimed, string, error) {
	return q.claim(ctx, workerID, jobID)
}

func (q *Queue) claim(ctx context.Context, workerID, specific string) (*Claimed, string, error) {
	res, err := claimScript.Run(ctx, q.rdb, nil, workerID, newToken(),
		q.cfg.DeadAfter.Milliseconds(), q.cfg.DefaultTimeout.Milliseconds(), q.cfg.LeaseGrace.Milliseconds(), q.cfg.ClaimScan,
		q.cfg.LimitMemPct, q.cfg.DefaultJobMemMB, boolArg(q.cfg.RequireUsage), specific, q.cfg.UserLimit).Result()
	if err != nil {
		return nil, "", fmt.Errorf("claim for %s: %w", workerID, err)
	}
	out, err := asStrings(res)
	if err != nil || len(out) == 0 {
		return nil, "", fmt.Errorf("claim for %s: bad reply %v", workerID, res)
	}
	switch out[0] {
	case "NONE":
		return nil, "empty", nil
	case "REJECT":
		return nil, out[1], nil
	}
	attempt, _ := strconv.Atoi(out[2])
	var spec Spec
	if err := json.Unmarshal([]byte(out[3]), &spec); err != nil {
		return nil, "", fmt.Errorf("claim %s: corrupt job spec: %w", out[1], err)
	}
	return &Claimed{JobID: out[1], Attempt: attempt, Token: out[4], Spec: spec}, "", nil
}

func (q *Queue) runDisp(ctx context.Context, s *redis.Script, args ...any) (Disposition, string, error) {
	res, err := s.Run(ctx, q.rdb, nil, args...).Result()
	if err != nil {
		return "", "", err
	}
	str, _ := res.(string)
	head, tail, _ := strings.Cut(str, ":")
	return disposition(head), tail, nil
}

// MarkRunning records that the worker started executing the attempt (idempotent).
func (q *Queue) MarkRunning(ctx context.Context, jobID, token string) (Disposition, error) {
	d, _, err := q.runDisp(ctx, markRunningScript, jobID, token)
	return d, err
}

// Complete commits the attempt's result. Only the holder of the current lease
// token can commit, and only once: a late or duplicate submission is reported as
// stale/duplicate and changes nothing.
func (q *Queue) Complete(ctx context.Context, jobID, token string, succeeded bool, res Result) (Disposition, error) {
	outcome := StateFailed
	if succeeded {
		outcome = StateSucceeded
	}
	payload, err := json.Marshal(res)
	if err != nil {
		return "", fmt.Errorf("marshal result: %w", err)
	}
	d, _, err := q.runDisp(ctx, completeScript, jobID, token, outcome, payload, res.OutputDigest)
	return d, err
}

// Fail ends the current attempt without committing a result and requeues the job
// (or parks it as DEAD when attempts are exhausted). countAttempt=false is used
// for evictions, which are not the job's fault. Returns the job's new state.
func (q *Queue) Fail(ctx context.Context, jobID, token, reason string, countAttempt bool) (Disposition, string, error) {
	return q.runDisp(ctx, failScript, jobID, token, reason, boolArg(countAttempt))
}

// Cancel moves a non-terminal job to CANCELED. A worker running it learns on its next heartbeat.
func (q *Queue) Cancel(ctx context.Context, jobID string) (Disposition, error) {
	d, _, err := q.runDisp(ctx, cancelScript, jobID)
	return d, err
}

// Revive gives a DEAD job a fresh attempt budget, keeping its queue position.
func (q *Queue) Revive(ctx context.Context, jobID string) (bool, error) {
	res, err := reviveScript.Run(ctx, q.rdb, nil, jobID).Result()
	if err != nil {
		return false, err
	}
	return res == "OK", nil
}

// WorkerReport is the state a worker reports in each heartbeat.
type WorkerReport struct {
	ID         string
	CPU        int
	MemMB      int
	GPU        int
	Overloaded bool
	Draining   bool
	MaxJobs    int
	Version    string
	Isolation  string // "hardened-runc" or "gvisor"
	AllowsNet  bool   // the contributor's policy permits network jobs

	// Scheduling facts.
	OS, Arch        string
	Labels          []string
	Preference      int // 0-100; 0 is treated as the default 50
	DiskFreeMB      int64
	Load1           float64
	CachedArtifacts []string
	Running         []string // jobs the worker believes it is running

	// Usage is the worker's latest resource sample; nil if it sent none.
	Usage *Usage
}

// Usage is the part of a worker's resource sample the scheduler acts on.
type Usage struct {
	MemTotalMB, MemUsedMB, JobsMemMB int64
	MemPct, CPUPct, DiskPct          float64 // DiskPct is the worse of the work-dir and Docker-root disks
	JSON                             string  // full sample, stored verbatim for display
}

// Heartbeat records worker liveness and capacity. It returns the reported jobs the
// worker no longer owns (requeued, cancelled, or finished); the worker must abort them.
func (q *Queue) Heartbeat(ctx context.Context, r WorkerReport) ([]string, error) {
	if r.MaxJobs < 1 {
		r.MaxJobs = 1
	}
	u := r.Usage
	hasUsage := "0"
	if u == nil {
		u = &Usage{}
	} else {
		hasUsage = "1"
	}
	args := []any{r.ID, r.CPU, r.MemMB, r.GPU, boolArg(r.Overloaded), boolArg(r.Draining), r.MaxJobs, r.Version, q.cfg.OrphanGrace.Milliseconds(),
		hasUsage, u.MemTotalMB, u.MemUsedMB, u.JobsMemMB, u.MemPct, u.CPUPct, u.DiskPct,
		q.cfg.LimitMemPct, q.cfg.LimitCPUPct, q.cfg.LimitDiskPct, q.cfg.ReleasePct, q.cfg.ReleaseAfter.Milliseconds(), u.JSON, r.Isolation, boolArg(r.AllowsNet), capsJSON(r)}
	for _, id := range r.Running {
		args = append(args, id)
	}
	res, err := heartbeatScript.Run(ctx, q.rdb, nil, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("heartbeat %s: %w", r.ID, err)
	}
	return asStrings(res)
}

func capsJSON(r WorkerReport) string {
	pref := r.Preference
	if pref <= 0 || pref > 100 {
		pref = 50
	}
	cached := r.CachedArtifacts
	if len(cached) > 200 {
		cached = cached[:200]
	}
	labels := r.Labels
	if labels == nil {
		labels = []string{}
	}
	if cached == nil {
		cached = []string{}
	}
	b, _ := json.Marshal(map[string]any{"os": r.OS, "arch": r.Arch, "labels": labels, "preference": pref,
		"disk_free_mb": r.DiskFreeMB, "load1": r.Load1, "cached": cached})
	return string(b)
}

// WorkerKnown reports whether the worker has ever heartbeated (and not been forgotten).
func (q *Queue) WorkerKnown(ctx context.Context, workerID string) (bool, error) {
	_, err := q.rdb.ZScore(ctx, "ch:workers", workerID).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return err == nil, err
}

// ReapDeadWorkers requeues jobs leased to workers silent for longer than DeadAfter.
func (q *Queue) ReapDeadWorkers(ctx context.Context) (jobs, workers int, err error) {
	res, err := reapWorkersScript.Run(ctx, q.rdb, nil, q.cfg.DeadAfter.Milliseconds(), q.cfg.ForgetAfter.Milliseconds()).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("reap dead workers: %w", err)
	}
	out, err := asStrings(res)
	if err != nil || len(out) != 2 {
		return 0, 0, fmt.Errorf("reap dead workers: bad reply %v", res)
	}
	jobs, _ = strconv.Atoi(out[0])
	workers, _ = strconv.Atoi(out[1])
	return jobs, workers, nil
}

// ReleaseWorker marks a worker dead at once and requeues its jobs without counting an attempt.
// It returns how many jobs were requeued. Used when a worker is revoked.
func (q *Queue) ReleaseWorker(ctx context.Context, workerID string) (int, error) {
	n, err := releaseWorkerScript.Run(ctx, q.rdb, nil, workerID).Int()
	if err != nil {
		return 0, fmt.Errorf("release worker %s: %w", workerID, err)
	}
	return n, nil
}

// ReapExpiredLeases requeues jobs that outlived their lease deadline.
func (q *Queue) ReapExpiredLeases(ctx context.Context) (int, error) {
	res, err := reapLeasesScript.Run(ctx, q.rdb, nil, 100).Int()
	if err != nil {
		return 0, fmt.Errorf("reap expired leases: %w", err)
	}
	return res, nil
}

// RunReaper runs failure detection until ctx is done. Because every step is an
// idempotent script over Redis state, several coordinators can run it concurrently,
// and a freshly started coordinator recovers in-flight jobs on its first tick.
// ReaperHooks lets the caller observe what the reaper does (for metrics). Any field may be nil.
type ReaperHooks struct {
	Requeued    func(reason string, jobs int) // reason is "worker_dead" or "lease_expired"
	WorkersLost func(workers int)
}

func (q *Queue) RunReaper(ctx context.Context, log *slog.Logger, hooks *ReaperHooks) {
	if hooks == nil {
		hooks = &ReaperHooks{}
	}
	t := time.NewTicker(q.cfg.ReapInterval)
	defer t.Stop()
	for {
		tick := func() {
			jobs, workers, err := q.ReapDeadWorkers(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("reap dead workers failed", "error", err)
				}
				return
			}
			if jobs > 0 {
				log.Warn("requeued jobs from dead workers", "jobs", jobs, "workers", workers)
				if hooks.Requeued != nil {
					hooks.Requeued("worker_dead", jobs)
				}
			}
			if workers > 0 && hooks.WorkersLost != nil {
				hooks.WorkersLost(workers)
			}
			if n, err := q.ReapVerifyTimeouts(ctx); err != nil {
				if ctx.Err() == nil {
					log.Error("reap verification timeouts failed", "error", err)
				}
			} else if n > 0 {
				log.Warn("verification timed out: the replica never finished", "jobs", n)
			}
			n, err := q.ReapExpiredLeases(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("reap expired leases failed", "error", err)
				}
				return
			}
			if n > 0 {
				log.Warn("requeued jobs with expired leases", "jobs", n)
				if hooks.Requeued != nil {
					hooks.Requeued("lease_expired", n)
				}
			}
		}
		tick()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Job is a point-in-time view of a job.
type Job struct {
	ID            string
	State         string
	Seq           int64
	Attempt       int
	MaxAttempts   int
	Worker        string
	LastWorker    string // the worker the job was most recently assigned to; kept after the job finishes
	LastFailure   string
	LastFailureMs int64
	CreatedMs     int64
	AssignedMs    int64
	StartedMs     int64
	FinishedMs    int64
	NotBeforeMs   int64
	// Commits is how many results were ever committed for this job. The Lua scripts
	// guarantee it is 0 until the job finishes and exactly 1 afterwards.
	Commits int
	// Exclude is the worker this job must not run on (its verification sibling's worker).
	Exclude string
	// Verification is "" for ordinary jobs; for verified jobs "pending", "match", "mismatch",
	// "timeout" or "unavailable".
	Verification string
	Spec         Spec
}

func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

// Get returns the job, or (nil, nil) if it does not exist.
func (q *Queue) Get(ctx context.Context, jobID string) (*Job, error) {
	m, err := q.rdb.HGetAll(ctx, "ch:job:"+jobID).Result()
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", jobID, err)
	}
	if len(m) == 0 {
		return nil, nil
	}
	return jobFromMap(m), nil
}

// GetResult returns the committed result, or (nil, nil) if none.
func (q *Queue) GetResult(ctx context.Context, jobID string) (*Result, error) {
	raw, err := q.rdb.Get(ctx, "ch:result:"+jobID).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get result %s: %w", jobID, err)
	}
	var r Result
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, fmt.Errorf("decode result %s: %w", jobID, err)
	}
	return &r, nil
}

// Counts returns the number of jobs per state.
func (q *Queue) Counts(ctx context.Context) (map[string]int64, error) {
	m, err := q.rdb.HGetAll(ctx, "ch:counts").Result()
	if err != nil {
		return nil, fmt.Errorf("counts: %w", err)
	}
	out := make(map[string]int64, len(AllStates))
	for _, s := range AllStates {
		out[s] = atoi64(m[s])
	}
	return out, nil
}

// QueueDepth is the number of jobs waiting for a worker (PENDING + REQUEUED).
func (q *Queue) QueueDepth(ctx context.Context) (int64, error) {
	return q.rdb.ZCard(ctx, "ch:pending").Result()
}

// DeadJobs lists dead-letter job IDs, oldest first.
func (q *Queue) DeadJobs(ctx context.Context) ([]string, error) {
	return q.rdb.ZRange(ctx, "ch:dead", 0, -1).Result()
}

// WorkerInfo describes a registered worker.
type WorkerInfo struct {
	ID              string
	Alive           bool
	CPU, MemMB, GPU int
	Overloaded      bool
	Draining        bool
	MaxJobs         int
	Version         string
	RegisteredMs    int64
	LastHeartbeatMs int64
	Running         []string
	Isolation       string
	AllowsNetwork   bool
	OverloadReasons string // "ram,cpu" while the coordinator considers the host overloaded
	UsageJSON       string // the worker's last full resource sample
}

// ListWorkers returns all known workers, oldest registration first.
func (q *Queue) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	ids, err := q.rdb.ZRange(ctx, "ch:workers", 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	t, err := q.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis time: %w", err)
	}
	nowMs := t.UnixMilli()
	out := make([]WorkerInfo, 0, len(ids))
	for _, id := range ids {
		m, err := q.rdb.HGetAll(ctx, "ch:worker:"+id).Result()
		if err != nil {
			return nil, fmt.Errorf("worker %s: %w", id, err)
		}
		running, err := q.rdb.SMembers(ctx, "ch:wrun:"+id).Result()
		if err != nil {
			return nil, fmt.Errorf("worker %s running: %w", id, err)
		}
		last := atoi64(m["last_hb_ms"])
		out = append(out, WorkerInfo{
			ID: id, Alive: nowMs-last <= q.cfg.DeadAfter.Milliseconds(),
			CPU: int(atoi64(m["cpu"])), MemMB: int(atoi64(m["mem_mb"])), GPU: int(atoi64(m["gpu"])),
			Overloaded: m["overloaded"] == "1", Draining: m["draining"] == "1", MaxJobs: int(atoi64(m["max_jobs"])),
			Version: m["version"], RegisteredMs: atoi64(m["registered_ms"]), LastHeartbeatMs: last, Running: running,
			OverloadReasons: m["over_reasons"], UsageJSON: m["usage_json"], Isolation: m["isolation"], AllowsNetwork: m["allows_net"] == "1",
		})
	}
	return out, nil
}
