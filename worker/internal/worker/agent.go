package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/monitor"
	workeroutput "github.com/vim-all/ComputeHive/worker/internal/output"
)

// Failure reasons understood by the coordinator's retry policy.
const (
	ReasonJobExit = "job_exit" // the job's own failure: terminal
	ReasonTimeout = "timeout"  // the job ran past its limit: terminal
	ReasonInfra   = "infra"    // this worker could not run it: retried elsewhere
	ReasonEvicted = "evicted"  // this worker is giving the job back: retried, attempt not burned
)

var (
	errLeaseRevoked  = errors.New("coordinator revoked this worker's lease on the job")
	errPressureEvict = errors.New("host resource pressure: job evicted")
)

// healthMonitor is the resource monitor as the agent sees it.
type healthMonitor interface {
	Status() monitor.Status
	OverloadedFor() time.Duration
}

type storeAPI interface {
	PullJob(ctx context.Context) (*domain.Job, bool, error)
	PublishHeartbeat(ctx context.Context, heartbeat domain.WorkerHeartbeat) ([]string, error)
	ReportRunning(ctx context.Context, jobID string) error
	PublishJobResult(ctx context.Context, result domain.JobResult) (string, error)
	Forget(jobID string)
}

type fetcherAPI interface {
	Fetch(ctx context.Context, job domain.Job) (artifact.Bundle, error)
}

type reporterAPI interface {
	Snapshot(ctx context.Context) domain.ResourceSnapshot
}

type executorAPI interface {
	Run(ctx context.Context, job domain.Job, bundle artifact.Bundle) domain.JobResult
}

type outputManagerAPI interface {
	CollectAndUpload(ctx context.Context, jobID, outputDir string) (workeroutput.UploadResult, error)
}

type Agent struct {
	cfg      config.Config
	store    storeAPI
	fetcher  fetcherAPI
	reporter reporterAPI
	executor executorAPI
	outputs  outputManagerAPI
	health   healthMonitor
	logger   *slog.Logger

	// contributor controls (see controls.go); all optional
	pol       policyAPI
	hist      historyAPI
	sandbox   sandboxAPI
	isolation string
	stopRoot  context.CancelFunc
	killed    atomic.Bool
	draining  atomic.Bool // graceful shutdown: take no new job, finish the current one
	notTaking string
	started   time.Time

	mu           sync.RWMutex
	status       string
	currentJobID string
	abortCurrent context.CancelCauseFunc
	currentCPU   int
	currentMemMB int64
	jobsDone     int64
	jobsFailed   int64
	lastNoJobLog time.Time

	overloadLogged bool
	evictedForJob  string // job already evicted for pressure, so the policy fires once per job
}

func NewAgent(cfg config.Config, store storeAPI, fetcher fetcherAPI, reporter reporterAPI, executor executorAPI, outputs outputManagerAPI, health healthMonitor, logger *slog.Logger) *Agent {
	return &Agent{
		health:   health,
		cfg:      cfg,
		store:    store,
		fetcher:  fetcher,
		reporter: reporter,
		executor: executor,
		outputs:  outputs,
		logger:   logger,
		started:  time.Now().UTC(),
		status:   domain.WorkerStatusAvailable,
	}
}

// BeginDrain starts a graceful shutdown: the worker stops asking for jobs and reports itself draining so
// the coordinator stops assigning, but a job already running is left to finish. Run returns once the
// worker is idle. The caller decides how long to wait (it cancels Run's context to hand the job back).
func (a *Agent) BeginDrain() {
	if a.draining.CompareAndSwap(false, true) {
		a.logger.Info("draining: no new jobs; finishing the current one", "event", "drain_started", "job_id", a.currentJob())
	}
}

func (a *Agent) currentJob() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.currentJobID
}

func (a *Agent) Run(ctx context.Context) error {
	a.logger.Info("worker started", "coordinator_addr", a.cfg.CoordinatorAddr)

	go a.heartbeatLoop(ctx)
	go a.pressureLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Draining: this point is only reached between jobs, so any job in progress has finished.
		if a.draining.Load() {
			a.logger.Info("drained: no job in progress, exiting", "event", "drained")
			return nil
		}

		// Safety rule 1 (worker side): never ask for work while the host is overloaded.
		// The coordinator enforces the same rule independently from the heartbeat numbers.
		if st := a.health.Status(); st.Overloaded {
			a.logOverloadedOnce(st)
			if !sleepCtx(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		a.overloadLogged = false

		// Safety rule 2: the contributor's own rules (pause, schedule, idle-only, gVisor-only).
		if adm := a.admission(); !adm.Allowed {
			a.noteNotTaking(adm.Reason)
			if !sleepCtx(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		a.noteNotTaking("")

		job, found, err := a.store.PullJob(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.logger.Error("failed to pull job", "error", err)
			if !sleepCtx(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		if !found {
			now := time.Now().UTC()
			if a.lastNoJobLog.IsZero() || now.Sub(a.lastNoJobLog) >= 60*time.Second {
				a.logger.Info("no job available, continuing to poll")
				a.lastNoJobLog = now
			}
			if !sleepCtx(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		a.lastNoJobLog = time.Time{}
		a.logger.Info("job received", "job_id", job.ID, "attempt", job.Attempt, "image_ref", job.ImageRef)

		if err := a.handleJob(ctx, job); err != nil {
			a.logger.Error("job handling failed", "job_id", job.ID, "error", err)
		}
	}
}

func (a *Agent) logOverloadedOnce(st monitor.Status) {
	if !a.overloadLogged {
		a.overloadLogged = true
		a.logger.Warn("not requesting jobs: host overloaded", "event", "intake_paused", "reasons", strings.Join(st.Reasons, ","),
			"ram_percent", st.Metrics.MemPercent, "cpu_percent", st.Metrics.CPUPercent, "disk_percent", st.Metrics.DiskPercent())
	}
}

// pressureLoop is the over-pressure policy. Step 1 (stop accepting new work) is the gate in Run.
// Step 2: if the host is still over a limit after PressureGrace while a job is running, the
// job is evicted: stopped gracefully, reported as "evicted" so the coordinator requeues it
// elsewhere without counting an attempt against it, and logged as a clear event. With the
// default of one job per worker "lowest priority / most recent" is trivially that job.
func (a *Agent) pressureLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.cfg.PressureGrace <= 0 {
			continue
		}
		over := a.health.OverloadedFor()
		if over < a.cfg.PressureGrace {
			continue
		}
		a.mu.Lock()
		job, abort := a.currentJobID, a.abortCurrent
		already := job == "" || a.evictedForJob == job
		if !already {
			a.evictedForJob = job
		}
		a.mu.Unlock()
		if already || abort == nil {
			continue
		}
		st := a.health.Status()
		a.logger.Warn("evicting job: host stayed over its resource limit", "event", "job_evicted_pressure", "job_id", job,
			"overloaded_for", over.Round(time.Second).String(), "reasons", strings.Join(st.Reasons, ","),
			"ram_percent", st.Metrics.MemPercent, "cpu_percent", st.Metrics.CPUPercent, "disk_percent", st.Metrics.DiskPercent())
		abort(errPressureEvict)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()

	a.publishHeartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.publishHeartbeat(ctx)
		}
	}
}

func (a *Agent) publishHeartbeat(ctx context.Context) {
	heartbeatCtx, cancel := context.WithTimeout(ctx, a.cfg.HeartbeatInterval+2*time.Second)
	defer cancel()

	status, currentJob, cpuUsed, memoryUsed, jobsDone, jobsFailed := a.state()
	snapshot := a.reporter.Snapshot(heartbeatCtx)
	heartbeat := domain.WorkerHeartbeat{
		ID:     a.cfg.WorkerID,
		Status: status,
		Resources: domain.WorkerResources{
			CPUCores: snapshot.CPUCores,
			MemoryMB: snapshot.MemoryMB,
			GPU:      snapshot.GPU,
		},
		CurrentLoad: domain.WorkerCurrentLoad{CPUUsed: cpuUsed, MemoryUsed: memoryUsed},
		Capabilities: domain.WorkerCapabilities{
			Docker:       snapshot.DockerAvailable,
			GPUSupported: snapshot.GPUSupported,
		},
		LastHeartbeat: time.Now().UTC().Unix(),
		Stats:         domain.WorkerStats{JobsCompleted: jobsDone, JobsFailed: jobsFailed},
		MaxJobs:       1,
		Isolation:     a.isolation,
		AllowsNetwork: a.currentPolicy().AllowNetwork,
		OS:            a.cfg.OS,
		Arch:          a.cfg.Arch,
		Labels:        a.cfg.Labels,
		Preference:    a.cfg.Preference,
	}
	if c, ok := a.fetcher.(interface{ CachedDigests(limit int) []string }); ok {
		heartbeat.CachedArtifacts = c.CachedDigests(64)
	}
	heartbeat.Resources.CPUCores, heartbeat.Resources.MemoryMB = a.donated(heartbeat.Resources.CPUCores, heartbeat.Resources.MemoryMB)
	if adm := a.admission(); !adm.Allowed || a.draining.Load() {
		heartbeat.Draining = true // tell the coordinator too: it will refuse to assign us anything
	}
	if currentJob != "" {
		heartbeat.RunningJobs = []string{currentJob}
		// Live output for the job in progress, if the executor can provide it (the real Docker one can).
		if lt, ok := a.executor.(interface {
			LogTail(ctx context.Context, jobID string) string
		}); ok {
			if tail := lt.LogTail(heartbeatCtx, currentJob); tail != "" {
				heartbeat.LogTails = map[string]string{currentJob: tail}
			}
		}
	}
	if st := a.health.Status(); st.Metrics.At.IsZero() {
		heartbeat.Overloaded, heartbeat.OverloadReasons = true, st.Reasons // nothing sampled yet
	} else {
		mx := st.Metrics
		heartbeat.Usage, heartbeat.Overloaded, heartbeat.OverloadReasons = &mx, st.Overloaded, st.Reasons
	}

	cancelIDs, err := a.store.PublishHeartbeat(heartbeatCtx, heartbeat)
	if err != nil {
		if heartbeatCtx.Err() == nil {
			a.logger.Warn("failed to publish heartbeat", "error", err)
		}
		return
	}
	for _, id := range cancelIDs {
		a.revoke(id)
	}
}

// revoke aborts the running job if the coordinator says this worker no longer owns it
// (it was requeued after a missed-heartbeat window, cancelled, or finished elsewhere).
func (a *Agent) revoke(jobID string) {
	a.mu.RLock()
	current, abort := a.currentJobID, a.abortCurrent
	a.mu.RUnlock()
	if current == jobID && abort != nil {
		a.logger.Warn("aborting job: lease revoked by coordinator", "job_id", jobID)
		abort(errLeaseRevoked)
	}
}

func (a *Agent) handleJob(ctx context.Context, job *domain.Job) error {
	defer a.store.Forget(job.ID)
	job.Normalize()
	if err := job.Validate(); err != nil {
		return a.publishFailedJob(ctx, *job, time.Now().UTC(), err, ReasonJobExit)
	}

	pol := a.currentPolicy()
	if why := policyViolation(pol, *job); why != "" {
		a.record(*job, time.Now().UTC(), "refused", -1, ReasonEvicted, why)
		return a.publishFailedJob(ctx, *job, time.Now().UTC(), errors.New(why), ReasonEvicted)
	}
	a.applyPolicy(pol)

	startedAt := time.Now().UTC()
	jobCtx, abort := context.WithCancelCause(ctx)
	defer abort(nil)
	a.setState(domain.WorkerStatusBusy, job.ID, normalizeCPUUsed(job.CPUCores), job.MemoryMB, abort)
	defer a.setState(domain.WorkerStatusAvailable, "", 0, 0, nil)

	timeout := a.cfg.DefaultJobTimeout
	if job.TimeoutSeconds > 0 {
		timeout = time.Duration(job.TimeoutSeconds) * time.Second
	}
	runCtx, cancelRun := context.WithTimeout(jobCtx, timeout)
	defer cancelRun()
	a.logger.Info("starting job", "job_id", job.ID, "attempt", job.Attempt, "timeout", timeout.String())

	bundle := artifact.Bundle{}
	if strings.TrimSpace(job.ArtifactURL) != "" || strings.TrimSpace(job.ArtifactSHA256) != "" {
		job.ArtifactURL = artifact.ResolveURL(job.ArtifactURL, a.cfg.CoordinatorHTTPURL)
		var err error
		bundle, err = a.fetcher.Fetch(runCtx, *job)
		if err != nil {
			return a.finishAborted(ctx, jobCtx, runCtx, *job, startedAt, err)
		}
		a.logger.Info("artifact bundle ready", "job_id", job.ID, "size_bytes", bundle.SizeBytes)
		defer func() {
			if err := artifact.Cleanup(bundle); err != nil {
				a.logger.Warn("failed to cleanup artifact bundle", "job_id", job.ID, "error", err)
			}
		}()
	}

	if err := a.store.ReportRunning(runCtx, job.ID); err != nil && runCtx.Err() == nil {
		a.logger.Warn("could not report running state", "job_id", job.ID, "error", err)
	}

	result := a.executor.Run(runCtx, *job, bundle)
	if cleanupDir := strings.TrimSpace(result.CleanupDir); cleanupDir != "" {
		defer func() {
			if err := os.RemoveAll(cleanupDir); err != nil {
				a.logger.Warn("failed to cleanup job output directory", "job_id", job.ID, "path", cleanupDir, "error", err)
			}
		}()
	}

	// Never report on behalf of a lease we lost, or while shutting down.
	if jobCtx.Err() != nil {
		return a.finishAborted(ctx, jobCtx, runCtx, *job, startedAt, nil)
	}

	if strings.TrimSpace(result.Status) == "" {
		result.Status = domain.JobStatusFailed
	}
	if result.JobID == "" {
		result.JobID = job.ID
	}
	if result.WorkerID == "" {
		result.WorkerID = a.cfg.WorkerID
	}
	if result.Status != domain.JobStatusSucceeded && result.FailureReason == "" {
		switch {
		case errors.Is(runCtx.Err(), context.DeadlineExceeded):
			result.FailureReason = ReasonTimeout
		case result.ExitCode > 0:
			result.FailureReason = ReasonJobExit
		default:
			result.FailureReason = ReasonInfra
		}
	}

	if a.outputs != nil && result.Status == domain.JobStatusSucceeded {
		if outputDir := strings.TrimSpace(result.OutputDirHostPath); outputDir != "" {
			uploadResult, err := a.outputs.CollectAndUpload(runCtx, job.ID, outputDir)
			if err != nil {
				result.Error = combineErrors(result.Error, "collect job outputs: "+err.Error())
				result.Status = domain.JobStatusFailed
				result.FailureReason = ReasonInfra
				if result.ExitCode == 0 {
					result.ExitCode = -1
				}
			} else {
				result.OutputURI = uploadResult.ManifestURI
				result.OutputAPIURL = uploadResult.ManifestAPIURL
				result.OutputPublicURL = uploadResult.ManifestPublicURL
				result.OutputSizeBytes = uploadResult.TotalSizeBytes
				result.OutputArtifacts = uploadResult.Artifacts
			}
		}
	}

	if result.Status == domain.JobStatusSucceeded {
		// Fingerprint the output so the coordinator can compare the replicas of a verify=true job.
		result.OutputDigest = executor.OutputDigest(result.ExitCode, result.Stdout, result.OutputDirHostPath)
	}
	a.record(*job, startedAt, result.Status, result.ExitCode, result.FailureReason, result.Error)
	disposition, err := a.store.PublishJobResult(ctx, result)
	if err != nil {
		return fmt.Errorf("publish job result: %w", err)
	}
	a.recordResult(result.Status)
	logAttrs := []any{"job_id", job.ID, "status", result.Status, "reason", result.FailureReason,
		"exit_code", result.ExitCode, "duration_ms", result.DurationMillis, "disposition", disposition}
	if result.Status != domain.JobStatusSucceeded { // say WHY an attempt failed, not just that it did
		logAttrs = append(logAttrs, "error", truncate(result.Error, 300), "stderr", truncate(strings.TrimSpace(result.Stderr), 300))
	}
	a.logger.Info("job result published", logAttrs...)
	return nil
}

// finishAborted handles an attempt that ended without a result of its own making:
// a fetch/setup error, lease revocation, or worker shutdown.
func (a *Agent) finishAborted(parent, jobCtx, runCtx context.Context, job domain.Job, startedAt time.Time, cause error) error {
	switch {
	case errors.Is(context.Cause(jobCtx), errLeaseRevoked):
		a.logger.Warn("job aborted, result discarded", "job_id", job.ID, "reason", "lease_revoked")
		return nil
	case isEvictCause(context.Cause(jobCtx)):
		cause := context.Cause(jobCtx)
		a.record(job, startedAt, "evicted", -1, ReasonEvicted, cause.Error())
		err := a.publishFailedJob(parent, job, startedAt, cause, ReasonEvicted)
		a.logger.Warn("job returned to the queue after eviction", "event", "job_requeued_after_eviction", "job_id", job.ID, "error", err)
		return nil
	case parent.Err() != nil:
		// Shutting down: hand the job straight back (no attempt burned) rather than
		// making the coordinator wait out the heartbeat window.
		giveBack, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.record(job, startedAt, "evicted", -1, ReasonEvicted, "worker shutting down")
		err := a.publishFailedJob(giveBack, job, startedAt, errors.New("worker shutting down"), ReasonEvicted)
		a.logger.Info("job handed back during shutdown", "job_id", job.ID, "error", err)
		return nil
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		return a.publishFailedJob(parent, job, startedAt, errors.New("job exceeded its runtime limit"), ReasonTimeout)
	default:
		if cause == nil {
			cause = errors.New("job aborted")
		}
		return a.publishFailedJob(parent, job, startedAt, cause, ReasonInfra)
	}
}

func combineErrors(existing, next string) string {
	existing = strings.TrimSpace(existing)
	next = strings.TrimSpace(next)
	switch {
	case existing == "":
		return next
	case next == "":
		return existing
	default:
		return existing + "; " + next
	}
}

func (a *Agent) publishFailedJob(ctx context.Context, job domain.Job, startedAt time.Time, taskErr error, reason string) error {
	a.logger.Error("job attempt failed", "job_id", job.ID, "reason", reason, "error", taskErr)
	finishedAt := time.Now().UTC()
	result := domain.JobResult{
		JobID:          job.ID,
		WorkerID:       a.cfg.WorkerID,
		Status:         domain.JobStatusFailed,
		StartedAt:      startedAt,
		FinishedAt:     finishedAt,
		DurationMillis: finishedAt.Sub(startedAt).Milliseconds(),
		ExitCode:       -1,
		ImageRef:       job.ImageRef,
		ArtifactSHA256: job.ArtifactSHA256,
		Error:          taskErr.Error(),
		Stderr:         taskErr.Error(),
		FailureReason:  reason,
	}
	if _, err := a.store.PublishJobResult(ctx, result); err != nil {
		return err
	}
	a.recordResult(domain.JobStatusFailed)
	return nil
}

func (a *Agent) setState(status, currentJobID string, cpuUsed int, memoryUsedMB int64, abort context.CancelCauseFunc) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = status
	a.currentJobID = currentJobID
	a.abortCurrent = abort
	if currentJobID == "" {
		a.evictedForJob = "" // the once-per-job eviction guard resets between jobs
	}
	a.currentCPU = max(cpuUsed, 0)
	a.currentMemMB = maxInt64(memoryUsedMB, 0)
}

func (a *Agent) state() (string, string, int, int64, int64, int64) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.status, a.currentJobID, a.currentCPU, a.currentMemMB, a.jobsDone, a.jobsFailed
}

func (a *Agent) recordResult(status string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if status == domain.JobStatusFailed {
		a.jobsFailed++
		return
	}
	a.jobsDone++
}

func normalizeCPUUsed(value float64) int {
	if value <= 0 {
		return 0
	}
	used := int(value)
	if used == 0 {
		return 1
	}
	return used
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
