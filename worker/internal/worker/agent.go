package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

type storeAPI interface {
	PullJob(ctx context.Context) (*domain.Job, bool, error)
	PublishHeartbeat(ctx context.Context, heartbeat domain.WorkerHeartbeat) error
	PublishJobStatus(ctx context.Context, status domain.JobState) error
	PublishAssignment(ctx context.Context, assignment domain.Assignment) error
	PublishJobResult(ctx context.Context, result domain.JobResult) error
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

type Agent struct {
	cfg      config.Config
	store    storeAPI
	fetcher  fetcherAPI
	reporter reporterAPI
	executor executorAPI
	logger   *slog.Logger
	started  time.Time

	mu           sync.RWMutex
	status       string
	currentJobID string
	currentCPU   int
	currentMemMB int64
	jobsDone     int64
	jobsFailed   int64
	lastNoJobLog time.Time
}

func NewAgent(cfg config.Config, store storeAPI, fetcher fetcherAPI, reporter reporterAPI, executor executorAPI, logger *slog.Logger) *Agent {
	return &Agent{
		cfg:      cfg,
		store:    store,
		fetcher:  fetcher,
		reporter: reporter,
		executor: executor,
		logger:   logger,
		started:  time.Now().UTC(),
		status:   domain.WorkerStatusAvailable,
	}
}

func (a *Agent) Run(ctx context.Context) error {
	a.logger.Info(
		"worker started",
		"worker_id", a.cfg.WorkerID,
		"redis_addr", a.cfg.RedisAddr,
		"queue_key", a.cfg.QueueKey,
		"poll_via_server", a.cfg.PollViaServer,
		"coordinator_addr", a.cfg.CoordinatorAddr,
	)

	go a.heartbeatLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		job, found, err := a.store.PullJob(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.logger.Error("failed to pull job", "error", err)
			time.Sleep(time.Second)
			continue
		}
		if !found {
			now := time.Now().UTC()
			if a.lastNoJobLog.IsZero() || now.Sub(a.lastNoJobLog) >= 15*time.Second {
				a.logger.Info("no job available, continuing to poll")
				a.lastNoJobLog = now
			}
			continue
		}
		a.lastNoJobLog = time.Time{}
		a.logger.Info("job received", "job_id", job.ID, "image_ref", job.ImageRef)

		if err := a.handleJob(ctx, job); err != nil {
			a.logger.Error("job handling failed", "job_id", job.ID, "error", err)
		}
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
	heartbeatCtx, cancel := context.WithTimeout(ctx, a.cfg.RedisIOTimeout+2*time.Second)
	defer cancel()

	status, _, cpuUsed, memoryUsed, jobsDone, jobsFailed := a.state()
	snapshot := a.reporter.Snapshot(heartbeatCtx)
	heartbeat := domain.WorkerHeartbeat{
		ID:     a.cfg.WorkerID,
		Status: status,
		Resources: domain.WorkerResources{
			CPUCores: snapshot.CPUCores,
			MemoryMB: snapshot.MemoryMB,
			GPU:      snapshot.GPU,
		},
		CurrentLoad: domain.WorkerCurrentLoad{
			CPUUsed:    cpuUsed,
			MemoryUsed: memoryUsed,
		},
		Capabilities: domain.WorkerCapabilities{
			Docker:       snapshot.DockerAvailable,
			GPUSupported: snapshot.GPUSupported,
		},
		LastHeartbeat: time.Now().UTC().Unix(),
		Stats: domain.WorkerStats{
			JobsCompleted: jobsDone,
			JobsFailed:    jobsFailed,
		},
	}

	if err := a.store.PublishHeartbeat(heartbeatCtx, heartbeat); err != nil && heartbeatCtx.Err() == nil {
		a.logger.Warn("failed to publish heartbeat", "error", err)
	}
}

func (a *Agent) handleJob(ctx context.Context, job *domain.Job) error {
	job.Normalize()
	if err := job.Validate(); err != nil {
		return a.publishInvalidJob(ctx, *job, err)
	}

	startedAt := time.Now().UTC()
	a.setState(domain.WorkerStatusBusy, job.ID, normalizeCPUUsed(job.CPUCores), job.MemoryMB)
	defer a.setState(domain.WorkerStatusAvailable, "", 0, 0)

	assignedAt := startedAt
	a.logger.Info("publishing assignment", "job_id", job.ID, "status", domain.AssignmentStatusAssigned)
	if err := a.store.PublishAssignment(ctx, domain.Assignment{
		JobID:    job.ID,
		WorkerID: a.cfg.WorkerID,
		Status:   domain.AssignmentStatusAssigned,
	}); err != nil {
		return fmt.Errorf("publish assignment status: %w", err)
	}

	a.logger.Info("starting job", "job_id", job.ID, "artifact_url", job.ArtifactURL, "image_ref", job.ImageRef)

	preparingStatus := job.BuildState(domain.JobStatusAssigned, &assignedAt, nil, nil, nil)
	if err := a.store.PublishJobStatus(ctx, preparingStatus); err != nil {
		return fmt.Errorf("publish preparing status: %w", err)
	}

	timeout := a.cfg.DefaultJobTimeout
	if job.TimeoutSeconds > 0 {
		timeout = time.Duration(job.TimeoutSeconds) * time.Second
	}
	a.logger.Info("job timeout selected", "job_id", job.ID, "timeout", timeout.String())

	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bundle := artifact.Bundle{}
	if strings.TrimSpace(job.ArtifactURL) != "" || strings.TrimSpace(job.ArtifactSHA256) != "" {
		a.logger.Info("fetching artifact bundle", "job_id", job.ID, "artifact_url", job.ArtifactURL)
		var err error
		bundle, err = a.fetcher.Fetch(jobCtx, *job)
		if err != nil {
			return a.publishFailedJob(ctx, *job, startedAt, err)
		}
		a.logger.Info("artifact bundle ready", "job_id", job.ID, "archive_path", bundle.ArchivePath, "size_bytes", bundle.SizeBytes)
		defer func() {
			if err := artifact.Cleanup(bundle); err != nil {
				a.logger.Warn("failed to cleanup artifact bundle", "job_id", job.ID, "error", err)
			}
		}()
	} else {
		a.logger.Info("no artifact provided; running with existing image reference", "job_id", job.ID, "image_ref", job.ImageRef)
	}

	runningAt := time.Now().UTC()
	runningStatus := job.BuildState(domain.JobStatusRunning, &assignedAt, &runningAt, nil, nil)
	if err := a.store.PublishJobStatus(ctx, runningStatus); err != nil {
		return fmt.Errorf("publish running status: %w", err)
	}
	if err := a.store.PublishAssignment(ctx, domain.Assignment{
		JobID:    job.ID,
		WorkerID: a.cfg.WorkerID,
		Status:   domain.AssignmentStatusRunning,
	}); err != nil {
		return fmt.Errorf("publish running assignment status: %w", err)
	}
	a.logger.Info("starting container execution", "job_id", job.ID, "image_ref", job.ImageRef)

	result := a.executor.Run(jobCtx, *job, bundle)

	if strings.TrimSpace(result.Status) == "" {
		result.Status = domain.JobStatusFailed
	}
	if result.JobID == "" {
		result.JobID = job.ID
	}
	if result.WorkerID == "" {
		result.WorkerID = a.cfg.WorkerID
	}

	if err := a.store.PublishJobResult(ctx, result); err != nil {
		return fmt.Errorf("publish job result: %w", err)
	}
	a.logger.Info("job result published", "job_id", job.ID, "status", result.Status, "duration_ms", result.DurationMillis)
	a.recordResult(result.Status)

	finishedAt := result.FinishedAt
	finalStatus := job.BuildState(result.Status, &assignedAt, &runningAt, &finishedAt, &result)
	if err := a.store.PublishJobStatus(ctx, finalStatus); err != nil {
		return fmt.Errorf("publish final job status: %w", err)
	}

	a.logger.Info("finished job", "job_id", job.ID, "status", result.Status, "exit_code", result.ExitCode)
	return nil
}

func (a *Agent) publishFailedJob(ctx context.Context, job domain.Job, startedAt time.Time, taskErr error) error {
	a.logger.Error("job failed before execution completion", "job_id", job.ID, "error", taskErr)
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
	}
	if err := a.store.PublishJobResult(ctx, result); err != nil {
		return err
	}
	a.recordResult(domain.JobStatusFailed)

	status := job.BuildState(domain.JobStatusFailed, &startedAt, &startedAt, &finishedAt, &result)
	return a.store.PublishJobStatus(ctx, status)
}

func (a *Agent) publishInvalidJob(ctx context.Context, job domain.Job, validationErr error) error {
	a.logger.Error("invalid job payload", "job_id", job.ID, "error", validationErr)
	if strings.TrimSpace(job.ID) == "" {
		return validationErr
	}

	now := time.Now().UTC()
	result := domain.JobResult{
		JobID:          job.ID,
		WorkerID:       a.cfg.WorkerID,
		Status:         domain.JobStatusFailed,
		StartedAt:      now,
		FinishedAt:     now,
		DurationMillis: 0,
		ExitCode:       -1,
		ImageRef:       job.ImageRef,
		ArtifactSHA256: job.ArtifactSHA256,
		Error:          validationErr.Error(),
	}
	if err := a.store.PublishJobResult(ctx, result); err != nil {
		return err
	}
	a.recordResult(domain.JobStatusFailed)

	status := job.BuildState(domain.JobStatusFailed, &now, nil, &now, &result)
	return a.store.PublishJobStatus(ctx, status)
}

func (a *Agent) setState(status, currentJobID string, cpuUsed int, memoryUsedMB int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = status
	a.currentJobID = currentJobID
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
