package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	JobStatusPending   = "pending"
	JobStatusAssigned  = "assigned"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"

	// Backward-compatibility aliases.
	JobStatusPreparing = JobStatusAssigned
	JobStatusSucceeded = JobStatusCompleted
)

type Job struct {
	ID             string            `json:"id,omitempty"`
	Status         string            `json:"status,omitempty"`
	TaskID         string            `json:"task_id,omitempty"`
	ArtifactURL    string            `json:"artifact_url,omitempty"`
	S3URL          string            `json:"s3_url,omitempty"`
	ArtifactSHA256 string            `json:"artifact_sha256,omitempty"`
	ImageHash      string            `json:"image_hash,omitempty"`
	ImageRef       string            `json:"image_ref"`
	Command        []string          `json:"command,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	GPU            bool              `json:"gpu,omitempty"`
	CPUCores       float64           `json:"cpu_cores,omitempty"`
	MemoryMB       int64             `json:"memory_mb,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	CreatedAtUnix  int64             `json:"-"`
	AssignedAtUnix *int64            `json:"-"`
	StartedAtUnix  *int64            `json:"-"`
	CompletedAtUnix *int64           `json:"-"`
}

func (j *Job) UnmarshalJSON(data []byte) error {
	type imagePayload struct {
		Ref       string `json:"ref"`
		SourceURL string `json:"source_url"`
		Hash      string `json:"hash"`
	}
	type executionPayload struct {
		Command []string          `json:"command"`
		Env     map[string]string `json:"env,omitempty"`
	}
	type timeoutPayload struct {
		MaxExecutionSeconds int `json:"max_execution_seconds"`
	}
	type timestampsPayload struct {
		CreatedAt   int64  `json:"created_at"`
		AssignedAt  *int64 `json:"assigned_at"`
		StartedAt   *int64 `json:"started_at"`
		CompletedAt *int64 `json:"completed_at"`
	}
	type resourcesPayload struct {
		CPUCores float64 `json:"cpu_cores,omitempty"`
		MemoryMB int64   `json:"memory_mb,omitempty"`
		GPU      bool    `json:"gpu,omitempty"`
	}

	var payload struct {
		ID         string            `json:"id"`
		Status     string            `json:"status"`
		Image      imagePayload      `json:"image"`
		Execution  executionPayload  `json:"execution"`
		Timeouts   timeoutPayload    `json:"timeouts"`
		Timestamps timestampsPayload `json:"timestamps"`
		Resources  resourcesPayload  `json:"resources,omitempty"`

		TaskID         string            `json:"task_id,omitempty"`
		ArtifactURL    string            `json:"artifact_url,omitempty"`
		S3URL          string            `json:"s3_url,omitempty"`
		ArtifactSHA256 string            `json:"artifact_sha256,omitempty"`
		ImageHash      string            `json:"image_hash,omitempty"`
		ImageRef       string            `json:"image_ref,omitempty"`
		Command        []string          `json:"command,omitempty"`
		Env            map[string]string `json:"env,omitempty"`
		GPU            bool              `json:"gpu,omitempty"`
		CPUCores       float64           `json:"cpu_cores,omitempty"`
		MemoryMB       int64             `json:"memory_mb,omitempty"`
		TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	job := Job{
		ID:              strings.TrimSpace(payload.ID),
		Status:          strings.TrimSpace(payload.Status),
		TaskID:          strings.TrimSpace(payload.TaskID),
		ArtifactURL:     strings.TrimSpace(payload.ArtifactURL),
		S3URL:           strings.TrimSpace(payload.S3URL),
		ArtifactSHA256:  strings.TrimSpace(payload.ArtifactSHA256),
		ImageHash:       strings.TrimSpace(payload.ImageHash),
		ImageRef:        strings.TrimSpace(payload.ImageRef),
		Command:         payload.Command,
		Env:             payload.Env,
		GPU:             payload.GPU,
		CPUCores:        payload.CPUCores,
		MemoryMB:        payload.MemoryMB,
		TimeoutSeconds:  payload.TimeoutSeconds,
		CreatedAtUnix:   payload.Timestamps.CreatedAt,
		AssignedAtUnix:  payload.Timestamps.AssignedAt,
		StartedAtUnix:   payload.Timestamps.StartedAt,
		CompletedAtUnix: payload.Timestamps.CompletedAt,
	}

	if strings.TrimSpace(job.ImageRef) == "" {
		job.ImageRef = strings.TrimSpace(payload.Image.Ref)
	}
	if strings.TrimSpace(job.ArtifactURL) == "" {
		job.ArtifactURL = strings.TrimSpace(payload.Image.SourceURL)
	}
	if strings.TrimSpace(job.ArtifactSHA256) == "" {
		job.ArtifactSHA256 = strings.TrimSpace(payload.Image.Hash)
	}
	if len(job.Command) == 0 {
		job.Command = payload.Execution.Command
	}
	if len(job.Env) == 0 {
		job.Env = payload.Execution.Env
	}
	if job.TimeoutSeconds == 0 {
		job.TimeoutSeconds = payload.Timeouts.MaxExecutionSeconds
	}
	if job.CPUCores == 0 {
		job.CPUCores = payload.Resources.CPUCores
	}
	if job.MemoryMB == 0 {
		job.MemoryMB = payload.Resources.MemoryMB
	}
	if !job.GPU {
		job.GPU = payload.Resources.GPU
	}

	*j = job.normalized()
	return nil
}

func (j *Job) Normalize() {
	normalized := j.normalized()
	*j = normalized
}

func (j Job) Validate() error {
	j = j.normalized()

	if strings.TrimSpace(j.ID) == "" {
		return fmt.Errorf("job id is required")
	}
	hasArtifact := strings.TrimSpace(j.ArtifactURL) != "" || strings.TrimSpace(j.ArtifactSHA256) != ""
	if hasArtifact {
		if strings.TrimSpace(j.ArtifactURL) == "" {
			return fmt.Errorf("job artifact_url is required when artifact_sha256 is provided")
		}
		if strings.TrimSpace(j.ArtifactSHA256) == "" {
			return fmt.Errorf("job artifact_sha256 is required when artifact_url is provided")
		}
	}
	if strings.TrimSpace(j.ImageRef) == "" {
		return fmt.Errorf("job image_ref is required")
	}
	if j.CPUCores < 0 {
		return fmt.Errorf("cpu_cores must be >= 0")
	}
	if j.MemoryMB < 0 {
		return fmt.Errorf("memory_mb must be >= 0")
	}
	if j.TimeoutSeconds < 0 {
		return fmt.Errorf("timeout_seconds must be >= 0")
	}

	return nil
}

func (j Job) normalized() Job {
	if strings.TrimSpace(j.ID) == "" {
		j.ID = strings.TrimSpace(j.TaskID)
	}
	if strings.TrimSpace(j.Status) == "" {
		j.Status = JobStatusPending
	}
	if strings.TrimSpace(j.ArtifactURL) == "" {
		j.ArtifactURL = strings.TrimSpace(j.S3URL)
	}
	if strings.TrimSpace(j.ArtifactSHA256) == "" {
		j.ArtifactSHA256 = strings.TrimSpace(j.ImageHash)
	}

	return j
}

type JobImage struct {
	Ref       string `json:"ref"`
	SourceURL string `json:"source_url"`
	Hash      string `json:"hash"`
}

type JobExecution struct {
	Command []string `json:"command"`
}

type JobTimeouts struct {
	MaxExecutionSeconds int `json:"max_execution_seconds"`
}

type JobResultState struct {
	Status   *string `json:"status"`
	Output   *string `json:"output"`
	Error    *string `json:"error"`
	ExitCode *int    `json:"exit_code"`
}

type JobTimestamps struct {
	CreatedAt   int64  `json:"created_at"`
	AssignedAt  *int64 `json:"assigned_at"`
	StartedAt   *int64 `json:"started_at"`
	CompletedAt *int64 `json:"completed_at"`
}

type JobState struct {
	ID         string         `json:"id"`
	Status     string         `json:"status"`
	Image      JobImage       `json:"image"`
	Execution  JobExecution   `json:"execution"`
	Timeouts   JobTimeouts    `json:"timeouts"`
	Result     JobResultState `json:"result"`
	Timestamps JobTimestamps  `json:"timestamps"`
}

func (j Job) BuildState(status string, assignedAt, startedAt, completedAt *time.Time, result *JobResult) JobState {
	j = j.normalized()
	createdAt := j.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = time.Now().UTC().Unix()
	}

	stateResult := JobResultState{}
	if result != nil {
		resultStatus := strings.TrimSpace(result.Status)
		if resultStatus != "" {
			stateResult.Status = &resultStatus
		}
		if output := strings.TrimSpace(result.Stdout); output != "" {
			stateResult.Output = &output
		}
		errorMessage := strings.TrimSpace(result.Error)
		if errorMessage == "" {
			errorMessage = strings.TrimSpace(result.Stderr)
		}
		if errorMessage != "" {
			stateResult.Error = &errorMessage
		}
		exitCode := result.ExitCode
		stateResult.ExitCode = &exitCode
	}

	toUnix := func(value *time.Time) *int64 {
		if value == nil {
			return nil
		}
		unix := value.UTC().Unix()
		return &unix
	}

	return JobState{
		ID:     j.ID,
		Status: status,
		Image: JobImage{
			Ref:       j.ImageRef,
			SourceURL: j.ArtifactURL,
			Hash:      j.ArtifactSHA256,
		},
		Execution: JobExecution{
			Command: j.Command,
		},
		Timeouts: JobTimeouts{
			MaxExecutionSeconds: j.TimeoutSeconds,
		},
		Result: stateResult,
		Timestamps: JobTimestamps{
			CreatedAt:   createdAt,
			AssignedAt:  toUnix(assignedAt),
			StartedAt:   toUnix(startedAt),
			CompletedAt: toUnix(completedAt),
		},
	}
}

type JobResult struct {
	JobID             string    `json:"job_id"`
	WorkerID          string    `json:"worker_id"`
	Status            string    `json:"status"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	DurationMillis    int64     `json:"duration_millis"`
	ExitCode          int       `json:"exit_code"`
	ImageRef          string    `json:"image_ref,omitempty"`
	ArtifactSHA256    string    `json:"artifact_sha256,omitempty"`
	ArtifactSizeBytes int64     `json:"artifact_size_bytes,omitempty"`
	Stdout            string    `json:"stdout,omitempty"`
	Stderr            string    `json:"stderr,omitempty"`
	Error             string    `json:"error,omitempty"`
}
