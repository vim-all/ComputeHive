package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/adhyan-jain/ComputeHive/worker/internal/artifact"
	"github.com/adhyan-jain/ComputeHive/worker/internal/domain"
)

type DockerExecutor struct {
	binary       string
	workerID     string
	allowGPUJobs bool
}

func NewDockerExecutor(binary, workerID string, allowGPUJobs bool) *DockerExecutor {
	return &DockerExecutor{
		binary:       binary,
		workerID:     workerID,
		allowGPUJobs: allowGPUJobs,
	}
}

func (e *DockerExecutor) Run(ctx context.Context, job domain.Job, bundle artifact.Bundle) domain.JobResult {
	startedAt := time.Now().UTC()
	job.Normalize()
	artifactSHA256 := bundle.SHA256
	if strings.TrimSpace(artifactSHA256) == "" {
		artifactSHA256 = job.ArtifactSHA256
	}

	result := domain.JobResult{
		JobID:             job.ID,
		WorkerID:          e.workerID,
		Status:            domain.JobStatusFailed,
		StartedAt:         startedAt,
		ExitCode:          -1,
		ImageRef:          job.ImageRef,
		ArtifactSHA256:    artifactSHA256,
		ArtifactSizeBytes: bundle.SizeBytes,
	}

	if strings.TrimSpace(bundle.ArchivePath) != "" {
		loadStdout, loadStderr, err := e.runDockerCommand(ctx, "load", "-i", bundle.ArchivePath)
		if err != nil {
			result.Error = "docker load failed: " + err.Error()
			result.Stdout = loadStdout
			result.Stderr = loadStderr
			result.FinishedAt = time.Now().UTC()
			result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
			return result
		}
		defer e.cleanupImage(job.ImageRef)

		if _, _, err := e.runDockerCommand(ctx, "image", "inspect", job.ImageRef); err != nil {
			result.Error = "loaded archive does not contain expected image_ref: " + job.ImageRef
			result.Stdout = loadStdout
			result.FinishedAt = time.Now().UTC()
			result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
			return result
		}
	}

	args, err := e.buildRunArgs(job)
	if err != nil {
		result.Error = err.Error()
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result
	}

	cmd := exec.CommandContext(ctx, e.binary, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()

	if runErr != nil {
		result.Error = runErr.Error()
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		return result
	}

	result.Status = domain.JobStatusSucceeded
	result.ExitCode = 0
	return result
}

func (e *DockerExecutor) buildRunArgs(job domain.Job) ([]string, error) {
	job.Normalize()
	if err := job.Validate(); err != nil {
		return nil, err
	}
	if job.GPU && !e.allowGPUJobs {
		return nil, fmt.Errorf("gpu jobs are disabled on this worker")
	}

	args := []string{
		"run",
		"--rm",
		"--network",
		"none",
		"--name",
		containerName(e.workerID, job.ID),
		"--label",
		"computehive.worker.id=" + e.workerID,
		"--label",
		"computehive.job.id=" + job.ID,
	}

	if job.CPUCores > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(job.CPUCores, 'f', -1, 64))
	}
	if job.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", job.MemoryMB))
	}
	if job.GPU {
		args = append(args, "--gpus", "all")
	}

	keys := make([]string, 0, len(job.Env))
	for key := range job.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "-e", key+"="+job.Env[key])
	}

	args = append(args, job.ImageRef)
	args = append(args, job.Command...)
	return args, nil
}

func (e *DockerExecutor) runDockerCommand(ctx context.Context, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, e.binary, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func (e *DockerExecutor) cleanupImage(imageRef string) {
	if strings.TrimSpace(imageRef) == "" {
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, _ = e.runDockerCommand(cleanupCtx, "image", "rm", "-f", imageRef)
}

func containerName(workerID, jobID string) string {
	raw := strings.ToLower(workerID + "-" + jobID)
	var builder strings.Builder
	builder.Grow(len(raw))
	for _, char := range raw {
		switch {
		case char >= 'a' && char <= 'z':
			builder.WriteRune(char)
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
		case char == '-', char == '_', char == '.':
			builder.WriteRune(char)
		default:
			builder.WriteByte('-')
		}
	}

	name := "computehive-" + strings.Trim(builder.String(), "-")
	if len(name) > 63 {
		return name[:63]
	}

	return name
}
