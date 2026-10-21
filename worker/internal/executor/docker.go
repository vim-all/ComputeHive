package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

type DockerExecutor struct {
	binary              string
	workerID            string
	allowGPUJobs        bool
	outputCollectionDir string
}

func NewDockerExecutor(binary, workerID string, allowGPUJobs bool, outputCollectionDir string) *DockerExecutor {
	return &DockerExecutor{
		binary:              binary,
		workerID:            workerID,
		allowGPUJobs:        allowGPUJobs,
		outputCollectionDir: strings.TrimSpace(outputCollectionDir),
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

	outputRoot, err := os.MkdirTemp("", "computehive-output-*")
	if err != nil {
		result.Error = "create output directory: " + err.Error()
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result
	}
	result.CleanupDir = outputRoot
	result.OutputDirHostPath = outputRoot

	if strings.TrimSpace(bundle.ArchivePath) != "" {
		loadStdout, loadStderr, err := e.runDockerCommand(ctx, "load", "-i", bundle.ArchivePath)
		if err != nil {
			result.Error = "docker load failed: " + err.Error()
			result.FailureReason = domain.FailureInfra
			result.Stdout = loadStdout
			result.Stderr = loadStderr
			result.FinishedAt = time.Now().UTC()
			result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
			return result
		}
		defer e.cleanupImage(job.ImageRef)

		if _, _, err := e.runDockerCommand(ctx, "image", "inspect", job.ImageRef); err != nil {
			result.Error = "loaded archive does not contain expected image_ref: " + job.ImageRef
			result.FailureReason = domain.FailureInfra
			result.Stdout = loadStdout
			result.FinishedAt = time.Now().UTC()
			result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
			return result
		}
	}

	args, err := e.buildRunArgs(job, outputRoot)
	if err != nil {
		result.Error = err.Error()
		result.FailureReason = domain.FailureInfra
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result
	}

	cmd := exec.CommandContext(ctx, e.binary, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	name := containerName(e.workerID, job.ID)
	e.forceRemove(name) // a leftover from an earlier attempt would make `docker run --name` fail

	runErr := cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()

	// Cancelling the context only kills the `docker run` client; the container keeps
	// running unless we remove it ourselves (timeouts, lease revocation, shutdown).
	if ctx.Err() != nil {
		e.forceRemove(name)
	}

	if runErr != nil {
		result.Error = runErr.Error()
		switch exitErr := runErr.(type) {
		case *exec.ExitError:
			result.ExitCode = exitErr.ExitCode()
			switch {
			case errors.Is(ctx.Err(), context.DeadlineExceeded):
				result.FailureReason = domain.FailureTimeout
			case ctx.Err() != nil:
				result.FailureReason = domain.FailureEvicted
			case result.ExitCode == 125: // docker itself failed (daemon error, bad flags), not the job
				result.FailureReason = domain.FailureInfra
			default:
				result.FailureReason = domain.FailureJobExit
			}
		default: // the docker CLI could not even start
			result.FailureReason = domain.FailureInfra
		}
		return result
	}

	result.Status = domain.JobStatusSucceeded
	result.ExitCode = 0
	return result
}

func (e *DockerExecutor) buildRunArgs(job domain.Job, hostOutputDir string) ([]string, error) {
	job.Normalize()
	if err := job.Validate(); err != nil {
		return nil, err
	}
	if job.GPU && !e.allowGPUJobs {
		return nil, fmt.Errorf("gpu jobs are disabled on this worker")
	}
	if strings.TrimSpace(hostOutputDir) == "" {
		return nil, fmt.Errorf("host output directory is required")
	}
	if strings.TrimSpace(e.outputCollectionDir) == "" {
		return nil, fmt.Errorf("worker output collection directory is required")
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
		"-v",
		hostOutputDir + ":" + e.outputCollectionDir,
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
	args = append(args, "-e", "COMPUTEHIVE_OUTPUT_DIR="+e.outputCollectionDir)

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

// forceRemove kills and removes a container by name, ignoring "no such container".
func (e *DockerExecutor) forceRemove(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, _ = e.runDockerCommand(ctx, "rm", "-f", name)
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
