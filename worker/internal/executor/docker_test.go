package executor

import (
	"testing"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

func TestBuildArgsAddsExpectedDockerFlags(t *testing.T) {
	exec := NewDockerExecutor("docker", "worker-01", true)
	job := domain.Job{
		ID:             "job-01",
		ArtifactURL:    "https://example.com/job.tar.gz",
		ArtifactSHA256: "abc123",
		ImageRef:       "computehive/job-01:latest",
		Command:        []string{"python", "-c", "print('ok')"},
		Env:            map[string]string{"B": "2", "A": "1"},
		CPUCores:       2,
		MemoryMB:       512,
		GPU:            true,
	}

	args, err := exec.buildRunArgs(job)
	if err != nil {
		t.Fatalf("buildRunArgs returned error: %v", err)
	}

	expected := []string{
		"run",
		"--rm",
		"--network",
		"none",
		"--name",
		"computehive-worker-01-job-01",
		"--label",
		"computehive.worker.id=worker-01",
		"--label",
		"computehive.job.id=job-01",
		"--cpus",
		"2",
		"--memory",
		"512m",
		"--gpus",
		"all",
		"-e",
		"A=1",
		"-e",
		"B=2",
		"computehive/job-01:latest",
		"python",
		"-c",
		"print('ok')",
	}

	if len(args) != len(expected) {
		t.Fatalf("expected %d args, got %d: %#v", len(expected), len(args), args)
	}

	for index := range expected {
		if args[index] != expected[index] {
			t.Fatalf("arg %d mismatch: expected %q, got %q", index, expected[index], args[index])
		}
	}
}

func TestBuildArgsRejectsDisabledGPUJobs(t *testing.T) {
	exec := NewDockerExecutor("docker", "worker-01", false)
	job := domain.Job{
		ID:             "job-01",
		ArtifactURL:    "https://example.com/job.tar.gz",
		ArtifactSHA256: "abc123",
		ImageRef:       "computehive/job-01:latest",
		GPU:            true,
	}

	if _, err := exec.buildRunArgs(job); err == nil {
		t.Fatal("expected gpu job rejection")
	}
}
