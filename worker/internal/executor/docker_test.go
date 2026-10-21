package executor

import (
	"testing"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

func TestBuildArgsAddsExpectedDockerFlags(t *testing.T) {
	exec := NewDockerExecutor("docker", "worker-01", true, "/computehive/output", DefaultLimits())
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

	args, err := exec.buildRunArgs(job, "/tmp/computehive-output-job-01")
	if err != nil {
		t.Fatalf("buildRunArgs returned error: %v", err)
	}

	expected := []string{
		"run",
		"--network",
		"none",
		"--name",
		"computehive-worker-01-job-01",
		"--label",
		"computehive.worker.id=worker-01",
		"--label",
		"computehive.job.id=job-01",
		"-v",
		"/tmp/computehive-output-job-01:/computehive/output",
		"--cpus",
		"2",
		"--memory",
		"512m",
		"--memory-swap",
		"512m",
		"--pids-limit",
		"256",
		"--gpus",
		"all",
		"-e",
		"A=1",
		"-e",
		"B=2",
		"-e",
		"COMPUTEHIVE_OUTPUT_DIR=/computehive/output",
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
	exec := NewDockerExecutor("docker", "worker-01", false, "/computehive/output", DefaultLimits())
	job := domain.Job{
		ID:             "job-01",
		ArtifactURL:    "https://example.com/job.tar.gz",
		ArtifactSHA256: "abc123",
		ImageRef:       "computehive/job-01:latest",
		GPU:            true,
	}

	if _, err := exec.buildRunArgs(job, "/tmp/computehive-output-job-01"); err == nil {
		t.Fatal("expected gpu job rejection")
	}
}

func TestBuildArgsAppliesDefaultLimitsWhenJobDeclaresNone(t *testing.T) {
	exec := NewDockerExecutor("docker", "w", false, "/out", Limits{DefaultMemoryMB: 300, DefaultCPUs: 0.5, PidsLimit: 64})
	args, err := exec.buildRunArgs(domain.Job{ID: "j", ImageRef: "img"}, "/tmp/o")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"--cpus": "0.5", "--memory": "300m", "--memory-swap": "300m", "--pids-limit": "64"}
	for flag, value := range want {
		found := false
		for i := range args[:len(args)-1] {
			if args[i] == flag && args[i+1] == value {
				found = true
			}
		}
		if !found {
			t.Errorf("expected %s %s in %v", flag, value, args)
		}
	}
}

func TestBuildArgsNeverAllowsSwapBeyondMemory(t *testing.T) {
	exec := NewDockerExecutor("docker", "w", false, "/out", DefaultLimits())
	args, _ := exec.buildRunArgs(domain.Job{ID: "j", ImageRef: "img", MemoryMB: 2048}, "/tmp/o")
	var mem, swap string
	for i := range args[:len(args)-1] {
		switch args[i] {
		case "--memory":
			mem = args[i+1]
		case "--memory-swap":
			swap = args[i+1]
		}
	}
	if mem == "" || mem != swap {
		t.Fatalf("--memory (%q) and --memory-swap (%q) must be equal so the container has no swap", mem, swap)
	}
}

func TestRunArgsDoNotUseAutoRemove(t *testing.T) {
	exec := NewDockerExecutor("docker", "w", false, "/out", DefaultLimits())
	args, _ := exec.buildRunArgs(domain.Job{ID: "j", ImageRef: "img"}, "/tmp/o")
	for _, a := range args {
		if a == "--rm" {
			t.Fatal("--rm races with explicit removal and can strand a Dead container; the executor removes containers itself")
		}
	}
}
