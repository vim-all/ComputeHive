package executor

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

const testImage = "alpine:3.21"

func needDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
	if err := exec.Command("docker", "image", "inspect", testImage).Run(); err != nil {
		if out, err := exec.Command("docker", "pull", testImage).CombinedOutput(); err != nil {
			t.Skipf("cannot get %s: %v: %s", testImage, err, out)
		}
	}
}

func limited() *DockerExecutor {
	return NewDockerExecutor("docker", "limits-test", false, "/computehive/output",
		Limits{DefaultMemoryMB: 64, DefaultCPUs: 0.5, PidsLimit: 32, StopGrace: time.Second})
}

func containerExists(name string) bool {
	out, _ := exec.Command("docker", "ps", "-a", "-q", "--filter", "name=^"+name+"$").Output()
	return strings.TrimSpace(string(out)) != ""
}

// The caps really are on the running container, and cancelling the job really removes it.
func TestRunningContainerCarriesTheHardLimitsAndIsRemovedOnCancel(t *testing.T) {
	needDocker(t)
	e := limited()
	job := domain.Job{ID: "inspect-1", ImageRef: testImage, Command: []string{"sleep", "60"}}
	name := containerName("limits-test", job.ID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan domain.JobResult, 1)
	go func() { done <- e.Run(ctx, job, artifact.Bundle{}) }()

	var out string
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := exec.Command("docker", "inspect", "--format",
			"{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.PidsLimit}} {{.HostConfig.NanoCpus}} {{.HostConfig.NetworkMode}}", name).Output()
		if err == nil {
			out = strings.TrimSpace(string(b))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("container never appeared")
		}
		time.Sleep(100 * time.Millisecond)
	}
	f := strings.Fields(out)
	if len(f) != 5 {
		t.Fatalf("inspect output %q", out)
	}
	mem, _ := strconv.ParseInt(f[0], 10, 64)
	swap, _ := strconv.ParseInt(f[1], 10, 64)
	pids, _ := strconv.ParseInt(f[2], 10, 64)
	nano, _ := strconv.ParseInt(f[3], 10, 64)
	if mem != 64<<20 {
		t.Errorf("Memory = %d, want %d", mem, int64(64<<20))
	}
	if swap != mem {
		t.Errorf("MemorySwap = %d, want == Memory (%d): swap must be unusable", swap, mem)
	}
	if pids != 32 {
		t.Errorf("PidsLimit = %d, want 32", pids)
	}
	if nano != 500_000_000 {
		t.Errorf("NanoCpus = %d, want 500000000 (0.5 CPU)", nano)
	}
	if f[4] != "none" {
		t.Errorf("NetworkMode = %q, want none", f[4])
	}

	cancel() // job cancelled / timed out / evicted
	select {
	case res := <-done:
		if res.Status == domain.JobStatusSucceeded {
			t.Fatalf("cancelled job reported success: %+v", res)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if containerExists(name) {
		out, _ := exec.Command("docker", "ps", "-a", "--filter", "name=limits-test", "--format", "{{.Names}} | {{.Status}} | {{.State}}").CombinedOutput()
		t.Fatalf("container survived cancellation: the job would keep running on the contributor's machine\n%s", out)
	}
}

// A job that allocates past its cap is OOM-killed by the kernel, not allowed to eat the host.
func TestJobExceedingItsMemoryCapIsKilled(t *testing.T) {
	needDocker(t)
	e := limited() // 64 MB, no swap
	job := domain.Job{ID: "oom-1", ImageRef: testImage,
		Command: []string{"sh", "-c", `x=$(head -c 300000000 /dev/zero | tr '\0' a); echo survived ${#x}`}}
	res := e.Run(context.Background(), job, artifact.Bundle{})
	if res.Status == domain.JobStatusSucceeded || strings.Contains(res.Stdout, "survived") {
		t.Fatalf("job allocated past its 64 MB cap: %+v", res)
	}
	if res.ExitCode != 137 {
		t.Fatalf("exit code %d, want 137 (SIGKILL from the OOM killer); result %+v", res.ExitCode, res)
	}
	if res.FailureReason != domain.FailureJobExit {
		t.Fatalf("failure reason %q: exceeding its own memory cap is the job's fault", res.FailureReason)
	}
	if containerExists(containerName("limits-test", job.ID)) {
		t.Fatal("container left behind")
	}
}

// A fork bomb stops at the PID limit; the host keeps working.
func TestForkBombIsContainedByThePidsLimit(t *testing.T) {
	needDocker(t)
	e := limited() // 32 pids
	// Counts how many of 400 background processes the kernel allowed (builtins only, so the
	// script itself still works once forking starts failing).
	job := domain.Job{ID: "fork-1", ImageRef: testImage,
		Command: []string{"sh", "-c", `ok=0; i=0; while [ $i -lt 400 ]; do if (sleep 4 &) 2>/dev/null; then ok=$((ok+1)); fi; i=$((i+1)); done; echo "spawned=$ok"`}}
	res := e.Run(context.Background(), job, artifact.Bundle{})
	idx := strings.Index(res.Stdout, "spawned=")
	if idx < 0 {
		t.Fatalf("no result in output %q (result %+v)", res.Stdout, res)
	}
	var spawned int
	if _, err := fmt.Sscanf(res.Stdout[idx:], "spawned=%d", &spawned); err != nil {
		t.Fatal(err)
	}
	if spawned >= 32 {
		t.Fatalf("%d of 400 processes were created inside a container limited to 32 PIDs: the fork bomb was not contained", spawned)
	}
	if spawned == 0 {
		t.Fatal("no process could be created at all; the test is not measuring anything")
	}
	t.Logf("fork bomb tried 400 processes, the kernel allowed %d (limit 32)", spawned)
	if containerExists(containerName("limits-test", job.ID)) {
		t.Fatal("container left behind")
	}
}

// Containers left by a crashed worker are swept at startup, and other workers' containers are not touched.
func TestCleanupOrphansRemovesOnlyThisWorkersContainers(t *testing.T) {
	needDocker(t)
	run := func(name, worker string) {
		out, err := exec.Command("docker", "run", "-d", "--name", name, "--label", "computehive.worker.id="+worker, testImage, "sleep", "120").CombinedOutput()
		if err != nil {
			t.Fatalf("start %s: %v: %s", name, err, out)
		}
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", name).Run() })
	}
	run("ch-orphan-mine", "limits-test")
	run("ch-orphan-theirs", "some-other-worker")

	n, err := limited().CleanupOrphans(context.Background())
	if err != nil || n < 1 {
		t.Fatalf("CleanupOrphans = %d, %v", n, err)
	}
	if containerExists("ch-orphan-mine") {
		t.Fatal("own orphan was not removed")
	}
	if !containerExists("ch-orphan-theirs") {
		t.Fatal("another worker's container was removed")
	}
}

// A job that outlives its timeout is stopped and removed, not left running.
func TestTimedOutJobIsStoppedAndClassifiedAsTimeout(t *testing.T) {
	needDocker(t)
	e := limited()
	job := domain.Job{ID: "timeout-1", ImageRef: testImage, Command: []string{"sleep", "120"}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	res := e.Run(ctx, job, artifact.Bundle{})
	if took := time.Since(start); took > 25*time.Second {
		t.Fatalf("timeout enforcement took %s", took)
	}
	if res.FailureReason != domain.FailureTimeout {
		t.Fatalf("failure reason %q, want timeout (result %+v)", res.FailureReason, res)
	}
	if containerExists(containerName("limits-test", job.ID)) {
		t.Fatal("timed-out container is still there")
	}
}
