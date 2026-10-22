package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

// Limits are the hard per-container caps applied to every job. A job's own request is
// honoured when it declares one; jobs that declare nothing get the defaults, so no
// container ever runs unbounded.
type Limits struct {
	DefaultMemoryMB int64         // --memory (and --memory-swap, so there is no swap to abuse)
	DefaultCPUs     float64       // --cpus
	PidsLimit       int           // --pids-limit: stops fork bombs
	StopGrace       time.Duration // SIGTERM-to-SIGKILL window when a job is cancelled, evicted or times out
}

// DefaultLimits: 512 MB, 1 CPU, 256 processes, 5s graceful stop.
func DefaultLimits() Limits {
	return Limits{DefaultMemoryMB: 512, DefaultCPUs: 1, PidsLimit: 256, StopGrace: 5 * time.Second}
}

// Sandbox holds the isolation settings that apply to every job container.
type Sandbox struct {
	// User is the container user ("uid[:gid]"). "" or "auto": the worker's own uid:gid when it is
	// not root (so files the job writes are deletable by the worker), otherwise 65534:65534.
	// Root is never accepted.
	User string
	// Runtime is the OCI runtime: "" for Docker's default (runc) or "runsc" for gVisor.
	Runtime string

	TmpfsMB          int           // size of the noexec /tmp scratch
	OutputMaxBytes   int64         // cap on the writable work directory, enforced by a watcher plus RLIMIT_FSIZE
	OutputMaxEntries int           // cap on files/dirs a job may create in it
	OutputPoll       time.Duration // watcher interval
	NoFile           int           // RLIMIT_NOFILE
	MaxLogBytes      int           // captured stdout and stderr are each truncated to this

	Images       ImagePolicy
	LoadPrefixes []string // tags an uploaded image archive may create; anything else is refused

	// DockerConfigDir is an EMPTY directory used as DOCKER_CONFIG for every docker command, so the
	// contributor's stored registry credentials are never used on a stranger's behalf.
	DockerConfigDir string
}

// DefaultSandbox is the strict profile: nothing allowed beyond the worker's own namespace.
func DefaultSandbox() Sandbox {
	return Sandbox{
		TmpfsMB: 64, OutputMaxBytes: 256 << 20, OutputMaxEntries: 20000, OutputPoll: 250 * time.Millisecond,
		NoFile: 1024, MaxLogBytes: 1 << 20,
		LoadPrefixes: []string{"computehive-job:", "computehive/"},
	}
}

// NetworkAttachment is how a job container is connected to a network.
type NetworkAttachment struct {
	Name    string            // "none" or a worker-created network
	Env     map[string]string // for example HTTPS_PROXY
	Cleanup func()
}

// NetworkProvider decides a job's network. The default provides none at all.
type NetworkProvider interface {
	Attach(ctx context.Context, job domain.Job) (NetworkAttachment, error)
	Owns(network string) bool
}

type noNetwork struct{}

func (noNetwork) Attach(context.Context, domain.Job) (NetworkAttachment, error) {
	return NetworkAttachment{Name: "none"}, nil
}
func (noNetwork) Owns(string) bool { return false }

type DockerExecutor struct {
	binary              string
	workerID            string
	allowGPUJobs        bool
	outputCollectionDir string
	limits              Limits
	net                 NetworkProvider

	sbMu sync.RWMutex
	sb   Sandbox
}

func NewDockerExecutor(binary, workerID string, allowGPUJobs bool, outputCollectionDir string, limits Limits, sb ...Sandbox) *DockerExecutor {
	s := DefaultSandbox()
	if len(sb) > 0 {
		s = sb[0]
	}
	if s.DockerConfigDir == "" {
		if dir, err := os.MkdirTemp("", "computehive-docker-config-"); err == nil {
			s.DockerConfigDir = dir
		}
	}
	return &DockerExecutor{
		binary:              binary,
		workerID:            workerID,
		allowGPUJobs:        allowGPUJobs,
		outputCollectionDir: strings.TrimSpace(outputCollectionDir),
		limits:              limits,
		sb:                  s,
		net:                 noNetwork{},
	}
}

// sbc returns a consistent copy of the sandbox settings.
func (e *DockerExecutor) sbc() Sandbox {
	e.sbMu.RLock()
	defer e.sbMu.RUnlock()
	return e.sb
}

// Configure changes the sandbox settings; it applies from the next job on. The worker uses it to
// apply the contributor's policy (disk quota, image allow/deny) without a restart.
func (e *DockerExecutor) Configure(fn func(*Sandbox)) {
	e.sbMu.Lock()
	defer e.sbMu.Unlock()
	fn(&e.sb)
}

// SetStopGrace changes how long containers get to exit after SIGTERM (0 = kill at once, for the kill switch).
func (e *DockerExecutor) SetStopGrace(d time.Duration) {
	e.sbMu.Lock()
	defer e.sbMu.Unlock()
	e.limits.StopGrace = d
}

// WithNetwork installs a network provider (the filtering-proxy egress mode).
func (e *DockerExecutor) WithNetwork(p NetworkProvider) *DockerExecutor {
	e.net = p
	return e
}

// IsolationLevel names the isolation this executor provides, for the worker's capabilities.
func (e *DockerExecutor) IsolationLevel() string {
	if e.sbc().Runtime == "runsc" {
		return "gvisor"
	}
	return "hardened-runc"
}

// DetectRuntime reports whether the Docker daemon has the gVisor runtime ("runsc") registered.
func DetectRuntime(ctx context.Context, dockerBinary string) (hasRunsc bool, err error) {
	out, err := exec.CommandContext(ctx, dockerBinary, "info", "--format", "{{json .Runtimes}}").Output()
	if err != nil {
		return false, fmt.Errorf("docker info: %w", err)
	}
	var runtimes map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(out), &runtimes); err != nil {
		return false, fmt.Errorf("parse docker runtimes: %w", err)
	}
	_, ok := runtimes["runsc"]
	return ok, nil
}

func (e *DockerExecutor) resolveUser() (string, error) {
	u := strings.TrimSpace(e.sbc().User)
	if u == "" || u == "auto" {
		if uid := os.Getuid(); uid != 0 {
			return fmt.Sprintf("%d:%d", uid, os.Getgid()), nil
		}
		return "65534:65534", nil
	}
	if isRootUser(u) {
		return "", fmt.Errorf("job user %q is root: refusing to run untrusted code as root", u)
	}
	return u, nil
}

func fail(result *domain.JobResult, started time.Time, reason, msg string) domain.JobResult {
	result.Error = msg
	result.FailureReason = reason
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(started).Milliseconds()
	return *result
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

	// Refuse malformed or hostile specs before touching Docker. These are the submitter's fault.
	if err := ValidateImageRef(job.ImageRef); err != nil {
		return fail(&result, startedAt, domain.FailureJobExit, "rejected: "+err.Error())
	}
	if err := ValidateJobFields(job.Env, job.Command); err != nil {
		return fail(&result, startedAt, domain.FailureJobExit, "rejected: "+err.Error())
	}

	outputRoot, err := os.MkdirTemp("", "computehive-output-*")
	if err != nil {
		return fail(&result, startedAt, domain.FailureInfra, "create output directory: "+err.Error())
	}
	outDir := filepath.Join(outputRoot, "out")
	if err := os.Mkdir(outDir, 0o700); err != nil {
		return fail(&result, startedAt, domain.FailureInfra, "create work directory: "+err.Error())
	}
	user, err := e.resolveUser()
	if err != nil {
		return fail(&result, startedAt, domain.FailureInfra, err.Error())
	}
	if !strings.HasPrefix(user, strconv.Itoa(os.Getuid())+":") {
		_ = os.Chmod(outDir, 0o777) // the job runs as a different uid than the worker: it must be able to write here
	}
	result.CleanupDir = outputRoot
	result.OutputDirHostPath = outDir

	if strings.TrimSpace(bundle.ArchivePath) != "" {
		tarInfo, err := artifact.InspectImageTar(bundle.ArchivePath)
		if err != nil {
			return fail(&result, startedAt, domain.FailureJobExit, "invalid image archive: "+err.Error())
		}
		if err := tarInfo.AllowedTags(e.sbc().LoadPrefixes, job.ImageRef); err != nil {
			return fail(&result, startedAt, domain.FailureJobExit, err.Error())
		}
		defer e.cleanupImages(tarInfo.RepoTags)
		loadStdout, loadStderr, err := e.runDockerCommand(ctx, "load", "-i", bundle.ArchivePath)
		if err != nil {
			result.Stdout, result.Stderr = loadStdout, loadStderr
			return fail(&result, startedAt, domain.FailureInfra, "docker load failed: "+err.Error())
		}
		if _, _, err := e.runDockerCommand(ctx, "image", "inspect", job.ImageRef); err != nil {
			return fail(&result, startedAt, domain.FailureInfra, "loaded archive does not contain expected image_ref: "+job.ImageRef)
		}
	} else if err := e.sbc().Images.Check(job.ImageRef); err != nil {
		return fail(&result, startedAt, domain.FailureJobExit, err.Error())
	}

	attach, err := e.net.Attach(ctx, job)
	if err != nil {
		return fail(&result, startedAt, domain.FailureInfra, "network setup: "+err.Error())
	}
	if attach.Cleanup != nil {
		defer attach.Cleanup()
	}

	args, err := e.buildRunArgsNet(job, outDir, user, attach)
	if err != nil {
		return fail(&result, startedAt, domain.FailureInfra, err.Error())
	}
	if err := auditRunArgs(args, job.ImageRef, e.expect(outDir, attach.Name)); err != nil {
		return fail(&result, startedAt, domain.FailureInfra, err.Error())
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var quotaBytes atomic.Int64
	go watchDirSize(runCtx, outDir, e.sbc().OutputMaxBytes, e.sbc().OutputMaxEntries, e.sbc().OutputPoll, func(b int64, n int) {
		quotaBytes.Store(max(b, 1))
		cancelRun()
	})

	cmd := exec.CommandContext(runCtx, e.binary, args...)
	cmd.Env = e.cmdEnv()
	stdout, stderr := newLimitedBuffer(e.sbc().MaxLogBytes), newLimitedBuffer(e.sbc().MaxLogBytes)
	cmd.Stdout, cmd.Stderr = stdout, stderr

	name := containerName(e.workerID, job.ID)
	e.forceRemove(name) // a leftover from an earlier attempt would make `docker run --name` fail

	runErr := cmd.Run()
	result.Stdout, result.Stderr = stdout.String(), stderr.String()
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()

	// Cancelling the context only kills the `docker run` client; the container keeps
	// running unless we stop it ourselves (timeouts, lease revocation, shutdown, quota).
	if runCtx.Err() != nil {
		e.stopContainer(name)
	}
	// We remove containers ourselves instead of using `docker run --rm`: --rm races with an
	// explicit `docker rm` and can leave the container in a "Dead" state that holds disk forever.
	e.forceRemove(name)

	if runErr != nil {
		result.Error = runErr.Error()
		switch exitErr := runErr.(type) {
		case *exec.ExitError:
			result.ExitCode = exitErr.ExitCode()
			switch {
			case quotaBytes.Load() > 0 && ctx.Err() == nil:
				result.FailureReason = domain.FailureJobExit
				result.Error = fmt.Sprintf("killed: work directory exceeded its %d MB limit (had %d MB)", e.sbc().OutputMaxBytes>>20, quotaBytes.Load()>>20)
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

func (e *DockerExecutor) expect(hostOutputDir, network string) auditExpect {
	return auditExpect{
		mount:    hostOutputDir + ":" + e.outputCollectionDir,
		runtime:  e.sbc().Runtime,
		allowGPU: e.allowGPUJobs,
		networkAllowed: func(n string) bool {
			return n == "none" || (n == network && e.net.Owns(n))
		},
	}
}

func (e *DockerExecutor) cmdEnv() []string {
	env := os.Environ()
	if e.sbc().DockerConfigDir != "" {
		env = append(env, "DOCKER_CONFIG="+e.sbc().DockerConfigDir)
	}
	return env
}

// buildRunArgs builds a job container with no network.
func (e *DockerExecutor) buildRunArgs(job domain.Job, hostOutputDir string) ([]string, error) {
	user, err := e.resolveUser()
	if err != nil {
		return nil, err
	}
	return e.buildRunArgsNet(job, hostOutputDir, user, NetworkAttachment{Name: "none"})
}

func (e *DockerExecutor) buildRunArgsNet(job domain.Job, hostOutputDir, user string, attach NetworkAttachment) ([]string, error) {
	job.Normalize()
	if err := job.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateImageRef(job.ImageRef); err != nil {
		return nil, err
	}
	if err := ValidateJobFields(job.Env, job.Command); err != nil {
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
	network := attach.Name
	if network == "" {
		network = "none"
	}

	args := []string{
		"run",
		"--pull", "never", // never fetch images: that would use the contributor's registry access
		"--network", network,
		"--name", containerName(e.workerID, job.ID),
		"--label", "computehive.worker.id=" + e.workerID,
		"--label", "computehive.job.id=" + job.ID,
		"--user", user,
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"--read-only",
		"--init",
		"--tmpfs", fmt.Sprintf("/tmp:rw,noexec,nosuid,nodev,size=%dm", e.sbc().TmpfsMB),
		"-v", hostOutputDir + ":" + e.outputCollectionDir,
	}
	if e.sbc().Runtime != "" {
		args = append(args, "--runtime", e.sbc().Runtime)
	}
	if e.sbc().NoFile > 0 {
		args = append(args, "--ulimit", fmt.Sprintf("nofile=%d:%d", e.sbc().NoFile, e.sbc().NoFile))
	}
	args = append(args, "--ulimit", "core=0:0")
	if e.sbc().OutputMaxBytes > 0 {
		args = append(args, "--ulimit", fmt.Sprintf("fsize=%d:%d", e.sbc().OutputMaxBytes, e.sbc().OutputMaxBytes))
	}

	cpus := job.CPUCores
	if cpus <= 0 {
		cpus = e.limits.DefaultCPUs
	}
	memoryMB := job.MemoryMB
	if memoryMB <= 0 {
		memoryMB = e.limits.DefaultMemoryMB
	}
	if cpus > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(cpus, 'f', -1, 64))
	}
	if memoryMB > 0 {
		// memory-swap equal to memory means zero swap: the container cannot spill past its cap onto the host's swap.
		args = append(args, "--memory", fmt.Sprintf("%dm", memoryMB), "--memory-swap", fmt.Sprintf("%dm", memoryMB))
	}
	if e.limits.PidsLimit > 0 {
		args = append(args, "--pids-limit", strconv.Itoa(e.limits.PidsLimit))
	}
	if job.GPU {
		args = append(args, "--gpus", "all")
	}

	env := map[string]string{"HOME": "/tmp", "TMPDIR": "/tmp"}
	for k, v := range attach.Env {
		env[k] = v
	}
	for k, v := range job.Env { // the job may override HOME/TMPDIR, but never the proxy or the reserved COMPUTEHIVE_ names
		if _, fromNet := attach.Env[k]; !fromNet {
			env[k] = v
		}
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "-e", key+"="+env[key])
	}
	args = append(args, "-e", "COMPUTEHIVE_OUTPUT_DIR="+e.outputCollectionDir)

	args = append(args, job.ImageRef)
	args = append(args, job.Command...)
	return args, nil
}

func (e *DockerExecutor) dockerCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, e.binary, args...)
	cmd.Env = e.cmdEnv()
	return cmd
}

func (e *DockerExecutor) runDockerCommand(ctx context.Context, args ...string) (string, string, error) {
	cmd := e.dockerCmd(ctx, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// stopContainer gives the container StopGrace to exit after SIGTERM, then SIGKILLs it.
func (e *DockerExecutor) stopContainer(name string) {
	e.sbMu.RLock()
	grace := max(int(e.limits.StopGrace.Seconds()), 0)
	e.sbMu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(grace+30)*time.Second)
	defer cancel()
	_, _, _ = e.runDockerCommand(ctx, "stop", "-t", strconv.Itoa(grace), name)
}

// CleanupOrphans removes containers left behind by a previous run of this worker (for example
// after a crash mid-job). It returns how many it removed. Call it once at startup.
func (e *DockerExecutor) CleanupOrphans(ctx context.Context) (int, error) {
	out, _, err := e.runDockerCommand(ctx, "ps", "-a", "-q", "--filter", "label=computehive.worker.id="+e.workerID)
	if err != nil {
		return 0, fmt.Errorf("list orphaned containers: %w", err)
	}
	ids := strings.Fields(out)
	if len(ids) == 0 {
		return 0, nil
	}
	if _, stderr, err := e.runDockerCommand(ctx, append([]string{"rm", "-f", "-v"}, ids...)...); err != nil {
		return 0, fmt.Errorf("remove orphaned containers: %w: %s", err, strings.TrimSpace(stderr))
	}
	return len(ids), nil
}

// forceRemove kills and removes a container by name, ignoring "no such container".
func (e *DockerExecutor) forceRemove(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, _ = e.runDockerCommand(ctx, "rm", "-f", "-v", name)
}

// cleanupImages removes every tag a job's archive created, so nothing it shipped stays on the machine.
func (e *DockerExecutor) cleanupImages(tags []string) {
	if len(tags) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _, _ = e.runDockerCommand(ctx, append([]string{"image", "rm", "-f"}, tags...)...)
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
