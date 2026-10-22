// Command computehive is the non-GUI client for a ComputeHive cluster. It talks
// to the same gRPC API as the Tauri desktop app.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	pb "coordinator/pkg/pb"
)

const usage = `computehive - submit and inspect jobs on a ComputeHive cluster

Usage:
  computehive [global flags] <command> [args]

Commands:
  submit <dir>      Build <dir> (Dockerfile) into an image, upload it, submit a job
  status <job-id>   Show job state, attempt, queue position and assigned worker
  cancel <job-id>   Cancel a queued or running job
  admin enroll      Mint an expiring token that lets a new worker enrol
  admin revoke <id> Cut a worker off and requeue its jobs
  result <job-id>   Show the job result (exit code, output excerpts)
  logs <job-id>     Show captured stdout/stderr of a finished job; with --follow, stream a running job's output
  jobs              List recent jobs (--limit N, --status running|failed|...)
  explain <job-id>  Show why a job went to the worker it did (or why it is still waiting)
  workers           List registered workers
  worker join       Run a worker on this machine (checks prerequisites first)
  worker status|pause|resume|kill|history|policy   control the worker on this machine

Global flags (or env):
  --coordinator ADDR   gRPC address        (COMPUTEHIVE_COORDINATOR, default localhost:50051)
  --http URL           artifact upload URL (COMPUTEHIVE_HTTP, default http://localhost:8080)
  --token TOKEN        API token            (COMPUTEHIVE_TOKEN, or .computehive/api.token)
  --ca FILE            coordinator CA cert  (COMPUTEHIVE_CA, or .computehive/ca.crt)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type globals struct {
	coordinator string
	httpURL     string
	token       string // API token
	ca          string // CA certificate that signed the coordinator's certificate
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func run(args []string, out, errOut io.Writer) int {
	g := globals{}
	fs := flag.NewFlagSet("computehive", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() { fmt.Fprint(errOut, usage) }
	fs.StringVar(&g.coordinator, "coordinator", envOr("COMPUTEHIVE_COORDINATOR", "localhost:50051"), "")
	fs.StringVar(&g.httpURL, "http", envOr("COMPUTEHIVE_HTTP", ""), "")
	fs.StringVar(&g.token, "token", envOr("COMPUTEHIVE_TOKEN", ""), "")
	fs.StringVar(&g.ca, "ca", envOr("COMPUTEHIVE_CA", ""), "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if g.httpURL == "" { // https when a CA is configured (the normal case), http for plain development setups
		g.httpURL = "http://localhost:8080"
		if g.caFile() != "" {
			g.httpURL = "https://localhost:8080"
		}
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return 2
	}

	ctx, stop := signalContext()
	defer stop()

	var err error
	switch rest[0] {
	case "submit":
		err = cmdSubmit(ctx, g, rest[1:], out)
	case "status":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdStatus(ctx, c, rest[1:], out) })
	case "cancel":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdCancel(ctx, c, rest[1:], out) })
	case "result":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdResult(ctx, g, c, rest[1:], out, false) })
	case "logs":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdLogs(ctx, g, c, rest[1:], out) })
	case "jobs":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdJobs(ctx, c, rest[1:], out) })
	case "explain":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdExplain(ctx, c, rest[1:], out) })
	case "workers":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdWorkers(ctx, c, out) })
	case "worker":
		err = cmdWorker(ctx, g, rest[1:], out, errOut)
	case "admin":
		err = cmdAdmin(ctx, g, rest[1:], out, errOut)
	case "help":
		fs.Usage()
	default:
		fmt.Fprintf(errOut, "unknown command %q\n\n", rest[0])
		fs.Usage()
		return 2
	}
	if err != nil {
		fmt.Fprintf(errOut, "error: %v\n", err)
		return 1
	}
	return 0
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func withClient(g globals, fn func(pb.ClientServiceClient) error) error {
	conn, err := g.dial()
	if err != nil {
		return fmt.Errorf("connect to coordinator %s: %w", g.coordinator, err)
	}
	defer conn.Close()
	return friendly(fn(pb.NewClientServiceClient(conn)))
}

func jobIDArg(args []string, name string) (string, error) {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return "", fmt.Errorf("usage: computehive %s <job-id>", name)
	}
	return args[0], nil
}

func cmdStatus(ctx context.Context, c pb.ClientServiceClient, args []string, out io.Writer) error {
	id, err := jobIDArg(args, "status")
	if err != nil {
		return err
	}
	resp, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: &pb.JobID{Value: id}})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "job:      %s\nstatus:   %s\nworker:   %s\nattempt:  %d of %d\nqueue #:  %d\n",
		id, shortStatus(resp.GetStatus()), orDash(resp.GetAssignedWorkerId()), resp.GetAttempt(), resp.GetMaxAttempts(), resp.GetQueueSeq())
	if resp.GetLastFailure() != "" {
		fmt.Fprintf(out, "last failure: %s\n", resp.GetLastFailure())
	}
	if v := resp.GetVerification(); v != "" {
		fmt.Fprintf(out, "verified: %s\n", v)
	}
	return nil
}

func cmdCancel(ctx context.Context, c pb.ClientServiceClient, args []string, out io.Writer) error {
	id, err := jobIDArg(args, "cancel")
	if err != nil {
		return err
	}
	resp, err := c.CancelJob(ctx, &pb.CancelJobRequest{JobId: &pb.JobID{Value: id}})
	if err != nil {
		return err
	}
	if resp.GetDisposition() == "duplicate" {
		fmt.Fprintf(out, "job %s had already finished; nothing to cancel\n", id)
		return nil
	}
	fmt.Fprintf(out, "job %s canceled\n", id)
	return nil
}

func cmdResult(ctx context.Context, g globals, c pb.ClientServiceClient, args []string, out io.Writer, logsOnly bool) error {
	fs := flag.NewFlagSet("result", flag.ContinueOnError)
	wait := fs.Bool("wait", false, "poll until the job reaches a terminal state")
	timeout := fs.Duration("timeout", 10*time.Minute, "give up waiting after this long")
	download := fs.String("download", "", "save the job's output files into this directory (verified against their SHA-256)")
	name := "result"
	if logsOnly {
		name = "logs"
	}
	// Accept flags before or after the job id.
	var id string
	var flagArgs []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") || len(flagArgs) > 0 && id != "" {
			flagArgs = append(flagArgs, a)
		} else if id == "" {
			id = a
		}
	}
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("usage: computehive %s <job-id> [--wait]", name)
	}

	deadline := time.Now().Add(*timeout)
	for {
		resp, err := c.GetJobResult(ctx, &pb.GetJobResultRequest{JobId: &pb.JobID{Value: id}})
		if err != nil {
			return err
		}
		terminal := resp.GetStatus() == pb.Status_STATUS_SUCCEEDED || resp.GetStatus() == pb.Status_STATUS_FAILED ||
			resp.GetStatus() == pb.Status_STATUS_CANCELED || resp.GetStatus() == pb.Status_STATUS_DEAD
		if resp.GetResultAvailable() && (terminal || !*wait) {
			printResult(out, id, resp, logsOnly)
			if *download != "" {
				if err := downloadOutputs(ctx, g, resp.GetResult().GetOutputArtifacts(), *download, out); err != nil {
					return err
				}
			}
			// Waiting is how scripts use this (`submit --wait && next-step`), so a job that did not
			// succeed must be a non-zero exit. Merely viewing a result (no --wait) stays exit 0.
			if *wait && resp.GetStatus() != pb.Status_STATUS_SUCCEEDED {
				return errNonSuccess
			}
			return nil
		}
		if terminal { // DEAD / CANCELED jobs finish without a result
			fmt.Fprintf(out, "job %s ended: %s (no result; see `computehive status %s`)\n", id, shortStatus(resp.GetStatus()), id)
			return errNonSuccess
		}
		if !*wait {
			fmt.Fprintf(out, "job %s: %s (no result yet)\n", id, shortStatus(resp.GetStatus()))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for job %s (status %s)", *timeout, id, shortStatus(resp.GetStatus()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func printResult(out io.Writer, id string, resp *pb.GetJobResultResponse, logsOnly bool) {
	r := resp.GetResult()
	if !logsOnly {
		fmt.Fprintf(out, "job:       %s\nstatus:    %s\nexit code: %d\n", id, shortStatus(resp.GetStatus()), r.GetExitCode())
		if r.GetFinishedAtUnix() > 0 {
			fmt.Fprintf(out, "finished:  %s\n", time.Unix(r.GetFinishedAtUnix(), 0).UTC().Format(time.RFC3339))
		}
		for _, a := range r.GetOutputArtifacts() {
			fmt.Fprintf(out, "output:    %s (%d bytes) %s\n", a.GetRelativePath(), a.GetSizeBytes(), firstNonEmpty(a.GetPublicUrl(), a.GetUri()))
		}
		fmt.Fprintln(out, "--- stdout (excerpt) ---")
	}
	// Job output is untrusted text: strip terminal control sequences before it reaches the user's terminal.
	stdout, stderr := sanitizeTerminal(r.GetStdoutExcerpt()), sanitizeTerminal(r.GetStderrExcerpt())
	fmt.Fprint(out, stdout)
	if !strings.HasSuffix(stdout, "\n") && stdout != "" {
		fmt.Fprintln(out)
	}
	if stderr != "" {
		fmt.Fprintln(out, "--- stderr (excerpt) ---")
		fmt.Fprint(out, stderr)
		if !strings.HasSuffix(stderr, "\n") {
			fmt.Fprintln(out)
		}
	}
}

func cmdWorkers(ctx context.Context, c pb.ClientServiceClient, out io.Writer) error {
	resp, err := c.ListWorkers(ctx, &pb.ListWorkersRequest{})
	if err != nil {
		return err
	}
	if len(resp.GetWorkers()) == 0 {
		fmt.Fprintln(out, "no workers registered")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "WORKER\tSTATE\tCPU\tMEM(MB)\tGPU\tRAM%\tCPU%\tDISK%\tJOBS\tPLATFORM\tRELIABILITY\tLAST HEARTBEAT")
	for _, w := range resp.GetWorkers() {
		res, u := w.GetResources(), w.GetUsage()
		hb := "-"
		if w.GetLastHeartbeatUnix() > 0 {
			hb = time.Since(time.Unix(w.GetLastHeartbeatUnix(), 0)).Round(time.Second).String() + " ago"
		}
		state := "ready"
		switch {
		case !w.GetAlive():
			state = "dead"
		case w.GetOverloaded():
			state = "overloaded"
			if r := strings.Join(u.GetOverloadReasons(), ","); r != "" {
				state += "(" + r + ")"
			}
		case w.GetDraining():
			state = "draining"
		}
		ram, cpu, dsk := "-", "-", "-"
		if u != nil {
			ram, cpu = fmt.Sprintf("%.0f", u.GetMemUsedPercent()), fmt.Sprintf("%.0f", u.GetCpuPercent())
			dsk = fmt.Sprintf("%.0f", max(u.GetWorkDiskUsedPercent(), u.GetDockerDiskUsedPercent()))
		}
		platform := orDash(w.GetOs())
		if w.GetArch() != "" {
			platform += "/" + w.GetArch()
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%d\t%s\t%.2f\t%s\n",
			w.GetWorkerId().GetValue(), state, res.GetCpuCores(), res.GetMemoryMb(), res.GetGpuCount(), ram, cpu, dsk, len(w.GetRunningJobs()),
			platform, w.GetReliability(), hb)
	}
	return tw.Flush()
}

// cmdExplain prints the scheduler's reasoning for a job: every worker with its verdict, and for the
// chosen one the weighted score breakdown.
func cmdExplain(ctx context.Context, c pb.ClientServiceClient, args []string, out io.Writer) error {
	id, err := jobIDArg(args, "explain")
	if err != nil {
		return err
	}
	resp, err := c.ExplainJob(ctx, &pb.ExplainJobRequest{JobId: &pb.JobID{Value: id}})
	if err != nil {
		return err
	}
	kind := "live evaluation of the current cluster"
	if resp.GetRecorded() {
		kind = "decision recorded when the job was assigned"
	}
	fmt.Fprintf(out, "job %s (%s)\n%s\n  %s\n", id, shortStatus(resp.GetStatus()), kind, resp.GetSummary())
	for _, e := range resp.GetEvaluations() {
		name := e.GetWorkerId().GetValue()
		switch {
		case !e.GetEligible():
			fmt.Fprintf(out, "\n  x %s: rejected\n", name)
			for _, r := range e.GetReasons() {
				fmt.Fprintf(out, "      - %s\n", r)
			}
		default:
			mark, note := " ", ""
			if e.GetChosen() {
				mark = "*"
			}
			if e.GetBusy() {
				note = " (busy)"
			}
			fmt.Fprintf(out, "\n  %s %s: score %.3f%s\n", mark, name, e.GetScore(), note)
			parts := make([]string, 0, len(e.GetParts()))
			for k := range e.GetParts() {
				parts = append(parts, k)
			}
			sort.Strings(parts)
			for _, k := range parts {
				fmt.Fprintf(out, "      %-12s %.3f  (weight %.2f)\n", k, e.GetParts()[k], resp.GetWeights()[k])
			}
		}
	}
	return nil
}

func cmdSubmit(ctx context.Context, g globals, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	cpu := fs.Int("cpu", 1, "CPU cores to request")
	mem := fs.Int("mem", 256, "memory to request, in MB")
	gpu := fs.Int("gpu", 0, "GPUs to request")
	timeout := fs.Int("timeout", 300, "max runtime in seconds")
	wait := fs.Bool("wait", false, "wait for the job and print its result")
	verify := fs.Bool("verify", false, "run on two different workers and accept the result only if their outputs match (the job must be deterministic)")
	isolation := fs.String("isolation", "", "only run on workers providing this isolation: gvisor")
	user := fs.String("user", "", "submitting user name (for fairness and per-user limits)")
	maxAttempts := fs.Int("max-attempts", 0, "attempts before the job is parked as dead (0 = cluster default)")
	rebuild := fs.Bool("rebuild", false, "rebuild the image even if the directory is unchanged since the last submit")
	minDisk := fs.Int("min-disk", 0, "free disk the worker must have, in MB")
	osName := fs.String("os", "", "only run on workers with this operating system (linux, darwin, windows)")
	arch := fs.String("arch", "", "only run on workers with this CPU architecture (amd64, arm64)")
	var netHosts, envs, labels multiFlag
	fs.Var(&labels, "label", "a label the worker must carry, for example fast-disk (repeatable)")
	fs.Var(&netHosts, "network-host", "a host the job needs to reach, for example pypi.org (repeatable; only workers whose contributor allows it will run the job)")
	fs.Var(&envs, "env", "KEY=VALUE environment variable (repeatable)")

	// Allow the directory before or after the flags.
	var dir string
	var flagArgs []string
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "-") && dir == "" {
			dir = args[i]
			continue
		}
		flagArgs = append(flagArgs, args[i])
	}
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if dir == "" {
		return errors.New("usage: computehive submit <dir> [--cpu N] [--mem MB] [--timeout S] [--wait] [-- command...]")
	}
	command := fs.Args()

	// Job defaults from the directory (Dockerfile LABELs, computehive.yaml); explicit flags win.
	man, err := loadManifest(dir)
	if err != nil {
		return err
	}
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	var merr error
	num := func(flagName, key string, dst *int) {
		if !explicit[flagName] {
			if v, err := man.integer(key, *dst); err != nil {
				merr = err
			} else {
				*dst = v
			}
		}
	}
	num("cpu", "cpu", cpu)
	num("mem", "mem", mem)
	num("gpu", "gpu", gpu)
	num("timeout", "timeout", timeout)
	num("min-disk", "min_disk", minDisk)
	num("max-attempts", "max_attempts", maxAttempts)
	if merr != nil {
		return merr
	}
	if !explicit["verify"] {
		if *verify, err = man.boolean("verify", *verify); err != nil {
			return err
		}
	}
	if !explicit["os"] {
		*osName = man.str("os", *osName)
	}
	if !explicit["arch"] {
		*arch = man.str("arch", *arch)
	}
	if !explicit["isolation"] {
		*isolation = man.str("isolation", *isolation)
	}
	if !explicit["user"] {
		*user = man.str("user", *user)
	}
	if !explicit["label"] {
		labels = append(labels, man.list("labels")...)
	}
	if !explicit["network-host"] {
		netHosts = append(netHosts, man.list("network_hosts")...)
	}

	if err := checkDocker(ctx); err != nil {
		return err
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(absDir, "Dockerfile")); err != nil {
		return fmt.Errorf("%s has no Dockerfile: a job directory must contain one", absDir)
	}

	tag, tarPath, cleanupImage, err := prepareImage(ctx, absDir, *rebuild, out)
	if err != nil {
		return err
	}
	defer cleanupImage()

	sha, size, err := hashFile(tarPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "uploading artifact sha256:%s (%d bytes) ...\n", sha[:12], size)
	if err := uploadArtifact(ctx, g, g.httpURL, tarPath, sha); err != nil {
		return err
	}

	env := map[string]string{}
	for _, kv := range envs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return fmt.Errorf("invalid --env %q: want KEY=VALUE", kv)
		}
		env[k] = v
	}

	var resp *pb.SubmitJobResponse
	err = withClient(g, func(c pb.ClientServiceClient) error {
		var callErr error
		resp, callErr = c.SubmitJob(ctx, &pb.SubmitJobRequest{
			ContainerImage:    tag,
			Command:           command,
			RequiredResources: &pb.ResourceSpec{CpuCores: int32(*cpu), MemoryMb: int32(*mem), GpuCount: int32(*gpu)},
			Environment:       env,
			MaxRuntimeSeconds: int32(*timeout),
			ArtifactUrl:       "computehive-artifact://" + sha,
			ArtifactSha256:    sha,
			Verify:            *verify,
			RequiredIsolation: *isolation,
			User:              *user,
			MaxAttempts:       int32(*maxAttempts),
			NetworkHosts:      netHosts,
			MinDiskMb:         int32(*minDisk),
			Os:                *osName,
			Arch:              *arch,
			Labels:            labels,
		})
		return callErr
	})
	if err != nil {
		return fmt.Errorf("submit job: %w", err)
	}
	id := resp.GetJobId().GetValue()
	fmt.Fprintf(out, "submitted job %s\n", id)
	if !*wait {
		fmt.Fprintf(out, "  computehive status %s\n  computehive result %s --wait\n", id, id)
		return nil
	}
	return withClient(g, func(c pb.ClientServiceClient) error {
		return cmdResult(ctx, g, c, []string{id, "--wait"}, out, false)
	})
}

var errNonSuccess = errors.New("job did not succeed")

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func checkDocker(ctx context.Context) error {
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker CLI not found in PATH: install Docker (https://docs.docker.com/get-docker/)")
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(cctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		msg := strings.TrimSpace(string(out))
		if strings.Contains(msg, "permission denied") {
			return errors.New("permission denied talking to the Docker daemon: add your user to the 'docker' group (sudo usermod -aG docker $USER) and re-login")
		}
		return fmt.Errorf("docker daemon is not reachable (is it running?): %s", msg)
	}
	return nil
}

func runStreaming(ctx context.Context, w io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(w, &buf)
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(buf.String()))
	}
	return nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func uploadArtifact(ctx context.Context, g globals, baseURL, path, sha string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(baseURL, "/")+"/artifacts/"+sha, f)
	if err != nil {
		return err
	}
	req.ContentLength = st.Size()
	if tok := g.apiToken(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	hc, err := g.httpClient()
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("upload artifact to %s: %w", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("upload artifact: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func shortStatus(s pb.Status) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "STATUS_"))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// downloadOutputs saves each output file under dir, checking its SHA-256 and refusing any path that
// would land outside dir (a hostile job controls the file names it reports).
func downloadOutputs(ctx context.Context, g globals, artifacts []*pb.OutputArtifact, dir string, out io.Writer) error {
	if len(artifacts) == 0 {
		fmt.Fprintln(out, "the job produced no output files")
		return nil
	}
	hc, err := g.httpClient()
	if err != nil {
		return err
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	for _, a := range artifacts {
		rest, ok := strings.CutPrefix(a.GetUri(), "computehive-output://")
		if !ok {
			fmt.Fprintf(out, "skipping %s: stored outside this cluster (%s)\n", a.GetRelativePath(), firstNonEmpty(a.GetPublicUrl(), a.GetUri()))
			continue
		}
		rel := filepath.FromSlash(a.GetRelativePath())
		dest := filepath.Join(root, rel)
		if !filepath.IsLocal(rel) || !strings.HasPrefix(dest, root+string(filepath.Separator)) {
			return fmt.Errorf("refusing output path %q: it would leave %s", a.GetRelativePath(), root)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(g.httpURL, "/")+"/outputs/"+rest, nil)
		if err != nil {
			return err
		}
		if tok := g.apiToken(); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := hc.Do(req)
		if err != nil {
			return friendly(err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("download %s: %s", a.GetRelativePath(), resp.Status)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			resp.Body.Close()
			return err
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			resp.Body.Close()
			return err
		}
		h := sha256.New()
		_, cerr := io.Copy(io.MultiWriter(f, h), resp.Body)
		resp.Body.Close()
		if cerr != nil || f.Close() != nil {
			return fmt.Errorf("download %s: %v", a.GetRelativePath(), cerr)
		}
		if got := hex.EncodeToString(h.Sum(nil)); a.GetSha256() != "" && got != a.GetSha256() {
			_ = os.Remove(dest)
			return fmt.Errorf("%s failed verification: sha256 %s, expected %s (file removed)", a.GetRelativePath(), got, a.GetSha256())
		}
		fmt.Fprintf(out, "saved %s (%d bytes, sha256 verified)\n", dest, a.GetSizeBytes())
	}
	return nil
}
