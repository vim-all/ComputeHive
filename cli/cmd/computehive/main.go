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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = `computehive - submit and inspect jobs on a ComputeHive cluster

Usage:
  computehive [global flags] <command> [args]

Commands:
  submit <dir>      Build <dir> (Dockerfile) into an image, upload it, submit a job
  status <job-id>   Show job state and assigned worker
  result <job-id>   Show the job result (exit code, output excerpts)
  logs <job-id>     Show captured stdout/stderr excerpts of a finished job
  workers           List registered workers
  worker join       Run a worker on this machine (checks prerequisites first)

Global flags (or env):
  --coordinator ADDR   gRPC address        (COMPUTEHIVE_COORDINATOR, default localhost:50051)
  --http URL           artifact upload URL (COMPUTEHIVE_HTTP, default http://localhost:8080)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type globals struct {
	coordinator string
	httpURL     string
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
	fs.StringVar(&g.httpURL, "http", envOr("COMPUTEHIVE_HTTP", "http://localhost:8080"), "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
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
	case "result":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdResult(ctx, c, rest[1:], out, false) })
	case "logs":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdResult(ctx, c, rest[1:], out, true) })
	case "workers":
		err = withClient(g, func(c pb.ClientServiceClient) error { return cmdWorkers(ctx, c, out) })
	case "worker":
		err = cmdWorker(ctx, g, rest[1:], out, errOut)
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
	conn, err := grpc.NewClient(g.coordinator, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to coordinator %s: %w", g.coordinator, err)
	}
	defer conn.Close()
	return fn(pb.NewClientServiceClient(conn))
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
	fmt.Fprintf(out, "job:    %s\nstatus: %s\nworker: %s\n", id, shortStatus(resp.GetStatus()), orDash(resp.GetAssignedWorkerId()))
	return nil
}

func cmdResult(ctx context.Context, c pb.ClientServiceClient, args []string, out io.Writer, logsOnly bool) error {
	fs := flag.NewFlagSet("result", flag.ContinueOnError)
	wait := fs.Bool("wait", false, "poll until the job reaches a terminal state")
	timeout := fs.Duration("timeout", 10*time.Minute, "give up waiting after this long")
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
		terminal := resp.GetStatus() == pb.Status_STATUS_SUCCEEDED || resp.GetStatus() == pb.Status_STATUS_FAILED || resp.GetStatus() == pb.Status_STATUS_CANCELED
		if resp.GetResultAvailable() && (terminal || !*wait) {
			printResult(out, id, resp, logsOnly)
			return nil
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
	fmt.Fprint(out, r.GetStdoutExcerpt())
	if !strings.HasSuffix(r.GetStdoutExcerpt(), "\n") && r.GetStdoutExcerpt() != "" {
		fmt.Fprintln(out)
	}
	if r.GetStderrExcerpt() != "" {
		fmt.Fprintln(out, "--- stderr (excerpt) ---")
		fmt.Fprint(out, r.GetStderrExcerpt())
		if !strings.HasSuffix(r.GetStderrExcerpt(), "\n") {
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
	fmt.Fprintln(tw, "WORKER\tALIVE\tCPU\tMEM(MB)\tGPU\tJOBS\tLAST HEARTBEAT\tVERSION")
	for _, w := range resp.GetWorkers() {
		res := w.GetResources()
		hb := "-"
		if w.GetLastHeartbeatUnix() > 0 {
			hb = time.Since(time.Unix(w.GetLastHeartbeatUnix(), 0)).Round(time.Second).String() + " ago"
		}
		fmt.Fprintf(tw, "%s\t%v\t%d\t%d\t%d\t%d\t%s\t%s\n",
			w.GetWorkerId().GetValue(), w.GetAlive(), res.GetCpuCores(), res.GetMemoryMb(), res.GetGpuCount(),
			len(w.GetRunningJobs()), hb, orDash(w.GetVersion()))
	}
	return tw.Flush()
}

func cmdSubmit(ctx context.Context, g globals, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	cpu := fs.Int("cpu", 1, "CPU cores to request")
	mem := fs.Int("mem", 256, "memory to request, in MB")
	gpu := fs.Int("gpu", 0, "GPUs to request")
	timeout := fs.Int("timeout", 300, "max runtime in seconds")
	wait := fs.Bool("wait", false, "wait for the job and print its result")
	var envs multiFlag
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

	tag := "computehive-job:" + fmt.Sprintf("%d", time.Now().UnixNano())
	fmt.Fprintf(out, "building image from %s ...\n", absDir)
	if err := runStreaming(ctx, io.Discard, "docker", "build", "-q", "-t", tag, absDir); err != nil {
		return fmt.Errorf("docker build failed: %w", err)
	}

	tar, err := os.CreateTemp("", "computehive-image-*.tar")
	if err != nil {
		return err
	}
	defer os.Remove(tar.Name())
	tar.Close()
	if err := runStreaming(ctx, io.Discard, "docker", "save", "-o", tar.Name(), tag); err != nil {
		return fmt.Errorf("docker save failed: %w", err)
	}

	sha, size, err := hashFile(tar.Name())
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "uploading artifact sha256:%s (%d bytes) ...\n", sha[:12], size)
	if err := uploadArtifact(ctx, g.httpURL, tar.Name(), sha); err != nil {
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
		return cmdResult(ctx, c, []string{id, "--wait"}, out, false)
	})
}

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

func uploadArtifact(ctx context.Context, baseURL, path, sha string) error {
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
	resp, err := http.DefaultClient.Do(req)
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
