package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const workerUsage = `usage: computehive worker join --coordinator <host:port> [--token <t>] [--http-url URL] [--worker-bin PATH]

Checks prerequisites (Docker, coordinator reachability, worker binary) and then
runs a worker in the foreground. Stop it with Ctrl-C; it finishes or requeues its
in-flight job before exiting.
`

func cmdWorker(ctx context.Context, _ globals, args []string, out, errOut io.Writer) error {
	if len(args) == 0 || args[0] != "join" {
		fmt.Fprint(errOut, workerUsage)
		return errors.New("unknown worker subcommand: want 'join'")
	}
	fs := flag.NewFlagSet("worker join", flag.ContinueOnError)
	fs.SetOutput(errOut)
	coordinator := fs.String("coordinator", envOr("COMPUTEHIVE_COORDINATOR", ""), "coordinator gRPC address host:port")
	token := fs.String("token", envOr("COMPUTEHIVE_TOKEN", ""), "enrollment token")
	httpURL := fs.String("http-url", "", "coordinator HTTP base URL (default http://<coordinator host>:8080)")
	bin := fs.String("worker-bin", "", "path to the worker binary (default: computehive-worker next to this binary, then PATH)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *coordinator == "" {
		fmt.Fprint(errOut, workerUsage)
		return errors.New("--coordinator is required")
	}
	if _, _, err := net.SplitHostPort(*coordinator); err != nil {
		return fmt.Errorf("--coordinator %q must be host:port (for example 10.0.0.5:50051): %w", *coordinator, err)
	}

	fmt.Fprintln(out, "checking prerequisites ...")
	if err := checkDocker(ctx); err != nil {
		return err
	}
	fmt.Fprintln(out, "  ok  docker daemon reachable")

	conn, err := net.DialTimeout("tcp", *coordinator, 3*time.Second)
	if err != nil {
		return fmt.Errorf("cannot reach coordinator at %s: %w (is it running, and is port 50051 open?)", *coordinator, err)
	}
	_ = conn.Close()
	fmt.Fprintf(out, "  ok  coordinator reachable at %s\n", *coordinator)

	workerPath, err := findWorkerBinary(*bin)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  ok  worker binary %s\n", workerPath)

	if *token != "" {
		fmt.Fprintln(out, "  note token accepted; enrollment tokens are enforced by the coordinator from the security phase onward")
	}

	cmd := exec.CommandContext(ctx, workerPath)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	cmd.Env = append(os.Environ(), "COORDINATOR_ADDR="+*coordinator)
	if *httpURL != "" {
		cmd.Env = append(cmd.Env, "COORDINATOR_HTTP_URL="+*httpURL)
	}
	if *token != "" {
		cmd.Env = append(cmd.Env, "COMPUTEHIVE_TOKEN="+*token)
	}
	cmd.Stdout, cmd.Stderr = out, errOut
	fmt.Fprintln(out, "joining cluster (Ctrl-C to leave) ...")
	if err := cmd.Run(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("worker exited: %w", err)
	}
	return nil
}

func findWorkerBinary(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("--worker-bin %s: %w", explicit, err)
		}
		return explicit, nil
	}
	if self, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(self), "computehive-worker")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	if p, err := exec.LookPath("computehive-worker"); err == nil {
		return p, nil
	}
	return "", errors.New("worker binary not found: run `make build` (puts bin/computehive-worker next to bin/computehive) or pass --worker-bin")
}
