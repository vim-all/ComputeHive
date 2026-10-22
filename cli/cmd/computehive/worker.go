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
	"strconv"
	"strings"
	"syscall"
	"time"
)

const workerUsage = `usage: computehive worker join --coordinator <host:port> (--ca ca.crt | --ca-sha256 <fingerprint>) --token <enrollment token> [--tls-cert C --tls-key K] [--http-url URL] [--worker-bin PATH]

Checks prerequisites (Docker, coordinator reachability, worker binary) and then
runs a worker in the foreground. Stop it with Ctrl-C; it finishes or requeues its
in-flight job before exiting.
`

func cmdWorker(ctx context.Context, _ globals, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, workerUsage+"\n"+controlUsage)
		return errors.New("missing worker subcommand")
	}
	if args[0] != "join" {
		return cmdWorkerControl(args, out, errOut)
	}
	fs := flag.NewFlagSet("worker join", flag.ContinueOnError)
	fs.SetOutput(errOut)
	coordinator := fs.String("coordinator", envOr("COMPUTEHIVE_COORDINATOR", ""), "coordinator gRPC address host:port")
	token := fs.String("token", envOr("ENROLLMENT_TOKEN", ""), "one-time enrollment token from the operator (`computehive admin enroll`)")
	ca := fs.String("ca", envOr("COMPUTEHIVE_CA", ""), "CA certificate that signed the coordinator's certificate (enables TLS)")
	tlsCert := fs.String("tls-cert", "", "client certificate for mutual TLS")
	tlsKey := fs.String("tls-key", "", "client certificate key for mutual TLS")
	stateDir := fs.String("state-dir", "", "where the worker keeps its credential, policy and history (default ~/.local/state/computehive/<id>)")
	workerID := fs.String("worker-id", "", "stable worker id (default: this host's name)")
	httpURL := fs.String("http-url", "", "coordinator HTTP base URL (default http://<coordinator host>:8080)")
	bin := fs.String("worker-bin", "", "path to the worker binary (default: computehive-worker next to this binary, then PATH)")
	caSHA := fs.String("ca-sha256", "", "fetch the cluster CA from the coordinator and trust it only if its SHA-256 fingerprint equals this (printed by `computehive admin enroll`); use instead of --ca")
	httpPort := fs.Int("http-port", 8080, "the coordinator's HTTPS port, where the CA is fetched from (with --ca-sha256)")
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

	// One-line join: fetch the cluster CA and trust it only if it matches the fingerprint the operator gave us.
	host, _, _ := net.SplitHostPort(*coordinator)
	if *ca == "" && *caSHA != "" {
		pemBytes, err := fetchCA(ctx, "https://"+net.JoinHostPort(host, strconv.Itoa(*httpPort))+"/ca.crt", *caSHA)
		if err != nil {
			return err
		}
		if *stateDir == "" {
			id := *workerID
			if id == "" {
				id, _ = os.Hostname()
			}
			home, herr := os.UserHomeDir()
			if herr != nil {
				return herr
			}
			*stateDir = filepath.Join(home, ".local", "state", "computehive", id)
		}
		if err := os.MkdirAll(*stateDir, 0o700); err != nil {
			return err
		}
		*ca = filepath.Join(*stateDir, "ca.crt")
		if err := os.WriteFile(*ca, pemBytes, 0o644); err != nil {
			return err
		}
		fmt.Fprintf(out, "  ok  fetched the cluster CA and verified its fingerprint (sha256 %s...)\n", strings.ToLower(strings.ReplaceAll(*caSHA, ":", ""))[:12])
	}
	if *httpURL == "" && *ca != "" {
		*httpURL = "https://" + net.JoinHostPort(host, strconv.Itoa(*httpPort)) // artifacts and outputs use TLS too
	}

	if *ca == "" {
		fmt.Fprintln(out, "  WARN no --ca given: the connection to the coordinator is NOT encrypted (development only)")
	} else if _, err := os.Stat(*ca); err != nil {
		return fmt.Errorf("--ca %s: %w", *ca, err)
	} else {
		fmt.Fprintf(out, "  ok  TLS with CA %s\n", *ca)
	}

	cmd := exec.CommandContext(ctx, workerPath)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	cmd.Env = append(os.Environ(), "COORDINATOR_ADDR="+*coordinator)
	if *httpURL != "" {
		cmd.Env = append(cmd.Env, "COORDINATOR_HTTP_URL="+*httpURL)
	}
	if *token != "" {
		cmd.Env = append(cmd.Env, "ENROLLMENT_TOKEN="+*token)
	}
	for _, kv := range [][2]string{{"COORDINATOR_TLS_CA", *ca}, {"COORDINATOR_TLS_CERT", *tlsCert}, {"COORDINATOR_TLS_KEY", *tlsKey},
		{"STATE_DIR", *stateDir}, {"WORKER_ID", *workerID}} {
		if kv[1] != "" {
			cmd.Env = append(cmd.Env, kv[0]+"="+kv[1])
		}
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
