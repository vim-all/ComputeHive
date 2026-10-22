package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"coordinator/internal/auth"
	"coordinator/internal/config"
	grpcserver "coordinator/internal/grpc"
	"coordinator/internal/httpapi"
	"coordinator/internal/queue"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "coordinator")

	if len(os.Args) > 1 && os.Args[1] == "certs" {
		os.Exit(runCerts(os.Args[2:]))
	}

	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Error("invalid config", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb, err := queue.NewRedisClient(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTLS)
	if err != nil {
		log.Error("redis init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Warn("redis close error", "error", err)
		}
	}()

	qcfg := queue.DefaultConfig()
	qcfg.MaxAttempts = cfg.MaxAttempts
	qcfg.BackoffBase, qcfg.BackoffMax = cfg.BackoffBase, cfg.BackoffMax
	qcfg.DeadAfter, qcfg.ReapInterval, qcfg.LeaseGrace = cfg.WorkerDeadAfter, cfg.ReapInterval, cfg.LeaseGrace
	qcfg.OrphanGrace = max(qcfg.OrphanGrace, 5*cfg.HeartbeatInterval)
	qcfg.LimitMemPct, qcfg.LimitCPUPct, qcfg.LimitDiskPct = cfg.LimitRAMPct, cfg.LimitCPUPct, cfg.LimitDiskPct
	qcfg.ReleasePct, qcfg.ReleaseAfter, qcfg.RequireUsage = cfg.ReleasePct, cfg.ReleaseAfter, cfg.RequireUsage
	qcfg.VerifyTimeout = cfg.VerifyTimeout
	if err := qcfg.Validate(); err != nil {
		log.Error("invalid scheduler config", "error", err)
		os.Exit(2)
	}
	q := queue.New(rdb, qcfg)

	acfg := auth.DefaultConfig()
	acfg.APIToken, acfg.Disabled = cfg.APIToken, cfg.AuthDisabled
	acfg.WorkerRPS, acfg.WorkerBurst = cfg.RateWorkerRPS, 2*cfg.RateWorkerRPS
	acfg.ClientRPS, acfg.ClientBurst = cfg.RateClientRPS, 2*cfg.RateClientRPS
	acfg.AnonRPS, acfg.AnonBurst = cfg.RateAnonRPS, 2*cfg.RateAnonRPS
	authSvc := auth.New(rdb, acfg, log)
	if cfg.TLSCertFile == "" {
		log.Warn("TLS is OFF: traffic, including credentials, is unencrypted. Development only.")
	}
	svc := grpcserver.NewServer(q, cfg, log).WithAuth(authSvc)

	// Failure detection. All state is in Redis, so on startup the first tick recovers
	// whatever was in flight when the previous coordinator died.
	go q.RunReaper(ctx, log, nil)
	log.Info("failure detection running",
		"worker_dead_after", cfg.WorkerDeadAfter.String(), "reap_interval", cfg.ReapInterval.String(),
		"max_requeue_latency", cfg.RequeueBound().String())

	handler, err := httpapi.New(cfg.ArtifactDir, cfg.ArtifactMaxBytes, func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}, authSvc, func(ctx context.Context, workerID, jobID string) (int, bool) {
		// Output files are accepted only from the worker that currently holds the job's lease.
		j, err := q.Get(ctx, jobID)
		if err != nil || j == nil || j.Worker != workerID || (j.State != queue.StateAssigned && j.State != queue.StateRunning) {
			return 0, false
		}
		return j.Attempt, true
	})
	if err != nil {
		log.Error("http init failed", "error", err)
		os.Exit(1)
	}
	httpServer := &http.Server{Addr: cfg.HTTPListenAddr(), Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("coordinator HTTP server listening", "addr", cfg.HTTPListenAddr(), "artifact_dir", cfg.ArtifactDir, "tls", cfg.TLSCertFile != "")
		var err error
		if cfg.TLSCertFile != "" {
			tlsCfg, terr := auth.ServerTLS(cfg.TLSCertFile, cfg.TLSKeyFile, "") // artifacts use bearer credentials, not client certificates
			if terr != nil {
				log.Error("http tls", "error", terr)
				os.Exit(1)
			}
			httpServer.TLSConfig = tlsCfg
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "error", err)
			os.Exit(1)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := grpcserver.Start(ctx, cfg, svc); err != nil {
		log.Error("gRPC server stopped", "error", err)
		os.Exit(1)
	}
}

// runCerts implements `coordinator certs`: generate (or extend) a private CA with a server
// certificate and optional client certificates for mutual TLS.
func runCerts(args []string) int {
	fs := flag.NewFlagSet("certs", flag.ContinueOnError)
	out := fs.String("out", "./certs", "directory to write ca.crt, ca.key, server.crt, server.key and client-*.crt/key into")
	hosts := fs.String("hosts", "localhost,127.0.0.1,coordinator", "comma-separated host names / IPs the server certificate must be valid for")
	clients := fs.String("clients", "", "comma-separated worker ids to issue client certificates for (mutual TLS)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	split := func(s string) []string {
		var r []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				r = append(r, p)
			}
		}
		return r
	}
	if err := auth.GenerateCertSet(*out, split(*hosts), split(*clients)); err != nil {
		fmt.Fprintln(os.Stderr, "certs:", err)
		return 1
	}
	fmt.Printf("wrote certificates to %s\n  CA (give to workers and the CLI): %s/ca.crt\n  server: %s/server.crt + server.key (coordinator: TLS_CERT_FILE / TLS_KEY_FILE)\n", *out, *out, *out)
	if len(split(*clients)) > 0 {
		fmt.Printf("  client certificates: %s/client-<id>.crt + .key (worker: COORDINATOR_TLS_CERT / COORDINATOR_TLS_KEY); the CA key stays on the coordinator\n", *out)
	}
	return 0
}
