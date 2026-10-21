package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"coordinator/internal/config"
	grpcserver "coordinator/internal/grpc"
	"coordinator/internal/httpapi"
	"coordinator/internal/queue"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "coordinator")

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
	q := queue.New(rdb, qcfg)
	svc := grpcserver.NewServer(q, cfg, log)

	// Failure detection. All state is in Redis, so on startup the first tick recovers
	// whatever was in flight when the previous coordinator died.
	go q.RunReaper(ctx, log, nil)
	log.Info("failure detection running",
		"worker_dead_after", cfg.WorkerDeadAfter.String(), "reap_interval", cfg.ReapInterval.String(),
		"max_requeue_latency", cfg.RequeueBound().String())

	handler, err := httpapi.New(cfg.ArtifactDir, cfg.ArtifactMaxBytes, func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	if err != nil {
		log.Error("http init failed", "error", err)
		os.Exit(1)
	}
	httpServer := &http.Server{Addr: cfg.HTTPListenAddr(), Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("coordinator HTTP server listening", "addr", cfg.HTTPListenAddr(), "artifact_dir", cfg.ArtifactDir)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
