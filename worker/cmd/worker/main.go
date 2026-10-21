package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/monitor"
	workeroutput "github.com/vim-all/ComputeHive/worker/internal/output"
	"github.com/vim-all/ComputeHive/worker/internal/resources"
	"github.com/vim-all/ComputeHive/worker/internal/store"
	workerapp "github.com/vim-all/ComputeHive/worker/internal/worker"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		slog.Error("invalid worker configuration", "error", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisStore := store.New(cfg)
	artifactFetcher := artifact.NewFetcherWithSigV4(cfg.ArtifactDownloadTimeout, cfg.ArtifactMaxBytes, artifact.SigV4Config{
		Enabled:         cfg.S3SignRequests,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
		SessionToken:    cfg.S3SessionToken,
		Region:          cfg.S3SigningRegion,
		Service:         cfg.S3SigningService,
	})
	resourceReporter := resources.NewReporter(cfg.DockerBinary)
	outputManager, err := workeroutput.NewManager(workeroutput.Config{
		BucketURL:       cfg.S3BucketURL,
		PublicBucketURL: cfg.S3PublicBucketURL,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
		SessionToken:    cfg.S3SessionToken,
		Region:          cfg.S3SigningRegion,
		Service:         cfg.S3SigningService,
		Timeout:         cfg.ArtifactDownloadTimeout,
		MaxFiles:        cfg.OutputMaxFiles,
		MaxBytes:        cfg.OutputMaxBytes,
	})
	if err != nil {
		logger.Error("invalid output uploader configuration", "error", err)
		os.Exit(2)
	}
	dockerExecutor := executor.NewDockerExecutor(cfg.DockerBinary, cfg.WorkerID, cfg.AllowGPUJobs, cfg.OutputCollectionDir, executor.Limits{
		DefaultMemoryMB: cfg.DefaultJobMemoryMB, DefaultCPUs: cfg.DefaultJobCPUs, PidsLimit: cfg.PidsLimit, StopGrace: cfg.StopGrace,
	})
	if n, err := dockerExecutor.CleanupOrphans(ctx); err != nil {
		logger.Warn("could not sweep orphaned job containers", "error", err)
	} else if n > 0 {
		logger.Warn("removed job containers left by a previous run", "count", n)
	}
	resourceMonitor := monitor.New(monitor.NewSystemSampler(os.TempDir(), cfg.DockerBinary, cfg.WorkerID), cfg.MonitorConfig(), nil, logger)
	go resourceMonitor.Run(ctx, cfg.SampleInterval)
	agent := workerapp.NewAgent(cfg, redisStore, artifactFetcher, resourceReporter, dockerExecutor, outputManager, resourceMonitor, logger)

	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped unexpectedly", "error", err)
		os.Exit(1)
	}

	logger.Info("worker stopped")
}
