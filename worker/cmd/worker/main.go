package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/control"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/history"
	"github.com/vim-all/ComputeHive/worker/internal/monitor"
	"github.com/vim-all/ComputeHive/worker/internal/netproxy"
	workeroutput "github.com/vim-all/ComputeHive/worker/internal/output"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
	"github.com/vim-all/ComputeHive/worker/internal/resources"
	"github.com/vim-all/ComputeHive/worker/internal/store"
	workerapp "github.com/vim-all/ComputeHive/worker/internal/worker"
)

func main() {
	// `worker proxy-serve` is the egress-filter sidecar that network jobs talk to (see internal/netproxy).
	if len(os.Args) > 1 && os.Args[1] == "proxy-serve" {
		os.Exit(netproxy.RunSidecar(os.Args[2:]))
	}
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
	artifactFetcher.WithCoordinator(cfg.CoordinatorHTTPURL, redisStore.TLS(), redisStore.ArtifactAuth())
	artifactCache, err := artifact.NewCache(filepath.Join(cfg.StateDir, "artifact-cache"), cfg.ArtifactCacheMB<<20)
	if err != nil {
		logger.Error("cannot open the artifact cache", "error", err)
		os.Exit(2)
	}
	artifactFetcher.WithCache(artifactCache)
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

		// With no S3 bucket, job outputs are stored on the coordinator.
		CoordinatorURL:    cfg.CoordinatorHTTPURL,
		CoordinatorClient: redisStore.HTTPClient(cfg.ArtifactDownloadTimeout),
		CoordinatorAuth:   redisStore.ArtifactAuth(),
	})
	if err != nil {
		logger.Error("invalid output uploader configuration", "error", err)
		os.Exit(2)
	}
	pol, err := policy.Open(filepath.Join(cfg.StateDir, "policy.json"))
	if err != nil {
		logger.Error("cannot load the contributor policy", "error", err)
		os.Exit(2)
	}
	hist := history.Open(filepath.Join(cfg.StateDir, "history.jsonl"), 500)

	sb := executor.DefaultSandbox()
	sb.User = cfg.JobUser
	sb.OutputMaxBytes = max(pol.Get().MaxJobDiskMB, 1) << 20
	sb.Images = executor.ImagePolicy{Allow: pol.Get().ImageAllow, Deny: pol.Get().ImageDeny}
	hasRunsc, rtErr := executor.DetectRuntime(ctx, cfg.DockerBinary)
	switch {
	case cfg.Isolation == "gvisor" && !hasRunsc:
		logger.Error("isolation=gvisor requires the runsc runtime, which this Docker daemon does not have; see docs/GVISOR.md", "detect_error", fmt.Sprint(rtErr))
		os.Exit(2)
	case cfg.Isolation != "runc" && hasRunsc:
		sb.Runtime = "runsc"
	}
	dockerExecutor := executor.NewDockerExecutor(cfg.DockerBinary, cfg.WorkerID, cfg.AllowGPUJobs, cfg.OutputCollectionDir, executor.Limits{
		DefaultMemoryMB: cfg.DefaultJobMemoryMB, DefaultCPUs: cfg.DefaultJobCPUs, PidsLimit: cfg.PidsLimit, StopGrace: cfg.StopGrace,
	}, sb)
	selfBinary, _ := os.Executable()
	netProvider := netproxy.NewProvider(cfg.DockerBinary, sb.DockerConfigDir, cfg.WorkerID, selfBinary, pol.Get, logger)
	if n := netProvider.SweepNetworks(ctx); n > 0 {
		logger.Warn("removed job networks left by a previous run", "count", n)
	}
	dockerExecutor.WithNetwork(netProvider)
	logger.Info("sandbox ready", "isolation", dockerExecutor.IsolationLevel(), "state_dir", cfg.StateDir)
	if n, err := dockerExecutor.CleanupOrphans(ctx); err != nil {
		logger.Warn("could not sweep orphaned job containers", "error", err)
	} else if n > 0 {
		logger.Warn("removed job containers left by a previous run", "count", n)
	}
	resourceMonitor := monitor.New(monitor.NewSystemSampler(os.TempDir(), cfg.DockerBinary, cfg.WorkerID), cfg.MonitorConfig(), nil, logger)
	go resourceMonitor.Run(ctx, cfg.SampleInterval)
	agent := workerapp.NewAgent(cfg, redisStore, artifactFetcher, resourceReporter, dockerExecutor, outputManager, resourceMonitor, logger).
		WithPolicy(pol).WithHistory(hist).WithSandbox(dockerExecutor).WithIsolation(dockerExecutor.IsolationLevel()).WithStop(stop)
	go func() {
		if err := control.Serve(ctx, cfg.ControlSocket, agent, logger); err != nil {
			logger.Error("control socket stopped", "error", err)
		}
	}()

	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped unexpectedly", "error", err)
		os.Exit(1)
	}

	logger.Info("worker stopped")
}
