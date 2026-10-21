package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"coordinator/internal/config"
	grpcserver "coordinator/internal/grpc"
	"coordinator/internal/httpapi"
	"coordinator/internal/store"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatalf("invalid config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient, err := store.NewRedisClient(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTLS)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			log.Printf("redis close error: %v", closeErr)
		}
	}()

	svc := grpcserver.NewServer(redisClient)

	handler, err := httpapi.New(cfg.ArtifactDir, cfg.ArtifactMaxBytes, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
	})
	if err != nil {
		log.Fatalf("http init failed: %v", err)
	}
	httpServer := &http.Server{Addr: cfg.HTTPListenAddr(), Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("coordinator HTTP server listening on %s (artifacts in %s)", cfg.HTTPListenAddr(), cfg.ArtifactDir)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server stopped with error: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := grpcserver.Start(ctx, cfg, svc); err != nil {
		log.Fatalf("gRPC server stopped with error: %v", err)
	}
}
