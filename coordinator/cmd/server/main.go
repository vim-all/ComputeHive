package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"coordinator/internal/config"
	grpcserver "coordinator/internal/grpc"
	"coordinator/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
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
	if err := grpcserver.Start(ctx, cfg, svc); err != nil {
		log.Fatalf("gRPC server stopped with error: %v", err)
	}
}
