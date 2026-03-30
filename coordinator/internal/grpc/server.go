package grpcserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"coordinator/internal/config"
	"coordinator/internal/scheduler"
	pb "coordinator/pkg/pb"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// Server implements both worker-facing and client-facing gRPC services.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	pb.UnimplementedClientServiceServer
	redis     *redis.Client
	scheduler *scheduler.Scheduler
}

func NewServer(redisClient *redis.Client) *Server {
	return &Server{
		redis:     redisClient,
		scheduler: scheduler.New(redisClient),
	}
}

func Start(ctx context.Context, cfg config.Config, svc *Server) error {
	listener, err := net.Listen("tcp", cfg.GRPCListenAddr())
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCListenAddr(), err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, svc)
	pb.RegisterClientServiceServer(grpcServer, svc)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	go svc.scheduler.StartWorkerMonitor(ctx, 3*time.Second)

	log.Printf("coordinator gRPC server listening on %s", cfg.GRPCListenAddr())
	return grpcServer.Serve(listener)
}
