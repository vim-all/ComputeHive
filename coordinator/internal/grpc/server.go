package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"coordinator/internal/config"
	"coordinator/internal/queue"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
)

// Server implements both worker-facing and client-facing gRPC services.
// It holds no state of its own: everything lives in the queue (Redis), so any
// number of coordinators can serve the same cluster and a crash loses nothing.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	pb.UnimplementedClientServiceServer
	queue *queue.Queue
	cfg   config.Config
	log   *slog.Logger
}

// NewServer builds the gRPC service over q.
func NewServer(q *queue.Queue, cfg config.Config, log *slog.Logger) *Server {
	return &Server{queue: q, cfg: cfg, log: log}
}

// Queue exposes the queue (readiness probes, metrics).
func (s *Server) Queue() *queue.Queue { return s.queue }

// Start serves gRPC until ctx is cancelled, then stops gracefully.
func Start(ctx context.Context, cfg config.Config, svc *Server) error {
	listener, err := net.Listen("tcp", cfg.GRPCListenAddr())
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCListenAddr(), err)
	}
	return Serve(ctx, listener, svc)
}

// Serve serves gRPC on an existing listener (tests use ephemeral ports).
func Serve(ctx context.Context, listener net.Listener, svc *Server) error {
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, svc)
	pb.RegisterClientServiceServer(grpcServer, svc)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	svc.log.Info("coordinator gRPC server listening", "addr", listener.Addr().String())
	return grpcServer.Serve(listener)
}
