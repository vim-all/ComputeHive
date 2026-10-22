package grpcserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"

	"coordinator/internal/auth"
	"coordinator/internal/config"
	"coordinator/internal/queue"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Server implements both worker-facing and client-facing gRPC services.
// It holds no state of its own: everything lives in the queue (Redis), so any
// number of coordinators can serve the same cluster and a crash loses nothing.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	pb.UnimplementedClientServiceServer
	pb.UnimplementedAdminServiceServer
	queue *queue.Queue
	cfg   config.Config
	log   *slog.Logger
	auth  *auth.Service // nil: no authentication (unit tests); production always sets it

	placer *placer // best-fit scheduling on top of the atomic claim
}

// NewServer builds the gRPC service over q.
func NewServer(q *queue.Queue, cfg config.Config, log *slog.Logger) *Server {
	return &Server{queue: q, cfg: cfg, log: log, placer: newPlacer(q, cfg)}
}

// WithAuth turns on authentication, rate limiting and the admin API.
func (s *Server) WithAuth(a *auth.Service) *Server { s.auth = a; return s }

func (s *Server) authEnforced() bool { return s.auth != nil && !s.auth.Disabled() }

// Queue exposes the queue (readiness probes, metrics).
func (s *Server) Queue() *queue.Queue { return s.queue }

// Start serves gRPC until ctx is cancelled, then stops gracefully.
func Start(ctx context.Context, cfg config.Config, svc *Server) error {
	listener, err := net.Listen("tcp", cfg.GRPCListenAddr())
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCListenAddr(), err)
	}
	var tlsCfg *tls.Config
	if cfg.TLSCertFile != "" {
		var err error
		if tlsCfg, err = auth.ServerTLS(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile); err != nil {
			return err
		}
	}
	return Serve(ctx, listener, svc, tlsCfg)
}

// Serve serves gRPC on an existing listener (tests use ephemeral ports).
func Serve(ctx context.Context, listener net.Listener, svc *Server, tlsCfg *tls.Config) error {
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(4 << 20)} // bound request size: an unauthenticated caller cannot make us buffer more
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	if svc.auth != nil {
		opts = append(opts, grpc.ChainUnaryInterceptor(svc.auth.Unary()))
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterWorkerServiceServer(grpcServer, svc)
	pb.RegisterClientServiceServer(grpcServer, svc)
	pb.RegisterAdminServiceServer(grpcServer, svc)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	svc.log.Info("coordinator gRPC server listening", "addr", listener.Addr().String(), "tls", tlsCfg != nil, "mtls", tlsCfg != nil && tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert, "auth", svc.authEnforced())
	return grpcServer.Serve(listener)
}
