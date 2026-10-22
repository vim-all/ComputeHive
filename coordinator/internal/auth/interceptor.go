package auth

import (
	"context"
	"net"
	"strings"
	"time"

	pb "coordinator/pkg/pb"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type limiterEntry struct {
	lim  *rate.Limiter
	seen time.Time
}

// allow applies a token bucket to key. Idle buckets are discarded so memory stays bounded.
func (s *Service) allow(key string, rps, burst float64) bool {
	if rps <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if len(s.limiters) > 4096 { // opportunistic cleanup instead of a janitor goroutine
		for k, e := range s.limiters {
			if now.Sub(e.seen) > 10*time.Minute {
				delete(s.limiters, k)
			}
		}
	}
	e, ok := s.limiters[key]
	if !ok {
		e = &limiterEntry{lim: rate.NewLimiter(rate.Limit(rps), max(int(burst), 1))}
		s.limiters[key] = e
	}
	e.seen = now
	return e.lim.AllowN(now, 1)
}

type identityKey struct{}

// Identity is who the interceptor decided the caller is.
type Identity struct {
	Kind     string // "worker", "enrolling", "client", "dev"
	WorkerID string
}

// IdentityFrom returns the caller identity the interceptor attached to ctx.
func IdentityFrom(ctx context.Context) Identity {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id
}

func peerHost(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

func bearer(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, v := range md.Get("authorization") {
		if tok, found := strings.CutPrefix(v, "Bearer "); found {
			return strings.TrimSpace(tok)
		}
	}
	return ""
}

var (
	errRate     = status.Error(codes.ResourceExhausted, "rate limit exceeded: slow down")
	errNoCreds  = status.Error(codes.Unauthenticated, "missing or invalid credentials")
	errRevoked  = status.Error(codes.PermissionDenied, "this worker has been revoked")
	errInternal = status.Error(codes.Unavailable, "authentication backend unavailable")
)

// Unary returns the interceptor that rate-limits, authenticates and tags every call.
//
//	WorkerService   a worker credential (RegisterWorker alone may instead carry an enrollment token)
//	ClientService   the API token
//	AdminService    the API token
//
// Anything else is refused. Source-address limiting runs first, before any secret is examined, so
// guessing tokens is throttled.
func (s *Service) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !s.allow("ip:"+peerHost(ctx), s.cfg.AnonRPS, s.cfg.AnonBurst) {
			return nil, errRate
		}
		if s.cfg.Disabled {
			return handler(context.WithValue(ctx, identityKey{}, Identity{Kind: "dev"}), req)
		}
		cred := bearer(ctx)
		switch {
		case strings.HasPrefix(info.FullMethod, "/compute.v1.WorkerService/"):
			return s.workerCall(ctx, req, handler, cred)
		case strings.HasPrefix(info.FullMethod, "/compute.v1.ClientService/"), strings.HasPrefix(info.FullMethod, "/compute.v1.AdminService/"):
			if !s.VerifyAPIToken(cred) {
				return nil, errNoCreds
			}
			if !s.allow("api:"+hashHex(cred)[:12], s.cfg.ClientRPS, s.cfg.ClientBurst) {
				return nil, errRate
			}
			return handler(context.WithValue(ctx, identityKey{}, Identity{Kind: "client"}), req)
		default:
			return nil, status.Error(codes.PermissionDenied, "unknown service")
		}
	}
}

func (s *Service) workerCall(ctx context.Context, req any, handler grpc.UnaryHandler, cred string) (any, error) {
	var id string
	register := false
	switch r := req.(type) {
	case *pb.RegisterWorkerRequest:
		id, register = r.GetWorkerId(), true
	case interface{ GetWorkerId() *pb.WorkerID }:
		id = r.GetWorkerId().GetValue()
	}

	if register {
		// A returning worker proves itself with its credential; a new one presents an enrollment token.
		if id != "" && cred != "" {
			st, err := s.VerifyWorker(ctx, id, cred)
			if err != nil {
				return nil, errInternal
			}
			switch st {
			case WorkerOK:
				return s.finishWorker(ctx, req, handler, id)
			case WorkerRevoked:
				return nil, errRevoked
			}
		}
		if r, _ := req.(*pb.RegisterWorkerRequest); r != nil && r.GetEnrollmentToken() != "" {
			return handler(context.WithValue(ctx, identityKey{}, Identity{Kind: "enrolling", WorkerID: id}), req)
		}
		return nil, status.Error(codes.Unauthenticated, "an enrollment token or this worker's credential is required to register")
	}

	if id == "" {
		return nil, errNoCreds
	}
	st, err := s.VerifyWorker(ctx, id, cred)
	if err != nil {
		return nil, errInternal
	}
	switch st {
	case WorkerRevoked:
		return nil, errRevoked
	case WorkerBadCredential:
		return nil, errNoCreds
	}
	return s.finishWorker(ctx, req, handler, id)
}

func (s *Service) finishWorker(ctx context.Context, req any, handler grpc.UnaryHandler, id string) (any, error) {
	if !s.allow("w:"+id, s.cfg.WorkerRPS, s.cfg.WorkerBurst) {
		return nil, errRate
	}
	return handler(context.WithValue(ctx, identityKey{}, Identity{Kind: "worker", WorkerID: id}), req)
}
