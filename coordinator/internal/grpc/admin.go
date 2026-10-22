package grpcserver

import (
	"context"
	"time"

	"coordinator/internal/auth"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) requireAuth() error {
	if s.auth == nil || s.auth.Disabled() {
		return status.Error(codes.FailedPrecondition, "authentication is disabled on this coordinator, so enrollment tokens and revocation do not apply")
	}
	return nil
}

// CreateEnrollmentToken mints an expiring, limited-use token. The secret is returned once; only its hash is stored.
func (s *Server) CreateEnrollmentToken(ctx context.Context, req *pb.CreateEnrollmentTokenRequest) (*pb.CreateEnrollmentTokenResponse, error) {
	if err := s.requireAuth(); err != nil {
		return nil, err
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	uses := int(req.GetMaxUses())
	if req.GetTtlSeconds() < 1 || ttl > 7*24*time.Hour {
		return nil, invalid("ttl_seconds must be between 1 and %d", 7*24*3600)
	}
	if uses < 1 || uses > 1000 {
		return nil, invalid("max_uses must be between 1 and 1000")
	}
	if len(req.GetNote()) > 200 {
		return nil, invalid("note longer than 200 bytes")
	}
	token, expires, err := s.auth.CreateEnrollmentToken(ctx, ttl, uses, req.GetNote())
	if err != nil {
		return nil, internal("create enrollment token", err)
	}
	s.log.Info("enrollment token created", "event", "enrollment_token_created", "expires", expires.UTC().Format(time.RFC3339), "max_uses", uses, "note", req.GetNote())
	return &pb.CreateEnrollmentTokenResponse{Token: token, ExpiresAtUnix: expires.Unix(), MaxUses: int32(uses)}, nil
}

// RevokeWorker destroys a worker's credential and immediately takes back the jobs it holds.
func (s *Server) RevokeWorker(ctx context.Context, req *pb.RevokeWorkerRequest) (*pb.RevokeWorkerResponse, error) {
	if err := s.requireAuth(); err != nil {
		return nil, err
	}
	id := req.GetWorkerId().GetValue()
	if !auth.WorkerIDPattern.MatchString(id) {
		return nil, invalid("worker_id must be 1-64 chars of [A-Za-z0-9._-], starting alphanumeric")
	}
	if err := s.auth.Revoke(ctx, id); err != nil {
		return nil, internal("revoke worker", err)
	}
	n, err := s.queue.ReleaseWorker(ctx, id)
	if err != nil {
		return nil, internal("requeue the revoked worker's jobs", err)
	}
	return &pb.RevokeWorkerResponse{RequeuedJobs: int32(n)}, nil
}
