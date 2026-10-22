package grpcserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
	"time"

	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// requestIDPattern limits what a client may supply as a correlation id: it ends up in every log line, so it
// must not be able to inject newlines, control characters or megabytes.
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// observe is the outermost interceptor. For every call it:
//   - assigns a correlation id (the caller's x-request-id if it is well-formed, else a fresh one) and
//     returns it in the x-request-id response header, so one id ties a client's error to a coordinator log line;
//   - records the call in the RPC metrics;
//   - writes one structured log line with the method, status code, duration and the job / worker involved.
//
// Routine polling (Heartbeat, and RequestJob that found nothing) is logged at debug level, otherwise an idle
// cluster would fill the log with one line per worker per two seconds.
func (s *Server) observe() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("x-request-id"); len(v) > 0 && requestIDPattern.MatchString(v[0]) {
				id = v[0]
			}
		}
		if id == "" {
			id = newRequestID()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", id))

		start := time.Now()
		resp, err := handler(ctx, req)
		d := time.Since(start)

		method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		code := status.Code(err)
		s.metrics.RPC(method, code.String(), d)

		attrs := []any{"request_id", id, "method", method, "code", code.String(), "duration_ms", float64(d.Microseconds()) / 1000}
		job, worker := correlate(req, resp)
		if job != "" {
			attrs = append(attrs, "job_id", job)
		}
		if worker != "" {
			attrs = append(attrs, "worker_id", worker)
		}
		switch {
		case err != nil && code != codes.NotFound:
			s.log.Warn("rpc failed", append(attrs, "error", status.Convert(err).Message())...)
		case routine(method, resp):
			s.log.Debug("rpc", attrs...)
		default:
			s.log.Info("rpc", attrs...)
		}
		return resp, err
	}
}

// routine reports whether the call is background polling that should not be logged at info level.
func routine(method string, resp any) bool {
	switch method {
	case "Heartbeat", "ListWorkers":
		return true
	case "RequestJob":
		r, ok := resp.(*pb.RequestJobResponse)
		return ok && !r.GetHasJob()
	}
	return false
}

// correlate extracts the job and worker ids a request (or its response) concerns, for the log line.
func correlate(req, resp any) (job, worker string) {
	switch r := req.(type) {
	case *pb.RequestJobRequest:
		worker = r.GetWorkerId().GetValue()
		if rr, ok := resp.(*pb.RequestJobResponse); ok {
			job = rr.GetJob().GetJobId().GetValue()
		}
	case *pb.SubmitResultRequest:
		worker, job = r.GetWorkerId().GetValue(), r.GetResult().GetJobId().GetValue()
	case *pb.HeartbeatRequest:
		worker = r.GetWorkerId().GetValue()
	case *pb.RegisterWorkerRequest:
		worker = r.GetWorkerId()
	case *pb.SubmitJobRequest:
		if rr, ok := resp.(*pb.SubmitJobResponse); ok {
			job = rr.GetJobId().GetValue()
		}
	case *pb.GetJobStatusRequest:
		job = r.GetJobId().GetValue()
	case *pb.GetJobResultRequest:
		job = r.GetJobId().GetValue()
	case *pb.CancelJobRequest:
		job = r.GetJobId().GetValue()
	case *pb.ExplainJobRequest:
		job = r.GetJobId().GetValue()
	}
	return job, worker
}
