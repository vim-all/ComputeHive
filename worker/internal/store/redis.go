// Package store is the worker's client for the coordinator's WorkerService.
// (The file name is historical: the worker no longer talks to Redis directly.)
package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	pb "coordinator/pkg/pb"
	"github.com/vim-all/ComputeHive/worker/internal/config"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Store talks to the coordinator. The worker's identity is its configured WorkerID,
// so it keeps the same identity across restarts and reconnects.
type Store struct {
	cfg config.Config

	mu          sync.Mutex
	conn        *grpc.ClientConn
	client      pb.WorkerServiceClient
	registered  bool
	leases      map[string]string // jobID -> lease token of the attempt this worker holds
	lastRes     *pb.ResourceSpec  // last resources announced, reused when re-registering
	isolation   string            // capabilities announced at (re)registration
	allowsNet   bool
	cred        *credentialStore
	tlsCfg      *tls.Config
	tlsErr      error
	warnedPlain bool
}

func New(cfg config.Config) *Store {
	s := &Store{cfg: cfg, leases: make(map[string]string), cred: loadCredential(filepath.Join(cfg.StateDir, "credential"))}
	s.tlsCfg, s.tlsErr = BuildTLS(cfg)
	return s
}

// TLS returns the TLS configuration in use (nil: plaintext), shared with the artifact fetcher.
func (s *Store) TLS() *tls.Config { return s.tlsCfg }

// Close releases the connection.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *Store) connect() (pb.WorkerServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		// NewClient connects lazily; an unreachable coordinator surfaces as an RPC
		// error (and is retried by the agent loop) instead of blocking here.
		if s.tlsErr != nil {
			return nil, fmt.Errorf("tls configuration: %w", s.tlsErr)
		}
		transport := insecure.NewCredentials()
		if s.tlsCfg != nil {
			transport = credentials.NewTLS(s.tlsCfg)
		} else if !s.warnedPlain {
			s.warnedPlain = true
			slog.Warn("connecting to the coordinator WITHOUT TLS: the worker credential and all job data cross the network in clear text. Set COORDINATOR_TLS_CA.")
		}
		conn, err := grpc.NewClient(s.cfg.CoordinatorAddr, grpc.WithTransportCredentials(transport), grpc.WithPerRPCCredentials(perRPC{s.cred}))
		if err != nil {
			return nil, fmt.Errorf("dial coordinator: %w", err)
		}
		s.conn = conn
		s.client = pb.NewWorkerServiceClient(conn)
	}
	return s.client, nil
}

func (s *Store) workerID() *pb.WorkerID { return &pb.WorkerID{Value: s.cfg.WorkerID} }

// register (re)announces this worker under its stable ID.
func (s *Store) register(ctx context.Context, client pb.WorkerServiceClient, res *pb.ResourceSpec) error {
	s.mu.Lock()
	if res == nil {
		res = s.lastRes
	} else {
		s.lastRes = res
	}
	s.mu.Unlock()
	token := ""
	if s.cred.get() == "" {
		if token = strings.TrimSpace(s.cfg.EnrollmentToken); token == "" {
			return errNoEnrollment
		}
	}
	resp, err := client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		EnrollmentToken:    token,
		AvailableResources: res,
		WorkerVersion:      s.cfg.Version,
		WorkerId:           s.cfg.WorkerID,
		MaxJobs:            1,
		Isolation:          s.isolation,
		AllowsNetwork:      s.allowsNet,
		Os:                 s.cfg.OS,
		Arch:               s.cfg.Arch,
		Labels:             s.cfg.Labels,
		Preference:         int32(s.cfg.Preference),
	})
	if err != nil {
		return fmt.Errorf("register worker %s: %w", s.cfg.WorkerID, err)
	}
	if got := resp.GetWorkerId().GetValue(); got != s.cfg.WorkerID {
		return fmt.Errorf("coordinator assigned id %q, wanted %q", got, s.cfg.WorkerID)
	}
	if issued := resp.GetCredential(); issued != "" {
		if err := s.cred.set(issued); err != nil {
			return fmt.Errorf("enrolled, but could not save the credential (the worker would have to be re-enrolled): %w", err)
		}
		slog.Info("enrolled with the coordinator; credential saved")
	}
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()
	return nil
}

func (s *Store) ensureRegistered(ctx context.Context, client pb.WorkerServiceClient) error {
	s.mu.Lock()
	ok := s.registered
	s.mu.Unlock()
	if ok {
		return nil
	}
	return s.register(ctx, client, nil)
}

func (s *Store) unregistered() {
	s.mu.Lock()
	s.registered = false
	s.mu.Unlock()
}

// PullJob asks the coordinator for the next job. The returned job carries the lease
// token that must accompany every report about it.
func (s *Store) PullJob(ctx context.Context) (*domain.Job, bool, error) {
	client, err := s.connect()
	if err != nil {
		return nil, false, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.PollTimeout+s.cfg.RedisIOTimeout)
	defer cancel()

	if err := s.ensureRegistered(reqCtx, client); err != nil {
		return nil, false, err
	}
	resp, err := client.RequestJob(reqCtx, &pb.RequestJobRequest{WorkerId: s.workerID()})
	if isNotFound(err) { // coordinator forgot us (e.g. we were declared dead): rejoin under the same ID
		s.unregistered()
		if err := s.ensureRegistered(reqCtx, client); err != nil {
			return nil, false, err
		}
		resp, err = client.RequestJob(reqCtx, &pb.RequestJobRequest{WorkerId: s.workerID()})
	}
	if err != nil {
		return nil, false, fmt.Errorf("request job from coordinator: %w", err)
	}
	if !resp.GetHasJob() || resp.GetJob() == nil {
		return nil, false, nil
	}

	m := resp.GetJob()
	res := m.GetRequiredResources()
	job := &domain.Job{
		ID:             strings.TrimSpace(m.GetJobId().GetValue()),
		Status:         domain.JobStatusAssigned,
		ImageRef:       strings.TrimSpace(m.GetContainerImage()),
		Command:        append([]string(nil), m.GetCommand()...),
		Env:            cloneStringMap(m.GetEnvironment()),
		CreatedAtUnix:  m.GetCreatedAtUnix(),
		TimeoutSeconds: int(m.GetMaxRuntimeSeconds()),
		ArtifactURL:    strings.TrimSpace(m.GetArtifactUrl()),
		ArtifactSHA256: strings.TrimSpace(m.GetArtifactSha256()),
		Attempt:        int(m.GetAttempt()),
		LeaseToken:     m.GetLeaseToken(),
		NetworkHosts:   append([]string(nil), m.GetNetworkHosts()...),
	}
	if res != nil {
		job.CPUCores = float64(res.GetCpuCores())
		job.MemoryMB = int64(res.GetMemoryMb())
		job.GPU = res.GetGpuCount() > 0
	}
	s.mu.Lock()
	s.leases[job.ID] = job.LeaseToken
	s.mu.Unlock()
	return job, true, nil
}

// PublishHeartbeat reports liveness, capacity and running jobs. It returns the jobs
// the coordinator says this worker no longer owns, which it must abort.
func (s *Store) PublishHeartbeat(ctx context.Context, hb domain.WorkerHeartbeat) ([]string, error) {
	client, err := s.connect()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.isolation, s.allowsNet = hb.Isolation, hb.AllowsNetwork
	s.mu.Unlock()
	res := &pb.ResourceSpec{
		CpuCores: int32(hb.Resources.CPUCores),
		MemoryMb: int32(hb.Resources.MemoryMB),
		GpuCount: boolToGPUCount(hb.Resources.GPU),
	}
	if err := s.ensureRegistered(ctx, client); err != nil {
		return nil, err
	}
	req := &pb.HeartbeatRequest{
		WorkerId:           s.workerID(),
		AvailableResources: res,
		Overloaded:         hb.Overloaded,
		Draining:           hb.Draining,
		MaxJobs:            int32(max(hb.MaxJobs, 1)),
		Usage:              usageToPB(hb),
		Isolation:          hb.Isolation,
		AllowsNetwork:      hb.AllowsNetwork,
		Os:                 hb.OS,
		Arch:               hb.Arch,
		Labels:             hb.Labels,
		Preference:         int32(hb.Preference),
		CachedArtifacts:    hb.CachedArtifacts,
	}
	for _, id := range hb.RunningJobs {
		req.RunningJobs = append(req.RunningJobs, &pb.JobID{Value: id})
	}
	resp, err := client.Heartbeat(ctx, req)
	if isNotFound(err) {
		if err := s.register(ctx, client, res); err != nil {
			return nil, err
		}
		resp, err = client.Heartbeat(ctx, req)
	}
	if err != nil {
		return nil, fmt.Errorf("publish coordinator heartbeat: %w", err)
	}
	return resp.GetCancelJobIds(), nil
}

func (s *Store) leaseFor(jobID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok, ok := s.leases[jobID]
	if !ok {
		return "", fmt.Errorf("no lease token for job %s", jobID)
	}
	return tok, nil
}

// ReportRunning tells the coordinator the attempt started executing.
func (s *Store) ReportRunning(ctx context.Context, jobID string) error {
	client, err := s.connect()
	if err != nil {
		return err
	}
	token, err := s.leaseFor(jobID)
	if err != nil {
		return err
	}
	_, err = client.SubmitResult(ctx, &pb.SubmitResultRequest{
		WorkerId:   s.workerID(),
		LeaseToken: token,
		Result:     &pb.Result{JobId: &pb.JobID{Value: jobID}, Status: pb.Status_STATUS_RUNNING},
	})
	if err != nil {
		return fmt.Errorf("report running: %w", err)
	}
	return nil
}

// PublishJobResult commits the attempt's outcome. The returned disposition is
// "ok", "duplicate" or "stale"; a stale result was discarded by the coordinator.
func (s *Store) PublishJobResult(ctx context.Context, result domain.JobResult) (string, error) {
	client, err := s.connect()
	if err != nil {
		return "", err
	}
	token, err := s.leaseFor(result.JobID)
	if err != nil {
		return "", err
	}
	resp, err := client.SubmitResult(ctx, &pb.SubmitResultRequest{
		WorkerId:   s.workerID(),
		LeaseToken: token,
		Result: &pb.Result{
			JobId:           &pb.JobID{Value: result.JobID},
			Status:          domainStatusToPB(result.Status),
			ExitCode:        int32(result.ExitCode),
			StdoutExcerpt:   trimTo(result.Stdout, 4096),
			StderrExcerpt:   trimTo(result.Stderr, 4096),
			OutputUri:       result.OutputURI,
			OutputApiUrl:    result.OutputAPIURL,
			OutputPublicUrl: result.OutputPublicURL,
			OutputSizeBytes: result.OutputSizeBytes,
			OutputArtifacts: toPBOutputArtifacts(result.OutputArtifacts),
			FinishedAtUnix:  result.FinishedAt.UTC().Unix(),
			FailureReason:   result.FailureReason,
			OutputDigest:    result.OutputDigest,
		},
	})
	if err != nil {
		return "", fmt.Errorf("submit result to coordinator: %w", err)
	}
	s.Forget(result.JobID)
	return resp.GetDisposition(), nil
}

// Forget drops the lease token for a job this worker no longer holds.
func (s *Store) Forget(jobID string) {
	s.mu.Lock()
	delete(s.leases, jobID)
	s.mu.Unlock()
}

func usageToPB(hb domain.WorkerHeartbeat) *pb.ResourceUsage {
	m := hb.Usage
	if m == nil {
		return nil // no sample yet: the coordinator will refuse this worker work until there is one
	}
	return &pb.ResourceUsage{
		MemTotalMb: m.MemTotalMB, MemUsedMb: m.MemUsedMB, MemUsedPercent: m.MemPercent,
		CpuPercent: m.CPUPercent, Load1: m.Load1, Load5: m.Load5, Load15: m.Load15,
		WorkDiskUsedPercent: m.WorkDiskUsedPercent, DockerDiskUsedPercent: m.DockerDiskUsedPercent,
		SwapUsedPercent: m.SwapPercent, CpuTempC: m.CPUTempC, DockerHealthy: m.DockerHealthy,
		GpuCount: int32(m.GPUCount), GpuUtilPercent: m.GPUUtilPercent, NetEgressBytesPerSec: m.NetEgressBytesPerSec,
		JobsMemUsedMb: m.JobsMemUsedMB, OverloadReasons: hb.OverloadReasons, SampledAtUnix: m.At.Unix(), WorkDiskFreeMb: m.WorkDiskFreeMB,
	}
}

func boolToGPUCount(gpu bool) int32 {
	if gpu {
		return 1
	}
	return 0
}

func domainStatusToPB(status string) pb.Status {
	switch strings.TrimSpace(status) {
	case domain.JobStatusRunning:
		return pb.Status_STATUS_RUNNING
	case domain.JobStatusSucceeded:
		return pb.Status_STATUS_SUCCEEDED
	case domain.JobStatusFailed:
		return pb.Status_STATUS_FAILED
	default:
		return pb.Status_STATUS_UNSPECIFIED
	}
}

func trimTo(value string, maxLen int) string {
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen]
}

func isNotFound(err error) bool {
	return err != nil && status.Code(err) == codes.NotFound
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func toPBOutputArtifacts(artifacts []domain.OutputArtifact) []*pb.OutputArtifact {
	if len(artifacts) == 0 {
		return nil
	}
	items := make([]*pb.OutputArtifact, 0, len(artifacts))
	for _, a := range artifacts {
		items = append(items, &pb.OutputArtifact{
			RelativePath: a.RelativePath, ObjectKey: a.ObjectKey, Uri: a.URI, ApiUrl: a.APIURL,
			PublicUrl: a.PublicURL, Sha256: a.SHA256, SizeBytes: a.SizeBytes, ContentType: a.ContentType,
		})
	}
	return items
}
