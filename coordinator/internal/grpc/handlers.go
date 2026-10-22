package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"coordinator/internal/auth"
	"coordinator/internal/queue"
	pb "coordinator/pkg/pb"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var workerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func internal(op string, err error) error {
	return status.Errorf(codes.Internal, "%s: %v", op, err)
}

func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		workerID = uuid.NewString()
	} else if !workerIDPattern.MatchString(workerID) {
		return nil, status.Error(codes.InvalidArgument, "worker_id must be 1-64 chars of [A-Za-z0-9._-], starting alphanumeric")
	}
	if len(req.GetWorkerVersion()) > 64 {
		return nil, status.Error(codes.InvalidArgument, "worker_version too long")
	}
	credential := ""
	if s.authEnforced() {
		if workerID == "" || req.GetWorkerId() == "" {
			return nil, status.Error(codes.InvalidArgument, "worker_id is required")
		}
		if id := auth.IdentityFrom(ctx); id.Kind == "enrolling" {
			cred, err := s.auth.Enroll(ctx, workerID, req.GetEnrollmentToken())
			switch {
			case errors.Is(err, auth.ErrBadToken):
				return nil, status.Error(codes.Unauthenticated, err.Error())
			case errors.Is(err, auth.ErrAlreadyEnrolled):
				return nil, status.Error(codes.AlreadyExists, err.Error())
			case errors.Is(err, auth.ErrRevoked):
				return nil, status.Error(codes.PermissionDenied, err.Error())
			case err != nil:
				return nil, internal("enroll worker", err)
			}
			credential = cred
		}
	}
	res := req.GetAvailableResources()
	// Registration is a heartbeat that also (re)announces capacity; re-registering an
	// existing ID keeps the worker's identity and any jobs it still owns.
	if _, err := s.queue.Heartbeat(ctx, queue.WorkerReport{
		ID: workerID, CPU: int(res.GetCpuCores()), MemMB: int(res.GetMemoryMb()), GPU: int(res.GetGpuCount()),
		MaxJobs: int(req.GetMaxJobs()), Version: req.GetWorkerVersion(), Isolation: req.GetIsolation(), AllowsNet: req.GetAllowsNetwork(),
		OS: req.GetOs(), Arch: req.GetArch(), Labels: cleanLabels(req.GetLabels()), Preference: clampPref(req.GetPreference()),
	}); err != nil {
		return nil, internal("register worker", err)
	}
	s.log.Info("worker registered", "worker_id", workerID, "version", req.GetWorkerVersion())
	return &pb.RegisterWorkerResponse{
		WorkerId:                 &pb.WorkerID{Value: workerID},
		HeartbeatIntervalSeconds: int32(s.cfg.HeartbeatInterval.Seconds() + 0.5),
		Credential:               credential,
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	known, err := s.queue.WorkerKnown(ctx, workerID)
	if err != nil {
		return nil, internal("check worker", err)
	}
	if !known {
		return nil, status.Error(codes.NotFound, "worker not registered")
	}
	running := make([]string, 0, len(req.GetRunningJobs()))
	for _, j := range req.GetRunningJobs() {
		running = append(running, j.GetValue())
	}
	res := req.GetAvailableResources()
	s.metrics.WorkerOverloaded(workerID, req.GetOverloaded())
	cancel, err := s.queue.Heartbeat(ctx, queue.WorkerReport{
		ID: workerID, CPU: int(res.GetCpuCores()), MemMB: int(res.GetMemoryMb()), GPU: int(res.GetGpuCount()),
		Overloaded: req.GetOverloaded(), Draining: req.GetDraining(), MaxJobs: int(req.GetMaxJobs()), Running: running,
		Usage: usageFromPB(req.GetUsage()), Isolation: req.GetIsolation(), AllowsNet: req.GetAllowsNetwork(),
		OS: req.GetOs(), Arch: req.GetArch(), Labels: cleanLabels(req.GetLabels()), Preference: clampPref(req.GetPreference()),
		DiskFreeMB: req.GetUsage().GetWorkDiskFreeMb(), Load1: req.GetUsage().GetLoad1(), CachedArtifacts: cleanHashes(req.GetCachedArtifacts()),
	})
	if err != nil {
		return nil, internal("heartbeat", err)
	}
	stored := 0
	for jobID, tail := range req.GetLogTails() {
		if stored++; stored > maxLogTailsPerHeartbeat {
			break
		}
		// Accepted only for a job this worker holds (checked atomically in Redis); a failure is not worth failing the heartbeat.
		if _, err := s.queue.SetLogTail(ctx, workerID, jobID, tail); err != nil {
			s.log.Debug("could not store log tail", "worker_id", workerID, "job_id", jobID, "error", err)
		}
	}
	for _, id := range cancel {
		s.log.Warn("worker told to abort job it no longer owns", "worker_id", workerID, "job_id", id)
	}
	return &pb.HeartbeatResponse{Accepted: true, ServerTimeUnix: time.Now().Unix(), CancelJobIds: cancel}, nil
}

func (s *Server) RequestJob(ctx context.Context, req *pb.RequestJobRequest) (*pb.RequestJobResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	claimed, reason, err := s.placer.claimFor(ctx, workerID)
	if err != nil {
		return nil, internal("claim job", err)
	}
	if claimed == nil {
		if reason == "unknown_worker" {
			return nil, status.Error(codes.NotFound, "worker not registered")
		}
		return &pb.RequestJobResponse{HasJob: false}, nil
	}
	sp := claimed.Spec
	s.log.Info("job assigned", "job_id", claimed.JobID, "worker_id", workerID, "attempt", claimed.Attempt)
	return &pb.RequestJobResponse{
		HasJob: true,
		Job: &pb.Job{
			JobId:             &pb.JobID{Value: claimed.JobID},
			ContainerImage:    sp.ContainerImage,
			Command:           sp.Command,
			RequiredResources: &pb.ResourceSpec{CpuCores: sp.CPUCores, MemoryMb: sp.MemoryMB, GpuCount: sp.GPUCount},
			Status:            pb.Status_STATUS_ASSIGNED,
			CreatedAtUnix:     sp.CreatedAtUnix,
			Environment:       sp.Environment,
			MaxRuntimeSeconds: sp.MaxRuntimeSeconds,
			ArtifactUrl:       sp.ArtifactURL,
			ArtifactSha256:    sp.ArtifactSHA256,
			Attempt:           int32(claimed.Attempt),
			LeaseToken:        claimed.Token,
			NetworkHosts:      sp.NetworkHosts,
		},
	}, nil
}

func dispositionResponse(d queue.Disposition) *pb.SubmitResultResponse {
	return &pb.SubmitResultResponse{Accepted: d == queue.DispOK || d == queue.DispDuplicate, Disposition: string(d)}
}

func (s *Server) SubmitResult(ctx context.Context, req *pb.SubmitResultRequest) (*pb.SubmitResultResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	r := req.GetResult()
	jobID := r.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "result.job_id is required")
	}
	token := req.GetLeaseToken()
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_token is required: it proves this worker still owns the job (upgrade the worker)")
	}
	log := s.log.With("job_id", jobID, "worker_id", workerID)

	switch r.GetStatus() {
	case pb.Status_STATUS_RUNNING:
		d, err := s.queue.MarkRunning(ctx, jobID, token)
		if err != nil {
			return nil, internal("mark running", err)
		}
		if d == queue.DispUnknown {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		return dispositionResponse(d), nil

	case pb.Status_STATUS_SUCCEEDED, pb.Status_STATUS_FAILED:
		succeeded := r.GetStatus() == pb.Status_STATUS_SUCCEEDED
		reason := r.GetFailureReason()

		// Worker-side problems and evictions are not the job's fault: retry instead of committing a failure.
		if !succeeded && (reason == queue.ReasonInfra || reason == queue.ReasonEvicted) {
			d, newState, err := s.queue.Fail(ctx, jobID, token, reason, reason == queue.ReasonInfra)
			if err != nil {
				return nil, internal("fail attempt", err)
			}
			if d == queue.DispOK {
				s.placer.released(workerID, jobID)
				s.metrics.JobRequeued(reason, 1)
			}
			log.Warn("attempt failed, job retried", "reason", reason, "disposition", d, "new_state", newState)
			return dispositionResponse(d), nil
		}

		res := queue.Result{
			JobID: jobID, WorkerID: workerID, Status: r.GetStatus().String(),
			ExitCode: r.GetExitCode(), FailureReason: reason,
			StdoutExcerpt: r.GetStdoutExcerpt(), StderrExcerpt: r.GetStderrExcerpt(),
			OutputURI: r.GetOutputUri(), OutputAPIURL: r.GetOutputApiUrl(), OutputPublicURL: r.GetOutputPublicUrl(),
			OutputSizeBytes: r.GetOutputSizeBytes(), FinishedAtUnix: r.GetFinishedAtUnix(), OutputDigest: r.GetOutputDigest(),
		}
		if res.FinishedAtUnix == 0 {
			res.FinishedAtUnix = time.Now().Unix()
		}
		for _, a := range r.GetOutputArtifacts() {
			res.OutputArtifacts = append(res.OutputArtifacts, queue.OutputArtifact{
				RelativePath: a.GetRelativePath(), ObjectKey: a.GetObjectKey(), URI: a.GetUri(), APIURL: a.GetApiUrl(),
				PublicURL: a.GetPublicUrl(), SHA256: a.GetSha256(), SizeBytes: a.GetSizeBytes(), ContentType: a.GetContentType(),
			})
		}
		var ran time.Duration
		if j, err := s.queue.Get(ctx, jobID); err == nil && j != nil {
			res.Attempt = j.Attempt
			if j.AssignedMs > 0 {
				ran = time.Since(time.UnixMilli(j.AssignedMs))
			}
		}
		d, err := s.queue.Complete(ctx, jobID, token, succeeded, res)
		if err != nil {
			return nil, internal("complete job", err)
		}
		if d == queue.DispUnknown {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		if d == queue.DispStale {
			log.Warn("discarded result from superseded attempt")
		} else if d == queue.DispOK {
			s.placer.released(workerID, jobID)
			s.metrics.JobCompleted(r.GetStatus().String(), reason, r.GetExitCode(), ran)
			log.Info("result committed", "status", r.GetStatus().String(), "exit_code", r.GetExitCode())
		}
		return dispositionResponse(d), nil

	default:
		return nil, status.Error(codes.InvalidArgument, "result.status must be RUNNING, SUCCEEDED, or FAILED")
	}
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if err := validateSubmit(req); err != nil {
		return nil, err
	}
	res := req.GetRequiredResources()
	jobID := uuid.NewString()
	spec := queue.Spec{
		ID: jobID, ContainerImage: req.GetContainerImage(), Command: req.GetCommand(),
		CPUCores: res.GetCpuCores(), MemoryMB: res.GetMemoryMb(), GPUCount: res.GetGpuCount(),
		Environment: req.GetEnvironment(), MaxRuntimeSeconds: req.GetMaxRuntimeSeconds(),
		ArtifactURL: req.GetArtifactUrl(), ArtifactSHA256: req.GetArtifactSha256(),
		User: req.GetUser(), Verify: req.GetVerify(), CreatedAtUnix: time.Now().Unix(),
		RequiredIsolation: req.GetRequiredIsolation(), NetworkHosts: req.GetNetworkHosts(),
		MinDiskMB: req.GetMinDiskMb(), OS: req.GetOs(), Arch: req.GetArch(), Labels: req.GetLabels(),
	}
	enqueue := s.queue.Enqueue
	if req.GetVerify() {
		enqueue = s.queue.EnqueuePair // run on two different workers and compare their outputs
	}
	seq, _, err := enqueue(ctx, spec, int(req.GetMaxAttempts()))
	if err != nil {
		return nil, internal("enqueue job", err)
	}
	s.metrics.JobSubmitted()
	s.log.Info("job submitted", "job_id", jobID, "image", spec.ContainerImage, "queue_seq", seq)
	return &pb.SubmitJobResponse{JobId: &pb.JobID{Value: jobID}, Status: pb.Status_STATUS_QUEUED}, nil
}

func (s *Server) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
	jobID := req.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	j, err := s.queue.Get(ctx, jobID)
	if err != nil {
		return nil, internal("get job", err)
	}
	if j == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	tail, err := s.queue.LogTail(ctx, jobID)
	if err != nil {
		return nil, internal("read log tail", err)
	}
	return &pb.GetJobStatusResponse{
		Status: stateToPB(j.State), AssignedWorkerId: firstNonEmpty(j.Worker, j.LastWorker), UpdatedAtUnix: time.Now().Unix(),
		Attempt: int32(j.Attempt), MaxAttempts: int32(j.MaxAttempts), LastFailure: j.LastFailure, QueueSeq: j.Seq,
		Verification: j.Verification, LogTail: tail,
	}, nil
}

// maxLogTailsPerHeartbeat bounds how much a single heartbeat can make the coordinator write.
const maxLogTailsPerHeartbeat = 8

// pbToState is the inverse of stateToPB, for filters ("" means no filter).
func pbToState(s pb.Status) string {
	for _, st := range queue.AllStates {
		if stateToPB(st) == s && s != pb.Status_STATUS_UNSPECIFIED {
			return st
		}
	}
	return ""
}

func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	if req.GetLimit() < 0 || req.GetLimit() > 200 {
		return nil, status.Error(codes.InvalidArgument, "limit must be 0-200 (0 = default 50)")
	}
	state := pbToState(req.GetStatus())
	if req.GetStatus() != pb.Status_STATUS_UNSPECIFIED && state == "" {
		return nil, status.Error(codes.InvalidArgument, "unknown status filter")
	}
	jobs, err := s.queue.ListJobs(ctx, int(req.GetLimit()), state)
	if err != nil {
		return nil, internal("list jobs", err)
	}
	resp := &pb.ListJobsResponse{}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, &pb.JobSummary{
			JobId: &pb.JobID{Value: j.ID}, Status: stateToPB(j.State), AssignedWorkerId: firstNonEmpty(j.Worker, j.LastWorker),
			Attempt: int32(j.Attempt), MaxAttempts: int32(j.MaxAttempts), CreatedAtUnix: j.CreatedMs / 1000,
			ContainerImage: j.Spec.ContainerImage, User: j.Spec.User, QueueSeq: j.Seq, LastFailure: j.LastFailure,
		})
	}
	return resp, nil
}

func (s *Server) GetJobResult(ctx context.Context, req *pb.GetJobResultRequest) (*pb.GetJobResultResponse, error) {
	jobID := req.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	j, err := s.queue.Get(ctx, jobID)
	if err != nil {
		return nil, internal("get job", err)
	}
	if j == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	rec, err := s.queue.GetResult(ctx, jobID)
	if err != nil {
		return nil, internal("get result", err)
	}
	if rec == nil || j.State == queue.StateVerifying {
		// a verified job's result is withheld until both replicas agree
		return &pb.GetJobResultResponse{Status: stateToPB(j.State), ResultAvailable: false}, nil
	}
	resultStatus := stateToPB(j.State)
	if v, ok := pb.Status_value[rec.Status]; ok {
		resultStatus = pb.Status(v)
	}
	return &pb.GetJobResultResponse{
		Status: stateToPB(j.State), ResultAvailable: true,
		Result: &pb.Result{
			JobId: &pb.JobID{Value: rec.JobID}, Status: resultStatus, ExitCode: rec.ExitCode,
			StdoutExcerpt: rec.StdoutExcerpt, StderrExcerpt: rec.StderrExcerpt, OutputUri: rec.OutputURI,
			OutputApiUrl: rec.OutputAPIURL, OutputPublicUrl: rec.OutputPublicURL, OutputSizeBytes: rec.OutputSizeBytes,
			OutputArtifacts: toPBOutputArtifacts(rec.OutputArtifacts), FinishedAtUnix: rec.FinishedAtUnix,
			FailureReason: rec.FailureReason,
		},
	}, nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	jobID := req.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	d, err := s.queue.Cancel(ctx, jobID)
	if err != nil {
		return nil, internal("cancel job", err)
	}
	if d == queue.DispUnknown {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	s.log.Info("job cancel requested", slog.String("job_id", jobID), slog.String("disposition", string(d)))
	return &pb.CancelJobResponse{Disposition: string(d)}, nil
}

func (s *Server) ListWorkers(ctx context.Context, _ *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	workers, err := s.queue.ListWorkers(ctx)
	if err != nil {
		return nil, internal("list workers", err)
	}
	resp := &pb.ListWorkersResponse{}
	// Scheduling facts (platform, labels, reliability) come from the scheduler's own view of the cluster.
	// If that read fails the list is still useful without them.
	facts := map[string]queue.WorkerState{}
	if states, err := s.queue.WorkerStates(ctx); err == nil {
		for _, st := range states {
			facts[st.ID] = st
		}
	}
	for _, w := range workers {
		st := facts[w.ID]
		var usage *pb.ResourceUsage
		if w.UsageJSON != "" {
			usage = &pb.ResourceUsage{}
			if err := protojson.Unmarshal([]byte(w.UsageJSON), usage); err != nil {
				usage = nil
			}
		}
		info := &pb.WorkerInfo{
			Overloaded:        w.Overloaded,
			Isolation:         w.Isolation,
			AllowsNetwork:     w.AllowsNetwork,
			Draining:          w.Draining,
			Usage:             usage,
			WorkerId:          &pb.WorkerID{Value: w.ID},
			Resources:         &pb.ResourceSpec{CpuCores: int32(w.CPU), MemoryMb: int32(w.MemMB), GpuCount: int32(w.GPU)},
			Version:           w.Version,
			Alive:             w.Alive,
			RegisteredAtUnix:  w.RegisteredMs / 1000,
			LastHeartbeatUnix: w.LastHeartbeatMs / 1000,
			Os:                st.OS, Arch: st.Arch, Labels: st.Labels, Reliability: st.Reliability, Preference: int32(st.Preference),
		}
		for _, id := range w.Running {
			info.RunningJobs = append(info.RunningJobs, &pb.JobID{Value: id})
		}
		resp.Workers = append(resp.Workers, info)
	}
	return resp, nil
}

// usageFromPB converts a worker's sample into the part the scheduler acts on, rejecting nonsense.
func usageFromPB(u *pb.ResourceUsage) *queue.Usage {
	if u == nil {
		return nil
	}
	clamp := func(v float64) float64 {
		switch {
		case v != v || v < 0: // NaN or negative
			return 0
		case v > 100:
			return 100
		}
		return v
	}
	raw, _ := protojson.Marshal(u)
	return &queue.Usage{
		MemTotalMB: max(u.GetMemTotalMb(), 0), MemUsedMB: max(u.GetMemUsedMb(), 0), JobsMemMB: max(u.GetJobsMemUsedMb(), 0),
		MemPct: clamp(u.GetMemUsedPercent()), CPUPct: clamp(u.GetCpuPercent()),
		DiskPct: max(clamp(u.GetWorkDiskUsedPercent()), clamp(u.GetDockerDiskUsedPercent())),
		JSON:    string(raw),
	}
}

func toPBOutputArtifacts(records []queue.OutputArtifact) []*pb.OutputArtifact {
	if len(records) == 0 {
		return nil
	}
	out := make([]*pb.OutputArtifact, 0, len(records))
	for _, r := range records {
		out = append(out, &pb.OutputArtifact{
			RelativePath: r.RelativePath, ObjectKey: r.ObjectKey, Uri: r.URI, ApiUrl: r.APIURL,
			PublicUrl: r.PublicURL, Sha256: r.SHA256, SizeBytes: r.SizeBytes, ContentType: r.ContentType,
		})
	}
	return out
}

func stateToPB(state string) pb.Status {
	switch state {
	case queue.StatePending:
		return pb.Status_STATUS_QUEUED
	case queue.StateAssigned:
		return pb.Status_STATUS_ASSIGNED
	case queue.StateRunning:
		return pb.Status_STATUS_RUNNING
	case queue.StateSucceeded:
		return pb.Status_STATUS_SUCCEEDED
	case queue.StateFailed:
		return pb.Status_STATUS_FAILED
	case queue.StateRequeued:
		return pb.Status_STATUS_REQUEUED
	case queue.StateDead:
		return pb.Status_STATUS_DEAD
	case queue.StateCanceled:
		return pb.Status_STATUS_CANCELED
	case queue.StateVerifying:
		return pb.Status_STATUS_VERIFYING
	default:
		return pb.Status_STATUS_UNSPECIFIED
	}
}

// firstNonEmpty returns the first non-empty string: a finished job has no current worker, but the one that
// last ran it is still worth showing.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
