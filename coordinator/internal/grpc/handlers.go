package grpcserver

import (
	"context"
	"log"
	"time"

	"coordinator/internal/store"
	pb "coordinator/pkg/pb"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	workerID := uuid.NewString()
	now := time.Now().Unix()

	worker := &store.WorkerRecord{
		ID:                 workerID,
		AvailableResources: req.GetAvailableResources(),
		WorkerVersion:      req.GetWorkerVersion(),
		RegisteredAtUnix:   now,
		LastHeartbeatUnix:  now,
	}

	if err := store.SaveWorker(ctx, s.redis, worker); err != nil {
		return nil, status.Errorf(codes.Internal, "save worker: %v", err)
	}
	if err := store.SetWorkerAlive(ctx, s.redis, workerID, 10*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "set worker heartbeat key: %v", err)
	}

	log.Printf("worker registered: id=%s version=%s", workerID, req.GetWorkerVersion())

	return &pb.RegisterWorkerResponse{
		WorkerId:                 &pb.WorkerID{Value: workerID},
		HeartbeatIntervalSeconds: 10,
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	worker, err := store.GetWorker(ctx, s.redis, workerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get worker: %v", err)
	}
	if worker == nil {
		return nil, status.Error(codes.NotFound, "worker not registered")
	}

	if err := store.SetWorkerAlive(ctx, s.redis, workerID, 10*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "set worker heartbeat key: %v", err)
	}

	worker.AvailableResources = req.GetAvailableResources()
	worker.LastHeartbeatUnix = time.Now().Unix()
	if err := store.SaveWorker(ctx, s.redis, worker); err != nil {
		return nil, status.Errorf(codes.Internal, "update worker heartbeat metadata: %v", err)
	}

	return &pb.HeartbeatResponse{
		Accepted:       true,
		ServerTimeUnix: time.Now().Unix(),
	}, nil
}

func (s *Server) RequestJob(ctx context.Context, req *pb.RequestJobRequest) (*pb.RequestJobResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	worker, err := store.GetWorker(ctx, s.redis, workerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get worker: %v", err)
	}
	if worker == nil {
		return nil, status.Error(codes.NotFound, "worker not registered")
	}

	alive, err := store.IsWorkerAlive(ctx, s.redis, workerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check worker heartbeat: %v", err)
	}
	if !alive {
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	jobID, err := store.ClaimNextQueuedJobForWorker(ctx, s.redis, workerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim job from queue: %v", err)
	}
	if jobID == "" {
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job: %v", err)
	}
	if job == nil {
		_ = store.RequeueJobID(ctx, s.redis, jobID)
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	job.Status = store.JobStatusRunning
	job.AssignedWorkerID = workerID
	if err := store.SaveJob(ctx, s.redis, job); err != nil {
		_ = store.RequeueJobID(ctx, s.redis, jobID)
		return nil, status.Errorf(codes.Internal, "save running job metadata: %v", err)
	}

	log.Printf("job assigned: job_id=%s worker_id=%s", jobID, workerID)

	return &pb.RequestJobResponse{
		HasJob: true,
		Job: &pb.Job{
			JobId:             &pb.JobID{Value: job.ID},
			ContainerImage:    job.ContainerImage,
			Command:           job.Command,
			RequiredResources: job.RequiredResources,
			Status:            pb.Status_STATUS_RUNNING,
			CreatedAtUnix:     job.CreatedAtUnix,
		},
	}, nil
}

func (s *Server) SubmitResult(ctx context.Context, req *pb.SubmitResultRequest) (*pb.SubmitResultResponse, error) {
	workerID := req.GetWorkerId().GetValue()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if req.GetResult() == nil || req.GetResult().GetJobId().GetValue() == "" {
		return nil, status.Error(codes.InvalidArgument, "result.job_id is required")
	}

	jobID := req.GetResult().GetJobId().GetValue()

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job for result submission: %v", err)
	}
	if job == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	assignedWorkerID, err := store.GetJobWorker(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get assigned worker: %v", err)
	}
	if assignedWorkerID == "" {
		assignedWorkerID = job.AssignedWorkerID
	}
	if assignedWorkerID == "" {
		return nil, status.Error(codes.FailedPrecondition, "job is not assigned")
	}
	if assignedWorkerID != workerID {
		return nil, status.Error(codes.PermissionDenied, "worker is not assigned to this job")
	}

	currentStatus, err := store.GetJobStatus(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job status: %v", err)
	}
	if currentStatus == "" {
		return nil, status.Error(codes.NotFound, "job status not found")
	}

	incomingStatus := req.GetResult().GetStatus()
	if incomingStatus != pb.Status_STATUS_RUNNING && incomingStatus != pb.Status_STATUS_SUCCEEDED && incomingStatus != pb.Status_STATUS_FAILED {
		return nil, status.Error(codes.InvalidArgument, "result.status must be RUNNING, SUCCEEDED, or FAILED")
	}

	if incomingStatus != pb.Status_STATUS_RUNNING {
		if currentStatus == store.JobStatusCompleted || currentStatus == store.JobStatusFailed {
			return &pb.SubmitResultResponse{Accepted: true}, nil
		}
		if currentStatus != store.JobStatusRunning {
			return nil, status.Error(codes.FailedPrecondition, "job is not in running state")
		}
	}

	result := &store.ResultRecord{
		JobID:          jobID,
		WorkerID:       workerID,
		ResultStatus:   incomingStatus.String(),
		Partial:        incomingStatus == pb.Status_STATUS_RUNNING,
		ExitCode:       req.GetResult().GetExitCode(),
		StdoutExcerpt:  req.GetResult().GetStdoutExcerpt(),
		StderrExcerpt:  req.GetResult().GetStderrExcerpt(),
		OutputURI:      req.GetResult().GetOutputUri(),
		FinishedAtUnix: req.GetResult().GetFinishedAtUnix(),
	}
	if !result.Partial && result.FinishedAtUnix == 0 {
		result.FinishedAtUnix = time.Now().Unix()
	}

	if result.Partial {
		if err := store.SaveResult(ctx, s.redis, result); err != nil {
			return nil, status.Errorf(codes.Internal, "save result: %v", err)
		}
		log.Printf("partial result saved: job_id=%s worker_id=%s", jobID, workerID)
		return &pb.SubmitResultResponse{Accepted: true}, nil
	}

	terminalStatus := store.JobStatusCompleted
	if incomingStatus == pb.Status_STATUS_FAILED {
		terminalStatus = store.JobStatusFailed
	}
	if err := store.FinalizeJobWithResult(ctx, s.redis, job, result, terminalStatus); err != nil {
		return nil, status.Errorf(codes.Internal, "finalize job with result: %v", err)
	}

	log.Printf("result submitted: job_id=%s worker_id=%s", jobID, workerID)

	return &pb.SubmitResultResponse{Accepted: true}, nil
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if req.GetContainerImage() == "" {
		return nil, status.Error(codes.InvalidArgument, "container_image is required")
	}
	if len(req.GetCommand()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	if req.GetRequiredResources() == nil {
		return nil, status.Error(codes.InvalidArgument, "required_resources is required")
	}

	jobID := uuid.NewString()
	job := &store.JobRecord{
		ID:                jobID,
		ContainerImage:    req.GetContainerImage(),
		Command:           req.GetCommand(),
		RequiredResources: req.GetRequiredResources(),
		Environment:       req.GetEnvironment(),
		MaxRuntimeSeconds: req.GetMaxRuntimeSeconds(),
		Status:            store.JobStatusQueued,
		CreatedAtUnix:     time.Now().Unix(),
	}

	if err := store.EnqueueJob(ctx, s.redis, job); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue job: %v", err)
	}

	log.Printf("job submitted: job_id=%s image=%s", jobID, req.GetContainerImage())

	return &pb.SubmitJobResponse{
		JobId:  &pb.JobID{Value: jobID},
		Status: pb.Status_STATUS_QUEUED,
	}, nil
}

func (s *Server) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
	jobID := req.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	statusStr, err := store.GetJobStatus(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job status: %v", err)
	}

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job metadata: %v", err)
	}
	if statusStr == "" && job == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	assignedWorkerID := ""
	if mappedWorkerID, mapErr := store.GetJobWorker(ctx, s.redis, jobID); mapErr == nil && mappedWorkerID != "" {
		assignedWorkerID = mappedWorkerID
	} else if job != nil {
		assignedWorkerID = job.AssignedWorkerID
	}

	return &pb.GetJobStatusResponse{
		Status:           redisStatusToPB(statusStr),
		AssignedWorkerId: assignedWorkerID,
		UpdatedAtUnix:    time.Now().Unix(),
	}, nil
}

func (s *Server) GetJobResult(ctx context.Context, req *pb.GetJobResultRequest) (*pb.GetJobResultResponse, error) {
	jobID := req.GetJobId().GetValue()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	statusStr, err := store.GetJobStatus(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job status: %v", err)
	}

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job metadata: %v", err)
	}

	resultRecord, err := store.GetResult(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job result: %v", err)
	}
	if statusStr == "" && job == nil && resultRecord == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if resultRecord == nil {
		return &pb.GetJobResultResponse{
			Status:          redisStatusToPB(statusStr),
			ResultAvailable: false,
		}, nil
	}

	resultStatus := redisStatusToPB(statusStr)
	if resultRecord.ResultStatus != "" {
		if val, ok := pb.Status_value[resultRecord.ResultStatus]; ok {
			resultStatus = pb.Status(val)
		}
	}

	return &pb.GetJobResultResponse{
		Status:          redisStatusToPB(statusStr),
		ResultAvailable: true,
		Result: &pb.Result{
			JobId:          &pb.JobID{Value: resultRecord.JobID},
			Status:         resultStatus,
			ExitCode:       resultRecord.ExitCode,
			StdoutExcerpt:  resultRecord.StdoutExcerpt,
			StderrExcerpt:  resultRecord.StderrExcerpt,
			OutputUri:      resultRecord.OutputURI,
			FinishedAtUnix: resultRecord.FinishedAtUnix,
		},
	}, nil
}

func redisStatusToPB(state string) pb.Status {
	switch state {
	case store.JobStatusQueued:
		return pb.Status_STATUS_QUEUED
	case store.JobStatusRunning:
		return pb.Status_STATUS_RUNNING
	case store.JobStatusCompleted:
		return pb.Status_STATUS_SUCCEEDED
	case store.JobStatusFailed:
		return pb.Status_STATUS_FAILED
	default:
		return pb.Status_STATUS_UNSPECIFIED
	}
}
