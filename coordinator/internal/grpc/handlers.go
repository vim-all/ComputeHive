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

	if err := store.SetWorkerAlive(ctx, s.redis, workerID, 10*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "set worker heartbeat key: %v", err)
	}

	worker, err := store.GetWorker(ctx, s.redis, workerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get worker: %v", err)
	}
	if worker != nil {
		worker.AvailableResources = req.GetAvailableResources()
		worker.LastHeartbeatUnix = time.Now().Unix()
		if err := store.SaveWorker(ctx, s.redis, worker); err != nil {
			return nil, status.Errorf(codes.Internal, "update worker heartbeat metadata: %v", err)
		}
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

	jobID, err := store.PopQueuedJobID(ctx, s.redis)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "pop job from queue: %v", err)
	}
	if jobID == "" {
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job: %v", err)
	}
	if job == nil {
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	pbJob := &pb.Job{
		JobId:             &pb.JobID{Value: job.ID},
		ContainerImage:    job.ContainerImage,
		Command:           job.Command,
		RequiredResources: job.RequiredResources,
		CreatedAtUnix:     job.CreatedAtUnix,
	}

	pickedWorkerID, err := s.scheduler.PickWorker(ctx, pbJob)
	if err != nil {
		_ = store.RequeueJobID(ctx, s.redis, jobID)
		return nil, status.Errorf(codes.Internal, "pick worker: %v", err)
	}
	if pickedWorkerID == "" {
		if err := store.RequeueJobID(ctx, s.redis, jobID); err != nil {
			return nil, status.Errorf(codes.Internal, "requeue job without eligible worker: %v", err)
		}
		return &pb.RequestJobResponse{HasJob: false}, nil
	}
	if pickedWorkerID != workerID {
		if err := store.RequeueJobID(ctx, s.redis, jobID); err != nil {
			return nil, status.Errorf(codes.Internal, "requeue non-turn job: %v", err)
		}
		return &pb.RequestJobResponse{HasJob: false}, nil
	}

	if err := s.scheduler.AssignJob(ctx, pbJob, workerID); err != nil {
		_ = store.RequeueJobID(ctx, s.redis, jobID)
		return nil, status.Errorf(codes.Internal, "assign job: %v", err)
	}

	log.Printf("job assigned: job_id=%s worker_id=%s", jobID, workerID)

	return &pb.RequestJobResponse{
		HasJob: true,
		Job: &pb.Job{
			JobId:             pbJob.JobId,
			ContainerImage:    pbJob.ContainerImage,
			Command:           pbJob.Command,
			RequiredResources: pbJob.RequiredResources,
			Status:            pb.Status_STATUS_RUNNING,
			CreatedAtUnix:     pbJob.CreatedAtUnix,
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
	result := &store.ResultRecord{
		JobID:          jobID,
		WorkerID:       workerID,
		ResultStatus:   req.GetResult().GetStatus().String(),
		Partial:        req.GetResult().GetStatus() == pb.Status_STATUS_RUNNING,
		ExitCode:       req.GetResult().GetExitCode(),
		StdoutExcerpt:  req.GetResult().GetStdoutExcerpt(),
		StderrExcerpt:  req.GetResult().GetStderrExcerpt(),
		OutputURI:      req.GetResult().GetOutputUri(),
		FinishedAtUnix: req.GetResult().GetFinishedAtUnix(),
	}

	if err := store.SaveResult(ctx, s.redis, result); err != nil {
		return nil, status.Errorf(codes.Internal, "save result: %v", err)
	}

	if result.Partial {
		log.Printf("partial result saved: job_id=%s worker_id=%s", jobID, workerID)
		return &pb.SubmitResultResponse{Accepted: true}, nil
	}

	if err := store.SetJobStatus(ctx, s.redis, jobID, store.JobStatusCompleted); err != nil {
		return nil, status.Errorf(codes.Internal, "set completed job status: %v", err)
	}
	if err := store.DeleteJobWorker(ctx, s.redis, jobID); err != nil {
		return nil, status.Errorf(codes.Internal, "clear job-worker assignment: %v", err)
	}

	job, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load completed job: %v", err)
	}
	if job != nil {
		job.Status = store.JobStatusCompleted
		job.AssignedWorkerID = ""
		if err := store.SaveJob(ctx, s.redis, job); err != nil {
			return nil, status.Errorf(codes.Internal, "save completed job: %v", err)
		}
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

	resultRecord, err := store.GetResult(ctx, s.redis, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job result: %v", err)
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
	default:
		return pb.Status_STATUS_UNSPECIFIED
	}
}
