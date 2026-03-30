package store

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pb "coordinator/pkg/pb"
	"github.com/adhyan-jain/ComputeHive/worker/internal/config"
	"github.com/adhyan-jain/ComputeHive/worker/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Store struct {
	cfg config.Config

	mu                  sync.Mutex
	coordinatorConn     *grpc.ClientConn
	coordinatorClient   pb.WorkerServiceClient
	coordinatorWorkerID string
}

func New(cfg config.Config) *Store {
	return &Store{cfg: cfg}
}

func (s *Store) PullJob(ctx context.Context) (*domain.Job, bool, error) {
	client, workerID, err := s.coordinator(ctx)
	if err != nil {
		return nil, false, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, s.cfg.PollTimeout+s.cfg.RedisIOTimeout)
	defer cancel()

	response, err := client.RequestJob(requestCtx, &pb.RequestJobRequest{
		WorkerId: &pb.WorkerID{Value: workerID},
	})
	if isNotFound(err) {
		newWorkerID, registerErr := s.reRegister(requestCtx)
		if registerErr != nil {
			return nil, false, fmt.Errorf("request job from coordinator: worker re-register failed: %w", registerErr)
		}
		response, err = client.RequestJob(requestCtx, &pb.RequestJobRequest{
			WorkerId: &pb.WorkerID{Value: newWorkerID},
		})
	}
	if err != nil {
		return nil, false, fmt.Errorf("request job from coordinator: %w", err)
	}
	if !response.GetHasJob() || response.GetJob() == nil {
		return nil, false, nil
	}

	jobMessage := response.GetJob()
	resources := jobMessage.GetRequiredResources()
	job := &domain.Job{
		ID:             strings.TrimSpace(jobMessage.GetJobId().GetValue()),
		Status:         domain.JobStatusAssigned,
		ImageRef:       strings.TrimSpace(jobMessage.GetContainerImage()),
		Command:        append([]string(nil), jobMessage.GetCommand()...),
		CreatedAtUnix:  jobMessage.GetCreatedAtUnix(),
		TimeoutSeconds: 0,
	}
	if resources != nil {
		job.CPUCores = float64(resources.GetCpuCores())
		job.MemoryMB = int64(resources.GetMemoryMb())
		job.GPU = resources.GetGpuCount() > 0
	}

	return job, true, nil
}

func (s *Store) PublishHeartbeat(ctx context.Context, heartbeat domain.WorkerHeartbeat) error {
	client, workerID, err := s.coordinator(ctx)
	if err != nil {
		return err
	}

	_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId: &pb.WorkerID{Value: workerID},
		AvailableResources: &pb.ResourceSpec{
			CpuCores: int32(heartbeat.Resources.CPUCores),
			MemoryMb: int32(heartbeat.Resources.MemoryMB),
			GpuCount: boolToGPUCount(heartbeat.Resources.GPU),
		},
	})
	if isNotFound(err) {
		if _, registerErr := s.reRegister(ctx); registerErr != nil {
			return fmt.Errorf("publish coordinator heartbeat: worker re-register failed: %w", registerErr)
		}
		_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{
			WorkerId: &pb.WorkerID{Value: s.coordinatorWorkerID},
			AvailableResources: &pb.ResourceSpec{
				CpuCores: int32(heartbeat.Resources.CPUCores),
				MemoryMb: int32(heartbeat.Resources.MemoryMB),
				GpuCount: boolToGPUCount(heartbeat.Resources.GPU),
			},
		})
	}
	if err != nil {
		return fmt.Errorf("publish coordinator heartbeat: %w", err)
	}

	return nil
}

func (s *Store) PublishJobStatus(ctx context.Context, status domain.JobState) error {
	_ = ctx
	_ = status
	return nil
}

func (s *Store) PublishAssignment(ctx context.Context, assignment domain.Assignment) error {
	_ = ctx
	_ = assignment
	return nil
}

func (s *Store) PublishJobResult(ctx context.Context, result domain.JobResult) error {
	client, workerID, err := s.coordinator(ctx)
	if err != nil {
		return err
	}

	_, err = client.SubmitResult(ctx, &pb.SubmitResultRequest{
		WorkerId: &pb.WorkerID{Value: workerID},
		Result: &pb.Result{
			JobId:          &pb.JobID{Value: result.JobID},
			Status:         domainStatusToPB(result.Status),
			ExitCode:       int32(result.ExitCode),
			StdoutExcerpt:  trimTo(result.Stdout, 4096),
			StderrExcerpt:  trimTo(result.Stderr, 4096),
			FinishedAtUnix: result.FinishedAt.UTC().Unix(),
		},
	})
	if isNotFound(err) {
		newWorkerID, registerErr := s.reRegister(ctx)
		if registerErr != nil {
			return fmt.Errorf("submit result to coordinator: worker re-register failed: %w", registerErr)
		}
		_, err = client.SubmitResult(ctx, &pb.SubmitResultRequest{
			WorkerId: &pb.WorkerID{Value: newWorkerID},
			Result: &pb.Result{
				JobId:          &pb.JobID{Value: result.JobID},
				Status:         domainStatusToPB(result.Status),
				ExitCode:       int32(result.ExitCode),
				StdoutExcerpt:  trimTo(result.Stdout, 4096),
				StderrExcerpt:  trimTo(result.Stderr, 4096),
				FinishedAtUnix: result.FinishedAt.UTC().Unix(),
			},
		})
	}
	if err != nil {
		return fmt.Errorf("submit result to coordinator: %w", err)
	}

	return nil
}

func (s *Store) coordinator(ctx context.Context) (pb.WorkerServiceClient, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coordinatorClient == nil {
		dialCtx, cancel := context.WithTimeout(ctx, s.cfg.RedisDialTimeout)
		defer cancel()

		conn, err := grpc.DialContext(dialCtx, s.cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return nil, "", fmt.Errorf("dial coordinator: %w", err)
		}

		s.coordinatorConn = conn
		s.coordinatorClient = pb.NewWorkerServiceClient(conn)
	}

	if strings.TrimSpace(s.coordinatorWorkerID) == "" {
		if _, err := s.reRegister(ctx); err != nil {
			return nil, "", fmt.Errorf("register worker with coordinator: %w", err)
		}
	}

	return s.coordinatorClient, s.coordinatorWorkerID, nil
}

func (s *Store) reRegister(ctx context.Context) (string, error) {
	registerResponse, err := s.coordinatorClient.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerVersion: s.cfg.Version,
	})
	if err != nil {
		return "", err
	}

	workerID := strings.TrimSpace(registerResponse.GetWorkerId().GetValue())
	if workerID == "" {
		return "", fmt.Errorf("coordinator returned empty worker id")
	}

	s.coordinatorWorkerID = workerID
	return workerID, nil
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
	case domain.JobStatusCompleted:
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
	if err == nil {
		return false
	}
	return status.Code(err) == codes.NotFound
}
