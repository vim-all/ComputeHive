package scheduler

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"coordinator/internal/store"
	pb "coordinator/pkg/pb"

	"github.com/redis/go-redis/v9"
)

// Scheduler performs fair worker selection and worker health monitoring.
type Scheduler struct {
	redis      *redis.Client
	mu         sync.Mutex
	nextWorker int
}

func New(redisClient *redis.Client) *Scheduler {
	return &Scheduler{redis: redisClient}
}

// PickWorker returns the next eligible worker using round-robin fairness.
func (s *Scheduler) PickWorker(ctx context.Context, job *pb.Job) (string, error) {
	workers, err := store.ListWorkers(ctx, s.redis)
	if err != nil {
		return "", err
	}
	if len(workers) == 0 {
		return "", nil
	}

	sort.Strings(workers)
	eligible := make([]string, 0, len(workers))
	for _, workerID := range workers {
		alive, err := store.IsWorkerAlive(ctx, s.redis, workerID)
		if err != nil {
			return "", err
		}
		if !alive {
			continue
		}

		if job == nil || job.GetRequiredResources() == nil {
			eligible = append(eligible, workerID)
			continue
		}

		workerMeta, err := store.GetWorker(ctx, s.redis, workerID)
		if err != nil {
			return "", err
		}
		if workerMeta == nil || workerMeta.AvailableResources == nil {
			eligible = append(eligible, workerID)
			continue
		}

		req := job.GetRequiredResources()
		res := workerMeta.AvailableResources
		if res.GetCpuCores() >= req.GetCpuCores() &&
			res.GetGpuCount() >= req.GetGpuCount() &&
			res.GetMemoryMb() >= req.GetMemoryMb() {
			eligible = append(eligible, workerID)
		}
	}

	if len(eligible) == 0 {
		return "", nil
	}

	s.mu.Lock()
	idx := s.nextWorker % len(eligible)
	picked := eligible[idx]
	s.nextWorker = (idx + 1) % len(eligible)
	s.mu.Unlock()

	return picked, nil
}

// AssignJob marks job as running and records its worker assignment.
func (s *Scheduler) AssignJob(ctx context.Context, job *pb.Job, workerID string) error {
	if job == nil || job.GetJobId().GetValue() == "" {
		return nil
	}
	jobID := job.GetJobId().GetValue()

	if err := store.SetJobStatus(ctx, s.redis, jobID, store.JobStatusRunning); err != nil {
		return err
	}
	if err := store.SetJobWorker(ctx, s.redis, jobID, workerID); err != nil {
		return err
	}

	jobRecord, err := store.GetJob(ctx, s.redis, jobID)
	if err != nil {
		return err
	}
	if jobRecord == nil {
		return nil
	}

	jobRecord.Status = store.JobStatusRunning
	jobRecord.AssignedWorkerID = workerID
	if err := store.SaveJob(ctx, s.redis, jobRecord); err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) StartWorkerMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkWorkers(ctx)
		}
	}
}

func (s *Scheduler) checkWorkers(ctx context.Context) {
	workers, err := store.ListWorkers(ctx, s.redis)
	if err != nil {
		log.Printf("monitor: list workers failed: %v", err)
		return
	}

	for _, workerID := range workers {
		alive, err := store.IsWorkerAlive(ctx, s.redis, workerID)
		if err != nil {
			log.Printf("monitor: worker heartbeat check failed worker_id=%s err=%v", workerID, err)
			continue
		}
		if alive {
			continue
		}

		jobs, err := store.JobsForWorker(ctx, s.redis, workerID)
		if err != nil {
			log.Printf("monitor: list assigned jobs failed worker_id=%s err=%v", workerID, err)
			continue
		}

		requeued := 0
		for _, jobID := range jobs {
			if err := store.RequeueRunningJob(ctx, s.redis, jobID); err != nil {
				log.Printf("monitor: requeue failed worker_id=%s job_id=%s err=%v", workerID, jobID, err)
				continue
			}
			requeued++
		}

		if err := store.RemoveWorker(ctx, s.redis, workerID); err != nil {
			log.Printf("monitor: remove worker failed worker_id=%s err=%v", workerID, err)
			continue
		}

		log.Printf("worker disconnected: worker_id=%s requeued_jobs=%d", workerID, requeued)
	}
}
