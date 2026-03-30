package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"crypto/tls"

	pb "coordinator/pkg/pb"

	"github.com/redis/go-redis/v9"
)

const (
	workersKey  = "workers"
	jobQueueKey = "job_queue"
)

const (
	JobStatusQueued    = "queued"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
)

type WorkerRecord struct {
	ID                 string           `json:"id"`
	AvailableResources *pb.ResourceSpec `json:"available_resources,omitempty"`
	WorkerVersion      string           `json:"worker_version,omitempty"`
	RegisteredAtUnix   int64            `json:"registered_at_unix"`
	LastHeartbeatUnix  int64            `json:"last_heartbeat_unix,omitempty"`
}

type JobRecord struct {
	ID                string            `json:"id"`
	ContainerImage    string            `json:"container_image"`
	Command           []string          `json:"command"`
	RequiredResources *pb.ResourceSpec  `json:"required_resources,omitempty"`
	Environment       map[string]string `json:"environment,omitempty"`
	MaxRuntimeSeconds int32             `json:"max_runtime_seconds,omitempty"`
	Status            string            `json:"status"`
	CreatedAtUnix     int64             `json:"created_at_unix"`
	AssignedWorkerID  string            `json:"assigned_worker_id,omitempty"`
}

type ResultRecord struct {
	JobID          string `json:"job_id"`
	WorkerID       string `json:"worker_id"`
	ResultStatus   string `json:"result_status,omitempty"`
	Partial        bool   `json:"partial,omitempty"`
	ExitCode       int32  `json:"exit_code,omitempty"`
	StdoutExcerpt  string `json:"stdout_excerpt,omitempty"`
	StderrExcerpt  string `json:"stderr_excerpt,omitempty"`
	OutputURI      string `json:"output_uri,omitempty"`
	FinishedAtUnix int64  `json:"finished_at_unix,omitempty"`
}

func workerKey(workerID string) string {
	return "worker:" + workerID
}

func workerAliveKey(workerID string) string {
	return "worker_alive:" + workerID
}

func jobKey(jobID string) string {
	return "job:" + jobID
}

func jobStatusKey(jobID string) string {
	return "job_status:" + jobID
}

func resultKey(jobID string) string {
	return "result:" + jobID
}

func jobWorkerKey(jobID string) string {
	return "job_worker:" + jobID
}

func NewRedisClient(ctx context.Context, addr, password string, db int, useTLS bool) (*redis.Client, error) {
	opt := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	if useTLS {
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	client := redis.NewClient(opt)

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}

	return client, nil
}

func ClaimNextQueuedJobForWorker(ctx context.Context, client *redis.Client, workerID string) (string, error) {
	const script = `
local queue_key = KEYS[1]
local status_prefix = ARGV[1]
local worker_prefix = ARGV[2]
local worker_id = ARGV[3]
local running_status = ARGV[4]

local job_id = redis.call('LPOP', queue_key)
if not job_id then
  return ''
end

redis.call('SET', status_prefix .. job_id, running_status)
redis.call('SET', worker_prefix .. job_id, worker_id)
return job_id
`

	res, err := client.Eval(ctx, script, []string{jobQueueKey}, "job_status:", "job_worker:", workerID, JobStatusRunning).Result()
	if err != nil {
		return "", fmt.Errorf("claim next queued job for worker %s: %w", workerID, err)
	}

	if res == nil {
		return "", nil
	}

	s, ok := res.(string)
	if ok {
		return s, nil
	}

	if b, ok := res.([]byte); ok {
		return string(b), nil
	}

	return fmt.Sprint(res), nil
}

func SaveWorker(ctx context.Context, client *redis.Client, worker *WorkerRecord) error {
	payload, err := json.Marshal(worker)
	if err != nil {
		return fmt.Errorf("marshal worker: %w", err)
	}

	pipe := client.TxPipeline()
	pipe.SAdd(ctx, workersKey, worker.ID)
	pipe.Set(ctx, workerKey(worker.ID), payload, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save worker %s: %w", worker.ID, err)
	}

	return nil
}

func ListWorkers(ctx context.Context, client *redis.Client) ([]string, error) {
	workers, err := client.SMembers(ctx, workersKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return workers, nil
}

func IsWorkerAlive(ctx context.Context, client *redis.Client, workerID string) (bool, error) {
	exists, err := client.Exists(ctx, workerAliveKey(workerID)).Result()
	if err != nil {
		return false, fmt.Errorf("check worker alive %s: %w", workerID, err)
	}
	return exists == 1, nil
}

func RemoveWorker(ctx context.Context, client *redis.Client, workerID string) error {
	pipe := client.TxPipeline()
	pipe.SRem(ctx, workersKey, workerID)
	pipe.Del(ctx, workerKey(workerID))
	pipe.Del(ctx, workerAliveKey(workerID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove worker %s: %w", workerID, err)
	}
	return nil
}

func GetWorker(ctx context.Context, client *redis.Client, workerID string) (*WorkerRecord, error) {
	raw, err := client.Get(ctx, workerKey(workerID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get worker %s: %w", workerID, err)
	}

	var worker WorkerRecord
	if err := json.Unmarshal([]byte(raw), &worker); err != nil {
		return nil, fmt.Errorf("unmarshal worker %s: %w", workerID, err)
	}

	return &worker, nil
}

func SetWorkerAlive(ctx context.Context, client *redis.Client, workerID string, ttl time.Duration) error {
	if err := client.Set(ctx, workerAliveKey(workerID), "1", ttl).Err(); err != nil {
		return fmt.Errorf("set worker alive %s: %w", workerID, err)
	}
	return nil
}

func EnqueueJob(ctx context.Context, client *redis.Client, job *JobRecord) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	pipe := client.TxPipeline()
	pipe.Set(ctx, jobKey(job.ID), payload, 0)
	pipe.Set(ctx, jobStatusKey(job.ID), JobStatusQueued, 0)
	pipe.RPush(ctx, jobQueueKey, job.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue job %s: %w", job.ID, err)
	}

	return nil
}

func PopQueuedJobID(ctx context.Context, client *redis.Client) (string, error) {
	jobID, err := client.LPop(ctx, jobQueueKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lpop %s: %w", jobQueueKey, err)
	}

	return jobID, nil
}

func RequeueJobID(ctx context.Context, client *redis.Client, jobID string) error {
	pipe := client.TxPipeline()
	pipe.Set(ctx, jobStatusKey(jobID), JobStatusQueued, 0)
	pipe.Del(ctx, jobWorkerKey(jobID))
	pipe.RPush(ctx, jobQueueKey, jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("requeue job id %s: %w", jobID, err)
	}
	return nil
}

func SaveJob(ctx context.Context, client *redis.Client, job *JobRecord) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	if err := client.Set(ctx, jobKey(job.ID), payload, 0).Err(); err != nil {
		return fmt.Errorf("save job %s: %w", job.ID, err)
	}

	return nil
}

func SetJobWorker(ctx context.Context, client *redis.Client, jobID, workerID string) error {
	if err := client.Set(ctx, jobWorkerKey(jobID), workerID, 0).Err(); err != nil {
		return fmt.Errorf("set job worker %s: %w", jobID, err)
	}
	return nil
}

func GetJobWorker(ctx context.Context, client *redis.Client, jobID string) (string, error) {
	workerID, err := client.Get(ctx, jobWorkerKey(jobID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get job worker %s: %w", jobID, err)
	}
	return workerID, nil
}

func DeleteJobWorker(ctx context.Context, client *redis.Client, jobID string) error {
	if err := client.Del(ctx, jobWorkerKey(jobID)).Err(); err != nil {
		return fmt.Errorf("delete job worker %s: %w", jobID, err)
	}
	return nil
}

func ListJobWorkerAssignments(ctx context.Context, client *redis.Client) (map[string]string, error) {
	assignments := make(map[string]string)
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, "job_worker:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan job_worker:*: %w", err)
		}
		if len(keys) > 0 {
			vals, err := client.MGet(ctx, keys...).Result()
			if err != nil {
				return nil, fmt.Errorf("mget job worker assignments: %w", err)
			}
			for i, key := range keys {
				if vals[i] == nil {
					continue
				}
				workerID, ok := vals[i].(string)
				if !ok || workerID == "" {
					continue
				}
				jobID := key[len("job_worker:"):]
				assignments[jobID] = workerID
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return assignments, nil
}

func JobsForWorker(ctx context.Context, client *redis.Client, workerID string) ([]string, error) {
	assignments, err := ListJobWorkerAssignments(ctx, client)
	if err != nil {
		return nil, err
	}

	jobs := make([]string, 0)
	for jobID, assignedWorker := range assignments {
		if assignedWorker == workerID {
			jobs = append(jobs, jobID)
		}
	}
	return jobs, nil
}

func GetJob(ctx context.Context, client *redis.Client, jobID string) (*JobRecord, error) {
	raw, err := client.Get(ctx, jobKey(jobID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", jobID, err)
	}

	var job JobRecord
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job %s: %w", jobID, err)
	}

	return &job, nil
}

func SetJobStatus(ctx context.Context, client *redis.Client, jobID, status string) error {
	if err := client.Set(ctx, jobStatusKey(jobID), status, 0).Err(); err != nil {
		return fmt.Errorf("set job status %s: %w", jobID, err)
	}
	return nil
}

func GetJobStatus(ctx context.Context, client *redis.Client, jobID string) (string, error) {
	status, err := client.Get(ctx, jobStatusKey(jobID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get job status %s: %w", jobID, err)
	}

	return status, nil
}

func SaveResult(ctx context.Context, client *redis.Client, result *ResultRecord) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	if err := client.Set(ctx, resultKey(result.JobID), payload, 0).Err(); err != nil {
		return fmt.Errorf("save result %s: %w", result.JobID, err)
	}

	return nil
}

func FinalizeJobWithResult(ctx context.Context, client *redis.Client, job *JobRecord, result *ResultRecord, terminalStatus string) error {
	if job == nil {
		return fmt.Errorf("finalize job: nil job")
	}
	if terminalStatus != JobStatusCompleted && terminalStatus != JobStatusFailed {
		return fmt.Errorf("finalize job %s: invalid terminal status %s", job.ID, terminalStatus)
	}

	job.Status = terminalStatus
	job.AssignedWorkerID = ""

	jobPayload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal finalized job %s: %w", job.ID, err)
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal finalized result %s: %w", job.ID, err)
	}

	pipe := client.TxPipeline()
	pipe.Set(ctx, resultKey(job.ID), resultPayload, 0)
	pipe.Set(ctx, jobStatusKey(job.ID), terminalStatus, 0)
	pipe.Del(ctx, jobWorkerKey(job.ID))
	pipe.Set(ctx, jobKey(job.ID), jobPayload, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("finalize job %s with result: %w", job.ID, err)
	}

	return nil
}

func GetResult(ctx context.Context, client *redis.Client, jobID string) (*ResultRecord, error) {
	raw, err := client.Get(ctx, resultKey(jobID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get result %s: %w", jobID, err)
	}

	var result ResultRecord
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("unmarshal result %s: %w", jobID, err)
	}

	return &result, nil
}

func RequeueRunningJob(ctx context.Context, client *redis.Client, jobID string) error {
	job, err := GetJob(ctx, client, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return nil
	}

	status, err := GetJobStatus(ctx, client, jobID)
	if err != nil {
		return err
	}
	if status != JobStatusRunning {
		return nil
	}

	job.Status = JobStatusQueued
	job.AssignedWorkerID = ""
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal requeue job %s: %w", jobID, err)
	}

	pipe := client.TxPipeline()
	pipe.Set(ctx, jobKey(jobID), payload, 0)
	pipe.Set(ctx, jobStatusKey(jobID), JobStatusQueued, 0)
	pipe.Del(ctx, jobWorkerKey(jobID))
	pipe.RPush(ctx, jobQueueKey, jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("requeue running job %s: %w", jobID, err)
	}
	return nil
}
