package config

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	WorkerID                string
	CoordinatorAddr         string
	PollViaServer           bool
	RedisAddr               string
	RedisUsername           string
	RedisPassword           string
	RedisDB                 int
	RedisUseTLS             bool
	QueueKey                string
	ResultsKey              string
	WorkerKeyPrefix         string
	JobKeyPrefix            string
	WorkerEventsChannel     string
	JobEventsChannel        string
	HeartbeatInterval       time.Duration
	HeartbeatTTL            time.Duration
	PollTimeout             time.Duration
	DefaultJobTimeout       time.Duration
	ResultTTL               time.Duration
	RedisDialTimeout        time.Duration
	RedisIOTimeout          time.Duration
	ArtifactDownloadTimeout time.Duration
	ArtifactMaxBytes        int64
	OutputCollectionDir     string
	OutputMaxFiles          int
	OutputMaxBytes          int64
	S3SignRequests          bool
	S3BucketURL             string
	S3PublicBucketURL       string
	S3AccessKeyID           string
	S3SecretAccessKey       string
	S3SessionToken          string
	S3SigningRegion         string
	S3SigningService        string
	DockerBinary            string
	AllowGPUJobs            bool
	Version                 string
}

func Parse(args []string) (Config, error) {
	dotEnv, _ := loadDotEnv(".env", "worker/.env")
	getenv := func(key string) string {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
		return strings.TrimSpace(dotEnv[key])
	}

	return parse(args, getenv)
}

func parse(args []string, getenv func(string) string) (Config, error) {
	cfg := Config{
		WorkerID:                envOr(getenv, "WORKER_ID", defaultWorkerID()),
		CoordinatorAddr:         envOr(getenv, "COORDINATOR_ADDR", "127.0.0.1:50051"),
		PollViaServer:           envBool(getenv, "POLL_VIA_SERVER", true),
		RedisAddr:               envOr(getenv, "REDIS_ADDR", "127.0.0.1:6379"),
		RedisUsername:           envOr(getenv, "REDIS_USERNAME", ""),
		RedisPassword:           envOr(getenv, "REDIS_PASSWORD", ""),
		RedisDB:                 envInt(getenv, "REDIS_DB", 0),
		RedisUseTLS:             envBool(getenv, "REDIS_USE_TLS", false),
		QueueKey:                envOr(getenv, "QUEUE_KEY", "computehive:jobs:pending"),
		ResultsKey:              envOr(getenv, "RESULTS_KEY", "computehive:jobs:results"),
		WorkerKeyPrefix:         envOr(getenv, "WORKER_KEY_PREFIX", "computehive:workers"),
		JobKeyPrefix:            envOr(getenv, "JOB_KEY_PREFIX", "computehive:jobs"),
		WorkerEventsChannel:     envOr(getenv, "WORKER_EVENTS_CHANNEL", "computehive:events:workers"),
		JobEventsChannel:        envOr(getenv, "JOB_EVENTS_CHANNEL", "computehive:events:jobs"),
		HeartbeatInterval:       envDuration(getenv, "HEARTBEAT_INTERVAL", 5*time.Second),
		HeartbeatTTL:            envDuration(getenv, "HEARTBEAT_TTL", 15*time.Second),
		PollTimeout:             envDuration(getenv, "POLL_TIMEOUT", 5*time.Second),
		DefaultJobTimeout:       envDuration(getenv, "DEFAULT_JOB_TIMEOUT", 30*time.Minute),
		ResultTTL:               envDuration(getenv, "RESULT_TTL", 7*24*time.Hour),
		RedisDialTimeout:        envDuration(getenv, "REDIS_DIAL_TIMEOUT", 5*time.Second),
		RedisIOTimeout:          envDuration(getenv, "REDIS_IO_TIMEOUT", 5*time.Second),
		ArtifactDownloadTimeout: envDuration(getenv, "ARTIFACT_DOWNLOAD_TIMEOUT", 10*time.Minute),
		ArtifactMaxBytes:        envInt64(getenv, "ARTIFACT_MAX_BYTES", 1<<30),
		OutputCollectionDir:     envOr(getenv, "OUTPUT_COLLECTION_DIR", "/computehive/output"),
		OutputMaxFiles:          envInt(getenv, "OUTPUT_MAX_FILES", 256),
		OutputMaxBytes:          envInt64(getenv, "OUTPUT_MAX_BYTES", 10<<30),
		S3SignRequests:          envBool(getenv, "S3_SIGN_REQUESTS", false),
		S3BucketURL:             envOr(getenv, "S3_BUCKET", ""),
		S3PublicBucketURL:       envOr(getenv, "S3_PUBLIC_BUCKET_URL", ""),
		S3AccessKeyID:           envOr(getenv, "S3_ACCESS_KEY_ID", ""),
		S3SecretAccessKey:       envOr(getenv, "S3_SECRET_ACCESS_KEY", ""),
		S3SessionToken:          envOr(getenv, "S3_SESSION_TOKEN", ""),
		S3SigningRegion:         envOr(getenv, "S3_SIGNING_REGION", "auto"),
		S3SigningService:        envOr(getenv, "S3_SIGNING_SERVICE", "s3"),
		DockerBinary:            envOr(getenv, "DOCKER_BINARY", "docker"),
		AllowGPUJobs:            envBool(getenv, "ALLOW_GPU_JOBS", true),
		Version:                 envOr(getenv, "WORKER_VERSION", "dev"),
	}
	if !cfg.S3SignRequests && strings.TrimSpace(getenv("S3_SIGN_REQUESTS")) == "" {
		cfg.S3SignRequests = strings.TrimSpace(cfg.S3AccessKeyID) != "" && strings.TrimSpace(cfg.S3SecretAccessKey) != ""
	}

	if err := applyRedisURL(&cfg, envOr(getenv, "REDIS_URL", "")); err != nil {
		return Config{}, err
	}
	if err := applyRedisURL(&cfg, strings.TrimSpace(cfg.RedisAddr)); err != nil {
		return Config{}, err
	}
	if err := applyRedisURL(&cfg, strings.TrimSpace(getenv("REDIS_DB"))); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.StringVar(&cfg.WorkerID, "worker-id", cfg.WorkerID, "Unique ID for this worker")
	fs.StringVar(&cfg.CoordinatorAddr, "coordinator-addr", cfg.CoordinatorAddr, "Coordinator gRPC address used for job polling")
	fs.BoolVar(&cfg.PollViaServer, "poll-via-server", cfg.PollViaServer, "Poll jobs from coordinator server instead of Redis queue")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", cfg.RedisAddr, "Redis host:port")
	fs.StringVar(&cfg.RedisUsername, "redis-username", cfg.RedisUsername, "Redis username (ACL)")
	fs.StringVar(&cfg.RedisPassword, "redis-password", cfg.RedisPassword, "Redis password")
	fs.IntVar(&cfg.RedisDB, "redis-db", cfg.RedisDB, "Redis database number")
	fs.BoolVar(&cfg.RedisUseTLS, "redis-use-tls", cfg.RedisUseTLS, "Enable TLS when connecting to Redis")
	fs.StringVar(&cfg.QueueKey, "queue-key", cfg.QueueKey, "Redis key containing pending jobs")
	fs.StringVar(&cfg.ResultsKey, "results-key", cfg.ResultsKey, "Redis list used to store finished job results")
	fs.StringVar(&cfg.WorkerKeyPrefix, "worker-key-prefix", cfg.WorkerKeyPrefix, "Redis prefix for worker state")
	fs.StringVar(&cfg.JobKeyPrefix, "job-key-prefix", cfg.JobKeyPrefix, "Redis prefix for job state")
	fs.StringVar(&cfg.WorkerEventsChannel, "worker-events-channel", cfg.WorkerEventsChannel, "Redis pub/sub channel for worker events")
	fs.StringVar(&cfg.JobEventsChannel, "job-events-channel", cfg.JobEventsChannel, "Redis pub/sub channel for job events")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", cfg.HeartbeatInterval, "How often the worker sends heartbeats")
	fs.DurationVar(&cfg.HeartbeatTTL, "heartbeat-ttl", cfg.HeartbeatTTL, "TTL for worker heartbeat records")
	fs.DurationVar(&cfg.PollTimeout, "poll-timeout", cfg.PollTimeout, "Blocking timeout while waiting for jobs")
	fs.DurationVar(&cfg.DefaultJobTimeout, "default-job-timeout", cfg.DefaultJobTimeout, "Fallback timeout for jobs without their own timeout")
	fs.DurationVar(&cfg.ResultTTL, "result-ttl", cfg.ResultTTL, "TTL for persisted job status and result keys")
	fs.DurationVar(&cfg.RedisDialTimeout, "redis-dial-timeout", cfg.RedisDialTimeout, "Dial timeout for Redis")
	fs.DurationVar(&cfg.RedisIOTimeout, "redis-io-timeout", cfg.RedisIOTimeout, "Read/write timeout for Redis")
	fs.DurationVar(&cfg.ArtifactDownloadTimeout, "artifact-download-timeout", cfg.ArtifactDownloadTimeout, "Timeout for downloading artifact archives")
	fs.Int64Var(&cfg.ArtifactMaxBytes, "artifact-max-bytes", cfg.ArtifactMaxBytes, "Maximum allowed artifact archive size in bytes")
	fs.StringVar(&cfg.OutputCollectionDir, "output-collection-dir", cfg.OutputCollectionDir, "Directory exposed inside the job container for returned job outputs")
	fs.IntVar(&cfg.OutputMaxFiles, "output-max-files", cfg.OutputMaxFiles, "Maximum number of output files to collect from a job")
	fs.Int64Var(&cfg.OutputMaxBytes, "output-max-bytes", cfg.OutputMaxBytes, "Maximum total size in bytes for collected job outputs")
	fs.BoolVar(&cfg.S3SignRequests, "s3-sign-requests", cfg.S3SignRequests, "Sign artifact download requests with AWS SigV4")
	fs.StringVar(&cfg.S3BucketURL, "s3-bucket", cfg.S3BucketURL, "Path-style S3 bucket URL used for artifact download and output uploads")
	fs.StringVar(&cfg.S3PublicBucketURL, "s3-public-bucket-url", cfg.S3PublicBucketURL, "Public base URL used when returning uploaded output links")
	fs.StringVar(&cfg.S3AccessKeyID, "s3-access-key-id", cfg.S3AccessKeyID, "S3 access key ID used for artifact request signing")
	fs.StringVar(&cfg.S3SecretAccessKey, "s3-secret-access-key", cfg.S3SecretAccessKey, "S3 secret access key used for artifact request signing")
	fs.StringVar(&cfg.S3SessionToken, "s3-session-token", cfg.S3SessionToken, "S3 session token used for artifact request signing")
	fs.StringVar(&cfg.S3SigningRegion, "s3-signing-region", cfg.S3SigningRegion, "Signing region for S3-compatible artifact downloads")
	fs.StringVar(&cfg.S3SigningService, "s3-signing-service", cfg.S3SigningService, "Signing service for S3-compatible artifact downloads")
	fs.StringVar(&cfg.DockerBinary, "docker-binary", cfg.DockerBinary, "Docker CLI binary path")
	fs.BoolVar(&cfg.AllowGPUJobs, "allow-gpu-jobs", cfg.AllowGPUJobs, "Allow jobs that request GPU access")
	fs.StringVar(&cfg.Version, "version", cfg.Version, "Worker version string")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if err := applyRedisURL(&cfg, strings.TrimSpace(cfg.RedisAddr)); err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(cfg.WorkerID) == "" {
		return Config{}, fmt.Errorf("worker-id is required")
	}
	if cfg.PollViaServer && strings.TrimSpace(cfg.CoordinatorAddr) == "" {
		return Config{}, fmt.Errorf("coordinator-addr is required when poll-via-server is enabled")
	}
	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return Config{}, fmt.Errorf("redis-addr is required")
	}
	if strings.TrimSpace(cfg.QueueKey) == "" {
		return Config{}, fmt.Errorf("queue-key is required")
	}
	if cfg.HeartbeatInterval <= 0 {
		return Config{}, fmt.Errorf("heartbeat-interval must be > 0")
	}
	if cfg.HeartbeatTTL < cfg.HeartbeatInterval {
		cfg.HeartbeatTTL = 3 * cfg.HeartbeatInterval
	}
	if cfg.PollTimeout <= 0 {
		return Config{}, fmt.Errorf("poll-timeout must be > 0")
	}
	if cfg.DefaultJobTimeout <= 0 {
		return Config{}, fmt.Errorf("default-job-timeout must be > 0")
	}
	if cfg.ResultTTL <= 0 {
		return Config{}, fmt.Errorf("result-ttl must be > 0")
	}
	if cfg.RedisDialTimeout <= 0 {
		return Config{}, fmt.Errorf("redis-dial-timeout must be > 0")
	}
	if cfg.RedisIOTimeout <= 0 {
		return Config{}, fmt.Errorf("redis-io-timeout must be > 0")
	}
	if cfg.ArtifactDownloadTimeout <= 0 {
		return Config{}, fmt.Errorf("artifact-download-timeout must be > 0")
	}
	if cfg.ArtifactMaxBytes <= 0 {
		return Config{}, fmt.Errorf("artifact-max-bytes must be > 0")
	}
	if strings.TrimSpace(cfg.OutputCollectionDir) == "" {
		return Config{}, fmt.Errorf("output-collection-dir is required")
	}
	if cfg.OutputMaxFiles <= 0 {
		return Config{}, fmt.Errorf("output-max-files must be > 0")
	}
	if cfg.OutputMaxBytes <= 0 {
		return Config{}, fmt.Errorf("output-max-bytes must be > 0")
	}

	return cfg, nil
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "worker-local"
	}

	return fmt.Sprintf("worker-%s", strings.ToLower(hostname))
}

func envOr(getenv func(string) string, key, fallback string) string {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func envInt(getenv func(string) string, key string, fallback int) int {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envInt64(getenv func(string) string, key string, fallback int64) int64 {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}

	return parsed
}

func envBool(getenv func(string) string, key string, fallback bool) bool {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envDuration(getenv func(string) string, key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func applyRedisURL(cfg *Config, raw string) error {
	raw = strings.TrimSpace(raw)
	if !isRedisURL(raw) {
		return nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid redis url: %w", err)
	}
	if parsed.Host == "" {
		return fmt.Errorf("invalid redis url: host is required")
	}

	cfg.RedisAddr = parsed.Host
	if parsed.User != nil {
		if username := strings.TrimSpace(parsed.User.Username()); username != "" {
			cfg.RedisUsername = username
		}
		if password, ok := parsed.User.Password(); ok {
			cfg.RedisPassword = password
		}
	}

	dbSegment := strings.TrimSpace(strings.TrimPrefix(parsed.Path, "/"))
	if dbSegment != "" {
		db, err := strconv.Atoi(dbSegment)
		if err != nil {
			return fmt.Errorf("invalid redis db in url path %q", parsed.Path)
		}
		cfg.RedisDB = db
	}

	if strings.EqualFold(parsed.Scheme, "rediss") {
		cfg.RedisUseTLS = true
	}

	return nil
}

func isRedisURL(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "redis://") || strings.HasPrefix(value, "rediss://")
}

func loadDotEnv(paths ...string) (map[string]string, error) {
	values := make(map[string]string)
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}

		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}

			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key == "" {
				continue
			}

			value = strings.Trim(value, "\"")
			value = strings.Trim(value, "'")
			values[key] = value
		}
	}

	return values, nil
}
