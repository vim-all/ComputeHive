package config

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for the coordinator process.
type Config struct {
	GRPCPort         string
	HTTPPort         string
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	RedisTLS         bool
	ArtifactDir      string
	ArtifactMaxBytes int64

	// Failure detection and retries (see internal/queue).
	HeartbeatInterval time.Duration // advertised to workers
	WorkerDeadAfter   time.Duration
	ReapInterval      time.Duration
	LeaseGrace        time.Duration
	MaxAttempts       int
	BackoffBase       time.Duration
	BackoffMax        time.Duration

	// Resource-safety rules the scheduler enforces regardless of what workers claim.
	LimitRAMPct  float64
	LimitCPUPct  float64
	LimitDiskPct float64
	ReleasePct   float64
	ReleaseAfter time.Duration
	RequireUsage bool
}

// Load reads configuration from flags (args), then environment, then .env files, then defaults.
func Load(args []string) (Config, error) {
	dotEnv, _ := loadDotEnv(".env", "coordinator/.env")
	getenv := func(key string) string {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
		return strings.TrimSpace(dotEnv[key])
	}

	redisAddr := strings.TrimSpace(getenv("REDIS_ADDR"))
	redisPassword := strings.TrimSpace(getenv("REDIS_PASSWORD"))
	redisDB := 0
	redisTLS := false
	urlProvided := false

	if rawURL := strings.TrimSpace(getenv("REDIS_URL")); rawURL != "" {
		urlProvided = true
		u, err := url.Parse(rawURL)
		if err != nil {
			return Config{}, fmt.Errorf("invalid REDIS_URL: %w", err)
		}
		if u.Host != "" {
			redisAddr = u.Host
		}
		if redisPassword == "" {
			if p, ok := u.User.Password(); ok {
				redisPassword = p
			}
		}
		if strings.EqualFold(u.Scheme, "rediss") {
			redisTLS = true
		}
		if strings.TrimSpace(getenv("REDIS_DB")) == "" {
			dbPart := strings.Trim(path.Clean(u.Path), "/")
			if dbPart != "" && dbPart != "." {
				parsedDB, parseErr := strconv.Atoi(dbPart)
				if parseErr != nil {
					return Config{}, fmt.Errorf("invalid REDIS_URL db path %q: %w", dbPart, parseErr)
				}
				redisDB = parsedDB
			}
		}
	}

	if redisAddr == "" {
		redisHost := strings.TrimSpace(getenv("REDIS_HOST"))
		redisPort := strings.TrimSpace(getenv("REDIS_PORT"))
		switch {
		case redisHost != "" && redisPort != "":
			redisAddr = redisHost + ":" + redisPort
		case redisHost == "" && redisPort == "":
			redisAddr = "127.0.0.1:6379"
		default:
			return Config{}, fmt.Errorf("redis config incomplete: REDIS_HOST and REDIS_PORT must be set together (or use REDIS_URL / REDIS_ADDR)")
		}
	}

	if rawDB := strings.TrimSpace(getenv("REDIS_DB")); rawDB != "" {
		parsedDB, err := strconv.Atoi(rawDB)
		if err != nil {
			return Config{}, fmt.Errorf("invalid REDIS_DB: %w", err)
		}
		redisDB = parsedDB
	}

	if rawTLS := strings.TrimSpace(getenv("REDIS_TLS")); rawTLS != "" {
		if !urlProvided {
			rawTLS = strings.ToLower(rawTLS)
			redisTLS = rawTLS == "1" || rawTLS == "true" || rawTLS == "yes"
		}
	}

	artifactMax := int64(1 << 30)
	if raw := getenv("ARTIFACT_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid ARTIFACT_MAX_BYTES %q: %w", raw, err)
		}
		artifactMax = parsed
	}

	cfg := Config{
		GRPCPort:         envOr(getenv, "GRPC_PORT", "50051"),
		HTTPPort:         envOr(getenv, "HTTP_PORT", "8080"),
		RedisAddr:        redisAddr,
		RedisPassword:    redisPassword,
		RedisDB:          redisDB,
		RedisTLS:         redisTLS,
		ArtifactDir:      envOr(getenv, "ARTIFACT_DIR", "./data/artifacts"),
		ArtifactMaxBytes: artifactMax,

		HeartbeatInterval: envDuration(getenv, "HEARTBEAT_INTERVAL", 2*time.Second),
		WorkerDeadAfter:   envDuration(getenv, "WORKER_DEAD_AFTER", 6*time.Second),
		ReapInterval:      envDuration(getenv, "REAP_INTERVAL", time.Second),
		LeaseGrace:        envDuration(getenv, "LEASE_GRACE", 30*time.Second),
		MaxAttempts:       envInt(getenv, "MAX_ATTEMPTS", 3),
		BackoffBase:       envDuration(getenv, "BACKOFF_BASE", time.Second),
		BackoffMax:        envDuration(getenv, "BACKOFF_MAX", 30*time.Second),

		LimitRAMPct:  envFloat(getenv, "RAM_LIMIT_PERCENT", 90),
		LimitCPUPct:  envFloat(getenv, "CPU_LIMIT_PERCENT", 90),
		LimitDiskPct: envFloat(getenv, "DISK_LIMIT_PERCENT", 90),
		ReleasePct:   envFloat(getenv, "RELEASE_PERCENT", 80),
		ReleaseAfter: envDuration(getenv, "RELEASE_AFTER", 10*time.Second),
		RequireUsage: envBool(getenv, "REQUIRE_USAGE", true),
	}
	if err := durationErr(getenv, "HEARTBEAT_INTERVAL", "WORKER_DEAD_AFTER", "REAP_INTERVAL", "LEASE_GRACE", "BACKOFF_BASE", "BACKOFF_MAX", "RELEASE_AFTER"); err != nil {
		return Config{}, err
	}
	if err := intErr(getenv, "MAX_ATTEMPTS"); err != nil {
		return Config{}, err
	}
	if err := floatErr(getenv, "RAM_LIMIT_PERCENT", "CPU_LIMIT_PERCENT", "DISK_LIMIT_PERCENT", "RELEASE_PERCENT"); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	fs.StringVar(&cfg.GRPCPort, "grpc-port", cfg.GRPCPort, "gRPC listen port")
	fs.StringVar(&cfg.HTTPPort, "http-port", cfg.HTTPPort, "HTTP listen port (artifacts, health)")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", cfg.RedisAddr, "Redis host:port")
	fs.StringVar(&cfg.RedisPassword, "redis-password", cfg.RedisPassword, "Redis password")
	fs.IntVar(&cfg.RedisDB, "redis-db", cfg.RedisDB, "Redis database number")
	fs.BoolVar(&cfg.RedisTLS, "redis-tls", cfg.RedisTLS, "Use TLS to connect to Redis")
	fs.StringVar(&cfg.ArtifactDir, "artifact-dir", cfg.ArtifactDir, "Directory for uploaded job artifacts")
	fs.Int64Var(&cfg.ArtifactMaxBytes, "artifact-max-bytes", cfg.ArtifactMaxBytes, "Maximum artifact upload size in bytes")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", cfg.HeartbeatInterval, "heartbeat interval advertised to workers")
	fs.DurationVar(&cfg.WorkerDeadAfter, "worker-dead-after", cfg.WorkerDeadAfter, "heartbeat silence after which a worker is dead and its jobs are requeued")
	fs.DurationVar(&cfg.ReapInterval, "reap-interval", cfg.ReapInterval, "how often failure detection runs")
	fs.DurationVar(&cfg.LeaseGrace, "lease-grace", cfg.LeaseGrace, "extra time past a job's runtime limit before its lease expires")
	fs.IntVar(&cfg.MaxAttempts, "max-attempts", cfg.MaxAttempts, "default attempts per job before it is parked as DEAD")
	fs.DurationVar(&cfg.BackoffBase, "backoff-base", cfg.BackoffBase, "first retry delay (doubles per attempt)")
	fs.DurationVar(&cfg.BackoffMax, "backoff-max", cfg.BackoffMax, "maximum retry delay")
	fs.Float64Var(&cfg.LimitRAMPct, "ram-limit-percent", cfg.LimitRAMPct, "no jobs for a worker whose RAM use is at or above this percentage")
	fs.Float64Var(&cfg.LimitCPUPct, "cpu-limit-percent", cfg.LimitCPUPct, "... CPU use")
	fs.Float64Var(&cfg.LimitDiskPct, "disk-limit-percent", cfg.LimitDiskPct, "... disk use")
	fs.Float64Var(&cfg.ReleasePct, "release-percent", cfg.ReleasePct, "an overloaded worker recovers only after every metric stays below this")
	fs.DurationVar(&cfg.ReleaseAfter, "release-after", cfg.ReleaseAfter, "... for this long")
	fs.BoolVar(&cfg.RequireUsage, "require-usage", cfg.RequireUsage, "refuse to assign jobs to workers that report no resource metrics")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports the first invalid setting with a message naming the flag and env var to fix.
func (c Config) Validate() error {
	for _, p := range []struct{ name, env, value string }{
		{"grpc-port", "GRPC_PORT", c.GRPCPort},
		{"http-port", "HTTP_PORT", c.HTTPPort},
	} {
		n, err := strconv.Atoi(strings.TrimPrefix(p.value, ":"))
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("invalid %s %q (env %s): must be a port number 1-65535", p.name, p.value, p.env)
		}
	}
	if c.GRPCPort == c.HTTPPort {
		return fmt.Errorf("grpc-port and http-port must differ (both %s)", c.GRPCPort)
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		return fmt.Errorf("redis-addr is required (env REDIS_ADDR)")
	}
	if c.RedisDB < 0 {
		return fmt.Errorf("invalid redis-db %d (env REDIS_DB): must be >= 0", c.RedisDB)
	}
	if strings.TrimSpace(c.ArtifactDir) == "" {
		return fmt.Errorf("artifact-dir is required (env ARTIFACT_DIR)")
	}
	if c.ArtifactMaxBytes <= 0 {
		return fmt.Errorf("invalid artifact-max-bytes %d (env ARTIFACT_MAX_BYTES): must be > 0", c.ArtifactMaxBytes)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("invalid heartbeat-interval %s (env HEARTBEAT_INTERVAL): must be > 0", c.HeartbeatInterval)
	}
	// A worker must be able to miss a couple of heartbeats without being declared dead.
	if c.WorkerDeadAfter < 2*c.HeartbeatInterval {
		return fmt.Errorf("worker-dead-after (%s, env WORKER_DEAD_AFTER) must be at least 2x heartbeat-interval (%s) or healthy workers will be declared dead",
			c.WorkerDeadAfter, c.HeartbeatInterval)
	}
	if c.ReapInterval <= 0 {
		return fmt.Errorf("invalid reap-interval %s (env REAP_INTERVAL): must be > 0", c.ReapInterval)
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("invalid max-attempts %d (env MAX_ATTEMPTS): must be >= 1", c.MaxAttempts)
	}
	for _, l := range []struct {
		name string
		v    float64
	}{{"ram-limit-percent", c.LimitRAMPct}, {"cpu-limit-percent", c.LimitCPUPct}, {"disk-limit-percent", c.LimitDiskPct}} {
		if l.v <= 0 || l.v > 100 {
			return fmt.Errorf("invalid %s %.1f: must be in (0, 100]", l.name, l.v)
		}
	}
	if c.ReleasePct <= 0 || c.ReleasePct >= min(c.LimitRAMPct, c.LimitCPUPct, c.LimitDiskPct) {
		return fmt.Errorf("release-percent (%.1f) must be positive and below every limit (ram %.1f, cpu %.1f, disk %.1f), or the hysteresis band is empty and workers would flap",
			c.ReleasePct, c.LimitRAMPct, c.LimitCPUPct, c.LimitDiskPct)
	}
	if c.ReleaseAfter < 0 {
		return fmt.Errorf("invalid release-after %s: must be >= 0", c.ReleaseAfter)
	}
	if c.BackoffBase < 0 || c.BackoffMax < c.BackoffBase {
		return fmt.Errorf("backoff-max (%s) must be >= backoff-base (%s) >= 0", c.BackoffMax, c.BackoffBase)
	}
	return nil
}

// RequeueBound is the worst-case time from a worker's last heartbeat to its jobs being requeued.
func (c Config) RequeueBound() time.Duration { return c.WorkerDeadAfter + c.ReapInterval }

func envDuration(getenv func(string) string, key string, fallback time.Duration) time.Duration {
	if v := getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func envFloat(getenv func(string) string, key string, fallback float64) float64 {
	if v := getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envBool(getenv func(string) string, key string, fallback bool) bool {
	switch strings.ToLower(getenv(key)) {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	}
	return fallback
}

func floatErr(getenv func(string) string, keys ...string) error {
	for _, k := range keys {
		if v := getenv(k); v != "" {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return fmt.Errorf("invalid %s %q: want a number like 90", k, v)
			}
		}
	}
	return nil
}

func envInt(getenv func(string) string, key string, fallback int) int {
	if v := getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// durationErr / intErr turn silently-ignored malformed env values into startup errors.
func durationErr(getenv func(string) string, keys ...string) error {
	for _, k := range keys {
		if v := getenv(k); v != "" {
			if _, err := time.ParseDuration(v); err != nil {
				return fmt.Errorf("invalid %s %q: want a duration like 2s or 500ms", k, v)
			}
		}
	}
	return nil
}

func intErr(getenv func(string) string, keys ...string) error {
	for _, k := range keys {
		if v := getenv(k); v != "" {
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("invalid %s %q: want an integer", k, v)
			}
		}
	}
	return nil
}

// HTTPListenAddr returns the HTTP listen address in net.Listen form.
func (c Config) HTTPListenAddr() string {
	if strings.HasPrefix(c.HTTPPort, ":") {
		return c.HTTPPort
	}
	return ":" + c.HTTPPort
}

func envOr(getenv func(string) string, key, fallback string) string {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func loadDotEnv(paths ...string) (map[string]string, error) {
	values := make(map[string]string)
	for _, file := range paths {
		if strings.TrimSpace(file) == "" {
			continue
		}

		data, err := os.ReadFile(filepath.Clean(file))
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

func (c Config) GRPCListenAddr() string {
	if strings.HasPrefix(c.GRPCPort, ":") {
		return c.GRPCPort
	}
	return ":" + c.GRPCPort
}
