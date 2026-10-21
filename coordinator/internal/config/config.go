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
