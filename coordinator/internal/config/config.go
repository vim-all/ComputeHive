package config

import (
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
	GRPCPort      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisTLS      bool
}

func Load() (Config, error) {
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
		if redisHost == "" || redisPort == "" {
			return Config{}, fmt.Errorf("redis config required: set REDIS_URL, REDIS_ADDR, or REDIS_HOST+REDIS_PORT")
		}
		redisAddr = redisHost + ":" + redisPort
	}

	if strings.HasPrefix(redisAddr, "localhost:") || strings.HasPrefix(redisAddr, "127.0.0.1:") || strings.HasPrefix(redisAddr, "::1:") {
		return Config{}, fmt.Errorf("cloud redis address required: localhost is disabled")
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

	return Config{
		GRPCPort:      envOr(getenv, "GRPC_PORT", "50051"),
		RedisAddr:     redisAddr,
		RedisPassword: redisPassword,
		RedisDB:       redisDB,
		RedisTLS:      redisTLS,
	}, nil
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
