package config

import (
	"fmt"
	"net/url"
	"os"
	"path"
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
	redisAddr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	redisPassword := strings.TrimSpace(os.Getenv("REDIS_PASSWORD"))
	redisDB := 0
	redisTLS := false
	urlProvided := false

	if rawURL := strings.TrimSpace(os.Getenv("REDIS_URL")); rawURL != "" {
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
		if strings.TrimSpace(os.Getenv("REDIS_DB")) == "" {
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
		redisHost := strings.TrimSpace(os.Getenv("REDIS_HOST"))
		redisPort := strings.TrimSpace(os.Getenv("REDIS_PORT"))
		if redisHost == "" || redisPort == "" {
			return Config{}, fmt.Errorf("redis config required: set REDIS_URL, REDIS_ADDR, or REDIS_HOST+REDIS_PORT")
		}
		redisAddr = redisHost + ":" + redisPort
	}

	if strings.HasPrefix(redisAddr, "localhost:") || strings.HasPrefix(redisAddr, "127.0.0.1:") || strings.HasPrefix(redisAddr, "::1:") {
		return Config{}, fmt.Errorf("cloud redis address required: localhost is disabled")
	}

	if rawDB := strings.TrimSpace(os.Getenv("REDIS_DB")); rawDB != "" {
		parsedDB, err := strconv.Atoi(rawDB)
		if err != nil {
			return Config{}, fmt.Errorf("invalid REDIS_DB: %w", err)
		}
		redisDB = parsedDB
	}

	if rawTLS := strings.TrimSpace(os.Getenv("REDIS_TLS")); rawTLS != "" {
		if !urlProvided {
			rawTLS = strings.ToLower(rawTLS)
			redisTLS = rawTLS == "1" || rawTLS == "true" || rawTLS == "yes"
		}
	}

	return Config{
		GRPCPort:      getEnv("GRPC_PORT", "50051"),
		RedisAddr:     redisAddr,
		RedisPassword: redisPassword,
		RedisDB:       redisDB,
		RedisTLS:      redisTLS,
	}, nil
}

func (c Config) GRPCListenAddr() string {
	if strings.HasPrefix(c.GRPCPort, ":") {
		return c.GRPCPort
	}
	return ":" + c.GRPCPort
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
