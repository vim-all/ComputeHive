package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadRejectsMalformedDuration(t *testing.T) {
	t.Setenv("AUTH_DISABLED", "true")
	t.Setenv("WORKER_DEAD_AFTER", "six seconds")
	if _, err := Load(nil); err == nil || !strings.Contains(err.Error(), "WORKER_DEAD_AFTER") {
		t.Fatalf("want an error naming WORKER_DEAD_AFTER, got %v", err)
	}
}

func TestLoadDefaultsAllowLocalRedis(t *testing.T) {
	for _, k := range []string{"REDIS_URL", "REDIS_ADDR", "REDIS_HOST", "REDIS_PORT", "GRPC_PORT", "HTTP_PORT"} {
		t.Setenv(k, "")
	}
	t.Setenv("AUTH_DISABLED", "true") // defaults are only startable in explicit dev mode
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	if cfg.RedisAddr != "127.0.0.1:6379" || cfg.GRPCPort != "50051" || cfg.HTTPPort != "8080" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadFlagsOverrideEnv(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_DISABLED", "true")
	cfg, err := Load([]string{"-redis-addr", "other:6380", "-http-port", "9000"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisAddr != "other:6380" || cfg.HTTPPort != "9000" {
		t.Fatalf("flags did not override env: %+v", cfg)
	}
}

func TestValidateErrors(t *testing.T) {
	base := Config{GRPCPort: "50051", HTTPPort: "8080", RedisAddr: "r:6379", ArtifactDir: "d", ArtifactMaxBytes: 1,
		HeartbeatInterval: 2 * time.Second, WorkerDeadAfter: 6 * time.Second, ReapInterval: time.Second,
		MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: 30 * time.Second,
		LimitRAMPct: 90, LimitCPUPct: 90, LimitDiskPct: 90, ReleasePct: 80, ReleaseAfter: 10 * time.Second,
		APIToken: "0123456789abcdef0123", TLSCertFile: "server.crt", TLSKeyFile: "server.key"}
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"bad grpc port", func(c *Config) { c.GRPCPort = "abc" }, "grpc-port"},
		{"port out of range", func(c *Config) { c.HTTPPort = "70000" }, "http-port"},
		{"ports collide", func(c *Config) { c.HTTPPort = "50051" }, "must differ"},
		{"no redis", func(c *Config) { c.RedisAddr = " " }, "redis-addr"},
		{"negative db", func(c *Config) { c.RedisDB = -1 }, "redis-db"},
		{"no artifact dir", func(c *Config) { c.ArtifactDir = "" }, "artifact-dir"},
		{"zero max bytes", func(c *Config) { c.ArtifactMaxBytes = 0 }, "artifact-max-bytes"},
		{"dead-after too tight for heartbeat", func(c *Config) { c.WorkerDeadAfter = 3 * time.Second }, "worker-dead-after"},
		{"zero heartbeat", func(c *Config) { c.HeartbeatInterval = 0 }, "heartbeat-interval"},
		{"zero reap interval", func(c *Config) { c.ReapInterval = 0 }, "reap-interval"},
		{"zero attempts", func(c *Config) { c.MaxAttempts = 0 }, "max-attempts"},
		{"ram limit over 100", func(c *Config) { c.LimitRAMPct = 101 }, "ram-limit-percent"},
		{"cpu limit zero", func(c *Config) { c.LimitCPUPct = 0 }, "cpu-limit-percent"},
		{"release equals limit (empty hysteresis band)", func(c *Config) { c.ReleasePct = 90 }, "release-percent"},
		{"release above a limit", func(c *Config) { c.LimitDiskPct = 75 }, "release-percent"},
		{"no api token and auth enabled", func(c *Config) { c.APIToken = "" }, "API token is required"},
		{"short api token", func(c *Config) { c.APIToken = "short" }, "too short"},
		{"auth without tls", func(c *Config) { c.TLSCertFile, c.TLSKeyFile = "", "" }, "plaintext"},
		{"cert without key", func(c *Config) { c.TLSKeyFile = "" }, "must be set together"},
		{"mtls without tls", func(c *Config) {
			c.TLSCertFile, c.TLSKeyFile, c.TLSClientCAFile = "", "", "ca.crt"
			c.AllowInsecure = true
		}, "mutual TLS requires TLS"},
		{"backoff inverted", func(c *Config) { c.BackoffMax = time.Millisecond }, "backoff-max"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("base config must validate: %v", err)
	}
}

func TestResourceLimitsAreConfigurableWithSaneDefaults(t *testing.T) {
	for _, k := range []string{"RAM_LIMIT_PERCENT", "CPU_LIMIT_PERCENT", "DISK_LIMIT_PERCENT", "RELEASE_PERCENT", "RELEASE_AFTER", "REQUIRE_USAGE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AUTH_DISABLED", "true")
	cfg, err := Load(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LimitRAMPct != 90 || cfg.LimitCPUPct != 90 || cfg.LimitDiskPct != 90 || cfg.ReleasePct != 80 || cfg.ReleaseAfter != 10*time.Second || !cfg.RequireUsage {
		t.Fatalf("defaults: %+v", cfg)
	}
	t.Setenv("RAM_LIMIT_PERCENT", "85")
	t.Setenv("RELEASE_PERCENT", "70")
	cfg, err = Load([]string{"-cpu-limit-percent", "95"})
	if err != nil || cfg.LimitRAMPct != 85 || cfg.LimitCPUPct != 95 || cfg.ReleasePct != 70 {
		t.Fatalf("overrides: %+v %v", cfg, err)
	}
	t.Setenv("RAM_LIMIT_PERCENT", "lots")
	if _, err := Load(nil); err == nil || !strings.Contains(err.Error(), "RAM_LIMIT_PERCENT") {
		t.Fatalf("malformed value must name the variable, got %v", err)
	}
}

func TestInsecureAndDisabledAuthAreExplicitDevOptIns(t *testing.T) {
	base := Config{GRPCPort: "50051", HTTPPort: "8080", RedisAddr: "r:6379", ArtifactDir: "d", ArtifactMaxBytes: 1,
		HeartbeatInterval: 2 * time.Second, WorkerDeadAfter: 6 * time.Second, ReapInterval: time.Second,
		MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: 30 * time.Second,
		LimitRAMPct: 90, LimitCPUPct: 90, LimitDiskPct: 90, ReleasePct: 80, ReleaseAfter: 10 * time.Second}

	c := base
	c.AuthDisabled = true
	if err := c.Validate(); err != nil {
		t.Fatalf("explicit dev mode must be startable: %v", err)
	}
	c = base
	c.APIToken, c.AllowInsecure = "0123456789abcdef0123", true
	if err := c.Validate(); err != nil {
		t.Fatalf("token + explicit insecure opt-in: %v", err)
	}
	if err := base.Validate(); err == nil {
		t.Fatal("a default config must refuse to start: secure by default")
	}
}

func TestAPITokenCanComeFromAFileSoItStaysOutOfTheProcessList(t *testing.T) {
	file := t.TempDir() + "/token"
	if err := os.WriteFile(file, []byte("  file-based-token-0123456789  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("API_TOKEN", "")
	t.Setenv("API_TOKEN_FILE", file)
	t.Setenv("ALLOW_INSECURE", "true")
	cfg, err := Load(nil)
	if err != nil || cfg.APIToken != "file-based-token-0123456789" {
		t.Fatalf("token %q err %v", cfg.APIToken, err)
	}
	t.Setenv("API_TOKEN_FILE", file+".missing")
	if _, err := Load(nil); err == nil || !strings.Contains(err.Error(), "API_TOKEN_FILE") {
		t.Fatalf("a missing token file must be a clear startup error, got %v", err)
	}
}
