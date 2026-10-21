package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultsAllowLocalRedis(t *testing.T) {
	for _, k := range []string{"REDIS_URL", "REDIS_ADDR", "REDIS_HOST", "REDIS_PORT", "GRPC_PORT", "HTTP_PORT"} {
		t.Setenv(k, "")
	}
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
	cfg, err := Load([]string{"-redis-addr", "other:6380", "-http-port", "9000"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisAddr != "other:6380" || cfg.HTTPPort != "9000" {
		t.Fatalf("flags did not override env: %+v", cfg)
	}
}

func TestValidateErrors(t *testing.T) {
	base := Config{GRPCPort: "50051", HTTPPort: "8080", RedisAddr: "r:6379", ArtifactDir: "d", ArtifactMaxBytes: 1}
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
