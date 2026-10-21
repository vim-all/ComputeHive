package config

import (
	"testing"
	"time"
)

func TestParseUsesEnvironmentDefaults(t *testing.T) {
	getenv := func(key string) string {
		values := map[string]string{
			"WORKER_ID":                 "worker-test-01",
			"REDIS_ADDR":                "redis.internal:6379",
			"HEARTBEAT_INTERVAL":        "9s",
			"ALLOW_GPU_JOBS":            "false",
			"ARTIFACT_DOWNLOAD_TIMEOUT": "3m",
			"ARTIFACT_MAX_BYTES":        "2048",
		}
		return values[key]
	}

	cfg, err := parse(nil, getenv)
	if err != nil {
		t.Fatalf("parse returned error: %v", err)
	}

	if cfg.WorkerID != "worker-test-01" {
		t.Fatalf("expected worker id from env, got %q", cfg.WorkerID)
	}
	if cfg.RedisAddr != "redis.internal:6379" {
		t.Fatalf("expected redis addr from env, got %q", cfg.RedisAddr)
	}
	if cfg.HeartbeatInterval != 9*time.Second {
		t.Fatalf("expected heartbeat interval 9s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.AllowGPUJobs {
		t.Fatalf("expected allow-gpu-jobs to be false")
	}
	if cfg.ArtifactDownloadTimeout != 3*time.Minute {
		t.Fatalf("expected artifact download timeout 3m, got %v", cfg.ArtifactDownloadTimeout)
	}
	if cfg.ArtifactMaxBytes != 2048 {
		t.Fatalf("expected artifact max bytes 2048, got %d", cfg.ArtifactMaxBytes)
	}
}

func TestParseAllowsFlagOverrides(t *testing.T) {
	getenv := func(key string) string {
		values := map[string]string{
			"WORKER_ID":  "worker-env",
			"REDIS_ADDR": "redis-env:6379",
		}
		return values[key]
	}

	cfg, err := parse([]string{"-worker-id", "worker-flag", "-redis-addr", "redis-flag:6379"}, getenv)
	if err != nil {
		t.Fatalf("parse returned error: %v", err)
	}

	if cfg.WorkerID != "worker-flag" {
		t.Fatalf("expected worker id to come from flags, got %q", cfg.WorkerID)
	}
	if cfg.RedisAddr != "redis-flag:6379" {
		t.Fatalf("expected redis addr to come from flags, got %q", cfg.RedisAddr)
	}
}

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestResourceSafetyDefaultsMatchTheSpec(t *testing.T) {
	cfg, err := parse(nil, envOf(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RAMLimitPercent != 90 || cfg.CPULimitPercent != 90 || cfg.DiskLimitPercent != 90 || cfg.ReleasePercent != 80 {
		t.Fatalf("limits %+v: want enter at 90%% and leave below 80%%", cfg)
	}
	if cfg.HeartbeatInterval != 2*time.Second || cfg.ReleaseAfter != 10*time.Second || cfg.SampleInterval != time.Second {
		t.Fatalf("timings %v %v %v", cfg.HeartbeatInterval, cfg.ReleaseAfter, cfg.SampleInterval)
	}
	if cfg.DefaultJobMemoryMB != 512 || cfg.DefaultJobCPUs != 1 || cfg.PidsLimit != 256 {
		t.Fatalf("container caps: %d MB, %v cpus, %d pids", cfg.DefaultJobMemoryMB, cfg.DefaultJobCPUs, cfg.PidsLimit)
	}
}

func TestResourceSafetyIsConfigurableAndValidated(t *testing.T) {
	ok, err := parse([]string{"-ram-limit-percent", "85", "-release-percent", "70"}, envOf(map[string]string{"CPU_LIMIT_PERCENT": "95"}))
	if err != nil || ok.RAMLimitPercent != 85 || ok.CPULimitPercent != 95 || ok.ReleasePercent != 70 {
		t.Fatalf("overrides: %+v %v", ok, err)
	}
	for name, env := range map[string]map[string]string{
		"release equals limit (no hysteresis band)": {"RELEASE_PERCENT": "90"},
		"limit above 100":       {"RAM_LIMIT_PERCENT": "120"},
		"release above a limit": {"DISK_LIMIT_PERCENT": "75"},
		"zero memory cap":       {"DEFAULT_JOB_MEMORY_MB": "0"},
		"zero pids cap":         {"JOB_PIDS_LIMIT": "0"},
	} {
		if _, err := parse(nil, envOf(env)); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}
