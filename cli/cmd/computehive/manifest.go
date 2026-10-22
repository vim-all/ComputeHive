package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// manifest holds job defaults read from the job directory, so `computehive submit .` is enough.
// Precedence, lowest to highest: built-in defaults, Dockerfile `LABEL computehive.<key>=...` lines,
// computehive.yaml, then explicit command-line flags.
//
// computehive.yaml is deliberately tiny, flat `key: value` lines (no nesting, no anchors), so the
// CLI needs no YAML dependency and the file can be read at a glance:
//
//	cpu: 2
//	mem: 1024
//	gpu: 0
//	timeout: 600
//	min_disk: 5000
//	os: linux
//	arch: amd64
//	isolation: gvisor
//	verify: true
//	labels: fast-disk, rack-1
//	network_hosts: pypi.org
type manifest struct {
	set map[string]string // key -> raw value, only for keys that were actually present
}

var manifestKeys = map[string]bool{
	"cpu": true, "mem": true, "gpu": true, "timeout": true, "min_disk": true, "os": true, "arch": true,
	"isolation": true, "verify": true, "labels": true, "network_hosts": true, "max_attempts": true, "user": true,
}

const knownKeys = "cpu, mem, gpu, timeout, min_disk, os, arch, isolation, verify, labels, network_hosts, max_attempts, user"

// loadManifest reads Dockerfile labels and computehive.yaml from dir. Unknown keys are an error: a typo
// such as `mem_mb: 4096` silently ignored would run the job with the wrong resources.
func loadManifest(dir string) (*manifest, error) {
	m := &manifest{set: map[string]string{}}
	if f, err := os.Open(filepath.Join(dir, "Dockerfile")); err == nil {
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if len(line) < 6 || !strings.EqualFold(line[:6], "LABEL ") {
				continue
			}
			for _, kv := range strings.Fields(line[6:]) {
				k, v, ok := strings.Cut(kv, "=")
				if !ok || !strings.HasPrefix(k, "computehive.") {
					continue
				}
				k = strings.TrimPrefix(k, "computehive.")
				if !manifestKeys[k] {
					return nil, fmt.Errorf("dockerfile: unknown label computehive.%s (known: %s)", k, knownKeys)
				}
				m.set[k] = strings.Trim(v, `"'`)
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "computehive.yaml"))
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	for n, line := range strings.Split(string(raw), "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		k, v = strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), `"'`)
		if !ok || k == "" {
			return nil, fmt.Errorf("computehive.yaml line %d: want `key: value`, got %q", n+1, line)
		}
		if !manifestKeys[k] {
			return nil, fmt.Errorf("computehive.yaml line %d: unknown key %q (known: %s)", n+1, k, knownKeys)
		}
		m.set[k] = v
	}
	return m, nil
}

func (m *manifest) str(key, fallback string) string {
	if v, ok := m.set[key]; ok {
		return v
	}
	return fallback
}

func (m *manifest) integer(key string, fallback int) (int, error) {
	v, ok := m.set[key]
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("manifest %s: %q is not a non-negative integer", key, v)
	}
	return n, nil
}

func (m *manifest) boolean(key string, fallback bool) (bool, error) {
	v, ok := m.set[key]
	if !ok {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("manifest %s: %q is not true/false", key, v)
	}
	return b, nil
}

func (m *manifest) list(key string) []string {
	return strings.FieldsFunc(m.set[key], func(r rune) bool { return r == ',' || r == ' ' })
}
