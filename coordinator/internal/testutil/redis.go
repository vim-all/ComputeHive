// Package testutil provides a real Redis for integration tests. Tests that need
// Redis skip (rather than fail) when neither Docker nor COMPUTEHIVE_TEST_REDIS is available.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a disposable Redis server.
type Redis struct {
	Addr string
	name string // docker container name; empty when using COMPUTEHIVE_TEST_REDIS
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func docker(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// Start returns a fresh, empty Redis. With COMPUTEHIVE_TEST_REDIS=host:port it uses
// that server (and FLUSHALLs it); otherwise it starts a container with AOF
// (appendfsync always, so a kill loses no acknowledged write).
func Start(t testing.TB) *Redis {
	t.Helper()
	if addr := os.Getenv("COMPUTEHIVE_TEST_REDIS"); addr != "" {
		r := &Redis{Addr: addr}
		c := r.Client()
		defer c.Close()
		if err := c.FlushAll(context.Background()).Err(); err != nil {
			t.Fatalf("flush COMPUTEHIVE_TEST_REDIS %s: %v", addr, err)
		}
		return r
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and COMPUTEHIVE_TEST_REDIS not set")
	}
	if _, err := docker("info", "--format", "{{.ServerVersion}}"); err != nil {
		t.Skip("docker daemon not reachable and COMPUTEHIVE_TEST_REDIS not set")
	}
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	name := "ch-test-redis-" + hex.EncodeToString(rnd[:])
	if out, err := docker("run", "-d", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:6379", port),
		"redis:7-alpine", "redis-server", "--appendonly", "yes", "--appendfsync", "always", "--save", ""); err != nil {
		t.Fatalf("start redis container: %v: %s", err, out)
	}
	t.Cleanup(func() { _, _ = docker("rm", "-f", "-v", name) })
	r := &Redis{Addr: fmt.Sprintf("127.0.0.1:%d", port), name: name}
	r.waitReady(t)
	return r
}

func (r *Redis) waitReady(t testing.TB) {
	t.Helper()
	c := r.Client()
	defer c.Close()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := c.Ping(ctx).Err()
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("redis %s never became ready: %v", r.Addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Client returns a new client; the caller should Close it (t.Cleanup is fine).
func (r *Redis) Client() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: r.Addr, MaxRetries: 10, MinRetryBackoff: 50 * time.Millisecond, MaxRetryBackoff: time.Second})
}

// KillAndRestart SIGKILLs the server (a crash, not a clean shutdown) and starts it
// again on the same port with the same data directory, then waits until it answers.
func (r *Redis) KillAndRestart(t testing.TB) {
	t.Helper()
	if r.name == "" {
		t.Skip("restart tests need the docker-managed Redis")
	}
	if out, err := docker("kill", r.name); err != nil {
		t.Fatalf("kill redis: %v: %s", err, out)
	}
	if out, err := docker("start", r.name); err != nil {
		t.Fatalf("restart redis: %v: %s", err, out)
	}
	r.waitReady(t)
}
