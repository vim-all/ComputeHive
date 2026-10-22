package netproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
)

const image = "alpine:3.21"

func needDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
			t.Skipf("cannot get %s: %v: %s", image, err, out)
		}
	}
}

// buildWorker compiles the worker as a static binary: the proxy sidecar's image is empty.
func buildWorker(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", out, "../../cmd/worker")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, b)
	}
	return out
}

func hostHasInternet() bool {
	c, err := net.DialTimeout("tcp", "example.com:80", 4*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// Network allowlist mode, end to end on real Docker: the job gets an internal network whose only
// peer is the filtering proxy. It runs on the host (not inside a container), like a contributor's worker.
func TestNetworkAllowlistModeOnRealDocker(t *testing.T) {
	needDocker(t)
	pol := policy.Default()
	pol.AllowNetwork, pol.AllowedHosts = true, []string{"example.com"}

	sb := executor.DefaultSandbox()
	sb.Images = executor.ImagePolicy{Allow: []string{image}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := executor.NewDockerExecutor("docker", "nettest", false, "/computehive/output", executor.DefaultLimits(), sb)
	prov := NewProvider("docker", sb.DockerConfigDir, "nettest", buildWorker(t), func() policy.Policy { return pol }, log)
	e.WithNetwork(prov)

	// A host service on a wildcard address: the documented residual risk of this mode.
	canary, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer canary.Close()
	go func() {
		for {
			c, err := canary.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	canaryPort := canary.Addr().(*net.TCPAddr).Port

	script := fmt.Sprintf(`
try() { n=$1; shift; if timeout 12 "$@" >/dev/null 2>&1; then echo "REACHED:$n"; else echo "BLOCKED:$n"; fi; }
P=${HTTP_PROXY#http://}; H=${P%%:*}; PT=${P##*:}
echo "proxy=$HTTP_PROXY"
try default-route    sh -c 'ip route | grep -q default'
try direct-internet  nc -w 3 -z 1.1.1.1 443
try direct-dns       nslookup example.com 8.8.8.8
try metadata-http    wget -T 5 -q -O- http://169.254.169.254/latest/meta-data/
try metadata-connect sh -c "(printf 'CONNECT 169.254.169.254:80 HTTP/1.1\r\nHost: x\r\n\r\n'; sleep 2) | nc -w 3 $H $PT | head -1 | grep -q ' 200 '"
try private-connect  sh -c "(printf 'CONNECT 10.0.0.1:443 HTTP/1.1\r\nHost: x\r\n\r\n'; sleep 2) | nc -w 3 $H $PT | head -1 | grep -q ' 200 '"
try loopback-connect sh -c "(printf 'CONNECT 127.0.0.1:%[1]d HTTP/1.1\r\nHost: x\r\n\r\n'; sleep 2) | nc -w 3 $H $PT | head -1 | grep -q ' 200 '"
try unlisted-host    wget -T 8 -q -O- http://example.org/
try ssh-port         sh -c "(printf 'CONNECT example.com:22 HTTP/1.1\r\nHost: x\r\n\r\n'; sleep 2) | nc -w 3 $H $PT | head -1 | grep -q ' 200 '"
try allowed-host     sh -c 'wget -T 10 -q -O- http://example.com/ | grep -q "Example Domain"'
try tunnel-allowed   sh -c "(printf 'CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n'; sleep 2) | nc -w 4 $H $PT | head -1 | grep -q ' 200 '"
try host-service     nc -w 3 -z 172.17.0.1 %[1]d
try host-gateway     nc -w 3 -z ${H%%.*}.${H#*.} %[1]d
`, canaryPort)

	job := domain.Job{ID: "net-1", ImageRef: image, NetworkHosts: []string{"example.com"}, Command: []string{"sh", "-c", script}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res := e.Run(ctx, job, artifact.Bundle{})
	if !strings.Contains(res.Stdout, "BLOCKED") {
		t.Fatalf("job did not run: %+v", res)
	}
	t.Logf("job output:\n%s", res.Stdout)

	outcome := map[string]string{}
	for _, l := range strings.Split(res.Stdout, "\n") {
		if k, v, ok := strings.Cut(l, ":"); ok && (k == "BLOCKED" || k == "REACHED") {
			outcome[v] = k
		}
	}
	for _, n := range []string{"direct-internet", "direct-dns", "metadata-http", "metadata-connect", "private-connect", "loopback-connect", "unlisted-host", "ssh-port", "default-route", "host-service", "host-gateway"} {
		if outcome[n] != "BLOCKED" {
			t.Errorf("%s: %q, want BLOCKED", n, outcome[n])
		}
	}
	if !strings.Contains(res.Stdout, "proxy=http://") {
		t.Errorf("job was not given a proxy:\n%s", res.Stdout)
	}

	if hostHasInternet() {
		for _, n := range []string{"allowed-host", "tunnel-allowed"} {
			if outcome[n] != "REACHED" {
				t.Errorf("%s: %q, want REACHED (the allowed host must work)", n, outcome[n])
			}
		}
	} else {
		t.Log("no internet from this host: skipping the positive checks")
	}

	// The job cannot reach the host's wildcard-bound services: its only peer is the sidecar.
	t.Logf("host service on a wildcard address, as seen from the job: %s", outcome["host-service"])

	if out, _ := exec.Command("docker", "network", "ls", "--filter", "name=chjob-", "-q").Output(); strings.TrimSpace(string(out)) != "" {
		t.Errorf("job network leaked: %s", out)
	}
}

// Without the contributor's permission, a job asking for the network is refused before anything is created.
func TestNetworkJobIsRefusedWhenPolicyForbidsIt(t *testing.T) {
	needDocker(t)
	sb := executor.DefaultSandbox()
	sb.Images = executor.ImagePolicy{Allow: []string{image}}
	pol := policy.Default() // allow_network = false
	e := executor.NewDockerExecutor("docker", "nettest", false, "/computehive/output", executor.DefaultLimits(), sb)
	e.WithNetwork(NewProvider("docker", sb.DockerConfigDir, "nettest", buildWorker(t), func() policy.Policy { return pol }, slog.New(slog.NewTextHandler(io.Discard, nil))))
	res := e.Run(context.Background(), domain.Job{ID: "net-2", ImageRef: image, NetworkHosts: []string{"example.com"}, Command: []string{"true"}}, artifact.Bundle{})
	if res.Status == domain.JobStatusSucceeded || !strings.Contains(res.Error, "not permitted") {
		t.Fatalf("network job ran without permission: %+v", res)
	}
	if out, _ := exec.Command("docker", "network", "ls", "--filter", "name=chjob-", "-q").Output(); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("a network was created for a refused job: %s", out)
	}
}
