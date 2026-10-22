package netproxy

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
)

const (
	emptyImage = "computehive-empty:1"
	proxyPort  = "3128"
)

// Provider gives a job network access ONLY when it asked for specific hosts and the contributor's
// policy permits every one of them.
//
// Design: Docker `--internal` networks have no route anywhere, and (verified on real Docker, see
// the integration test) the job cannot even reach the host's gateway address. The job is attached
// to such a network together with one peer: a locked-down sidecar container running this worker's
// own binary as `worker proxy-serve`. The sidecar also sits on the default bridge, so it alone has a
// way out, and it only makes connections the filter allows (listed hosts, public addresses, ports
// 80/443). The job therefore can reach neither the host, nor the LAN, nor anything but that proxy.
type Provider struct {
	docker    string
	configDir string
	workerID  string
	selfBin   string
	policy    func() policy.Policy
	log       *slog.Logger

	mu   sync.Mutex
	nets map[string]bool
}

// NewProvider returns a Provider. selfBinary is this worker's executable: it must be a static
// binary (the sidecar's image is empty), which the Dockerfile and `make build` produce.
func NewProvider(dockerBinary, configDir, workerID, selfBinary string, policyFn func() policy.Policy, log *slog.Logger) *Provider {
	if log == nil {
		log = slog.Default()
	}
	return &Provider{docker: dockerBinary, configDir: configDir, workerID: workerID, selfBin: selfBinary,
		policy: policyFn, log: log, nets: map[string]bool{}}
}

var _ executor.NetworkProvider = (*Provider)(nil)

func (p *Provider) Owns(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nets[name]
}

func (p *Provider) dockerOut(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, p.docker, args...)
	cmd.Env = os.Environ()
	if p.configDir != "" {
		cmd.Env = append(cmd.Env, "DOCKER_CONFIG="+p.configDir)
	}
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func jobSlug(jobID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(jobID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

// ensureEmptyImage creates a local image with no layers: the sidecar needs a root filesystem but
// nothing in it except the mounted proxy binary, so there is no shell and no package to attack.
func (p *Provider) ensureEmptyImage(ctx context.Context) error {
	if _, err := p.dockerOut(ctx, "image", "inspect", emptyImage); err == nil {
		return nil
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("tar -cT /dev/null | %s import - %s", p.docker, emptyImage))
	cmd.Env = os.Environ()
	if p.configDir != "" {
		cmd.Env = append(cmd.Env, "DOCKER_CONFIG="+p.configDir)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("create the empty proxy image: %v: %s", err, out)
	}
	return nil
}

// SweepNetworks removes job networks and proxy sidecars left by a crashed worker. It returns how many networks it removed.
func (p *Provider) SweepNetworks(ctx context.Context) int {
	out, err := p.dockerOut(ctx, "ps", "-a", "-q", "--filter", "label=computehive.proxy=1", "--filter", "label=computehive.worker.id="+p.workerID)
	if err == nil && out != "" {
		_, _ = p.dockerOut(ctx, append([]string{"rm", "-f", "-v"}, strings.Fields(out)...)...)
	}
	ids, err := p.dockerOut(ctx, "network", "ls", "-q", "--filter", "label=computehive.job.id")
	if err != nil || ids == "" {
		return 0
	}
	n := 0
	for _, id := range strings.Fields(ids) {
		if _, err := p.dockerOut(ctx, "network", "rm", id); err == nil {
			n++
		}
	}
	return n
}

// Attach implements executor.NetworkProvider.
func (p *Provider) Attach(ctx context.Context, job domain.Job) (executor.NetworkAttachment, error) {
	if len(job.NetworkHosts) == 0 {
		return executor.NetworkAttachment{Name: "none"}, nil
	}
	pol := p.policy()
	for _, h := range job.NetworkHosts {
		if !pol.HostAllowed(h) {
			return executor.NetworkAttachment{}, fmt.Errorf("network access to %q is not permitted by this worker's policy (allow_network=%v)", h, pol.AllowNetwork)
		}
	}
	if p.selfBin == "" {
		return executor.NetworkAttachment{}, fmt.Errorf("cannot locate the worker binary to run the egress proxy")
	}
	if err := p.ensureEmptyImage(ctx); err != nil {
		return executor.NetworkAttachment{}, err
	}

	slug := jobSlug(job.ID)
	netname, sidecar := "chjob-"+slug, "chproxy-"+slug
	cleanup := func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = p.dockerOut(c, "rm", "-f", "-v", sidecar)
		_, _ = p.dockerOut(c, "network", "rm", netname)
		p.mu.Lock()
		delete(p.nets, netname)
		p.mu.Unlock()
	}
	cleanup() // a leftover from an earlier attempt of the same job

	if out, err := p.dockerOut(ctx, "network", "create", "--internal", "--driver", "bridge", "--label", "computehive.job.id="+job.ID, netname); err != nil {
		return executor.NetworkAttachment{}, fmt.Errorf("create job network: %v: %s", err, out)
	}
	fail := func(format string, a ...any) (executor.NetworkAttachment, error) {
		cleanup()
		return executor.NetworkAttachment{}, fmt.Errorf(format, a...)
	}

	allow := strings.ToLower(strings.Join(job.NetworkHosts, ","))
	// Created on the default bridge (the sidecar's way out), then also joined to the job network.
	if out, err := p.dockerOut(ctx, "create", "--name", sidecar,
		"--label", "computehive.proxy=1", "--label", "computehive.worker.id="+p.workerID, "--label", "computehive.job.id="+job.ID,
		"--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
		"--pids-limit", "128", "--memory", "128m", "--memory-swap", "128m", "--cpus", "0.5",
		"-v", p.selfBin+":/proxy:ro", "--pull", "never",
		emptyImage, "/proxy", "proxy-serve", "--listen", ":"+proxyPort, "--allow", allow, "--job", job.ID); err != nil {
		return fail("create egress proxy: %v: %s", err, out)
	}
	if out, err := p.dockerOut(ctx, "network", "connect", netname, sidecar); err != nil {
		return fail("attach egress proxy to the job network: %v: %s", err, out)
	}
	if out, err := p.dockerOut(ctx, "start", sidecar); err != nil {
		return fail("start egress proxy: %v: %s", err, out)
	}
	ip, err := p.dockerOut(ctx, "inspect", "-f", fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, netname), sidecar)
	if err != nil || ip == "" {
		return fail("find the egress proxy's address: %v: %s", err, ip)
	}
	ready := false
	for i := 0; i < 50 && !ready; i++ {
		if logs, _ := p.dockerOut(ctx, "logs", sidecar); strings.Contains(logs, "proxy listening") {
			ready = true
			break
		}
		if st, _ := p.dockerOut(ctx, "inspect", "-f", "{{.State.Running}}", sidecar); st != "true" {
			logs, _ := p.dockerOut(ctx, "logs", sidecar)
			return fail("the egress proxy exited: %s", logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		return fail("the egress proxy did not start listening")
	}

	p.mu.Lock()
	p.nets[netname] = true
	p.mu.Unlock()

	proxyURL := "http://" + ip + ":" + proxyPort
	p.log.Info("job network opened", "event", "net_attach", "job_id", job.ID, "hosts", allow, "proxy", proxyURL)
	return executor.NetworkAttachment{
		Name: netname,
		Env: map[string]string{"HTTP_PROXY": proxyURL, "HTTPS_PROXY": proxyURL, "http_proxy": proxyURL, "https_proxy": proxyURL,
			"NO_PROXY": "", "no_proxy": ""},
		Cleanup: func() {
			c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			logs, _ := p.dockerOut(c, "logs", sidecar)
			cancel()
			p.log.Info("job network closed", "event", "net_detach", "job_id", job.ID,
				"requests_allowed", strings.Count(logs, "net_allowed"), "requests_denied", strings.Count(logs, "net_denied"))
			cleanup()
		},
	}, nil
}

// RunSidecar is `worker proxy-serve`: the filtering proxy that runs inside the sidecar container.
func RunSidecar(args []string) int {
	fs := flag.NewFlagSet("proxy-serve", flag.ContinueOnError)
	listen := fs.String("listen", ":"+proxyPort, "listen address")
	allow := fs.String("allow", "", "comma-separated host names this job may reach")
	job := fs.String("job", "", "job id (for log lines)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	allowed := map[string]bool{}
	for _, h := range strings.Split(*allow, ",") {
		if h = strings.TrimSpace(strings.ToLower(h)); h != "" {
			allowed[h] = true
		}
	}
	if len(allowed) == 0 {
		fmt.Fprintln(os.Stderr, "proxy-serve: --allow must list at least one host")
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	px := New(Config{Job: *job, Log: log, Allow: func(host string) bool { return allowed[host] }})
	srv := &http.Server{Addr: *listen, Handler: px, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	log.Info("proxy listening", "addr", *listen, "allow", *allow)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "proxy-serve:", err)
		return 1
	}
	return 0
}
