package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/artifact"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

// The hostile suite runs genuinely malicious scripts inside real containers, through the same
// executor production uses, and asserts two things: every attack is contained or killed, and the
// host is unharmed afterwards (no leftover containers, secrets untouched, disk and memory intact).
// Scripts print BLOCKED:<attack> or ESCAPED:<attack>; any ESCAPED fails the test.

// try runs one attack with a 10s limit so a hanging attack cannot stall the suite; a timeout counts as blocked.
const prelude = `try() { n=$1; shift; if timeout 10 "$@" >/dev/null 2>&1; then echo "ESCAPED:$n"; else echo "BLOCKED:$n"; fi; }
`

type hostileRig struct {
	t        *testing.T
	exec     *DockerExecutor
	sentinel string // a file on the host holding a secret the job must never see
	secret   string
	workDir  string
}

func newHostileRig(t *testing.T, tweak ...func(*Sandbox, *Limits)) *hostileRig {
	t.Helper()
	needDocker(t)
	sb := DefaultSandbox()
	sb.Images = ImagePolicy{Allow: []string{testImage}}
	sb.OutputMaxBytes = 50 << 20
	sb.OutputPoll = 100 * time.Millisecond
	lim := Limits{DefaultMemoryMB: 128, DefaultCPUs: 0.5, PidsLimit: 32, StopGrace: time.Second}
	for _, f := range tweak {
		f(&sb, &lim)
	}

	var rnd [16]byte
	_, _ = rand.Read(rnd[:])
	secret := "TOP-SECRET-" + hex.EncodeToString(rnd[:])
	dir, err := os.MkdirTemp("", "ch-host-secret-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sentinel := filepath.Join(dir, "credentials.txt")
	if err := os.WriteFile(sentinel, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	return &hostileRig{t: t, exec: NewDockerExecutor("docker", "hostile", false, "/computehive/output", lim, sb),
		sentinel: sentinel, secret: secret, workDir: os.TempDir()}
}

func (r *hostileRig) run(id, script string, mutate ...func(*domain.Job)) domain.JobResult {
	r.t.Helper()
	job := domain.Job{ID: id, ImageRef: testImage, Command: []string{"sh", "-c", prelude + script}}
	for _, m := range mutate {
		m(&job)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	res := r.exec.Run(ctx, job, artifact.Bundle{})
	r.hostIsHealthy(id)
	return res
}

func freeBytes(dir string) uint64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	return st.Bavail * uint64(st.Bsize)
}

// hostIsHealthy: nothing left running or stranded, the secret is intact, Docker still answers.
func (r *hostileRig) hostIsHealthy(id string) {
	r.t.Helper()
	if containerExists(containerName("hostile", id)) {
		r.t.Errorf("container for %s is still present after the job", id)
	}
	if out, _ := exec.Command("docker", "ps", "-a", "-q", "--filter", "label=computehive.worker.id=hostile").Output(); strings.TrimSpace(string(out)) != "" {
		r.t.Errorf("hostile containers left behind: %s", out)
	}
	if b, err := os.ReadFile(r.sentinel); err != nil || string(b) != r.secret {
		r.t.Errorf("the host secret was tampered with or removed: %q %v", b, err)
	}
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		r.t.Errorf("Docker stopped answering after the job: %v", err)
	}
}

func markers(out, kind string) []string {
	var got []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, kind+":") {
			got = append(got, strings.TrimPrefix(line, kind+":"))
		}
	}
	return got
}

// requireAllBlocked fails if any attack escaped, and if fewer than want attacks were even attempted
// (so a script that silently did nothing cannot pass).
func requireAllBlocked(t *testing.T, res domain.JobResult, want int) {
	t.Helper()
	if esc := markers(res.Stdout, "ESCAPED"); len(esc) > 0 {
		t.Fatalf("ATTACKS SUCCEEDED: %v\nstdout:\n%s\nstderr:\n%s", esc, res.Stdout, res.Stderr)
	}
	if blocked := markers(res.Stdout, "BLOCKED"); len(blocked) < want {
		t.Fatalf("only %d of %d attacks were attempted; the script did not run properly\nstdout:\n%s\nstderr:\n%s\nresult: %+v", len(blocked), want, res.Stdout, res.Stderr, res)
	}
}

func TestHostile(t *testing.T) {
	r := newHostileRig(t)

	t.Run("baseline: no privileges, filtered syscalls", func(t *testing.T) {
		res := r.run("base", `
echo "uid=$(id -u)"
echo "capeff=$(grep CapEff /proc/self/status | awk '{print $2}')"
echo "nonewprivs=$(grep NoNewPrivs /proc/self/status | awk '{print $2}')"
echo "seccomp=$(grep '^Seccomp:' /proc/self/status | awk '{print $2}')"
echo "readonly_root=$(grep ' / ' /proc/mounts | head -1 | grep -c 'ro[ ,]')"
`)
		if res.Status != domain.JobStatusSucceeded {
			t.Fatalf("baseline job failed: %+v", res)
		}
		out := res.Stdout
		for _, want := range []string{"capeff=0000000000000000", "nonewprivs=1", "seccomp=2", "readonly_root=1"} {
			if !strings.Contains(out, want) {
				t.Errorf("missing %q in:\n%s", want, out)
			}
		}
		if strings.Contains(out, "uid=0") {
			t.Errorf("job ran as root:\n%s", out)
		}
	})

	t.Run("(a) network: cannot reach the internet, LAN, or resolve names", func(t *testing.T) {
		res := r.run("net", `
try wget-ip       wget -T 3 -q -O- http://1.1.1.1/
try wget-name     wget -T 3 -q -O- http://example.com/
try tcp-connect   nc -w 2 -z 1.1.1.1 443
try tcp-lan       nc -w 2 -z 192.168.1.1 80
try tcp-docker-gw nc -w 2 -z 172.17.0.1 22
try ping          ping -c 1 -W 2 8.8.8.8
try dns           nslookup example.com 8.8.8.8
echo "interfaces=$(ls /sys/class/net | tr '\n' ' ')"
`)
		requireAllBlocked(t, res, 7)
		if !strings.Contains(res.Stdout, "interfaces=lo") || strings.Contains(res.Stdout, "eth0") {
			t.Errorf("the container should only have a loopback interface:\n%s", res.Stdout)
		}
	})

	t.Run("(b) host files: the host's secret is unreachable by any path", func(t *testing.T) {
		res := r.run("readhost", fmt.Sprintf(`
try cat-secret      cat %[1]s
try cat-parent      ls %[2]s
try find-by-name    sh -c 'find / -xdev -name credentials.txt 2>/dev/null | grep -q .'
try grep-for-secret sh -c 'find / -xdev -type f 2>/dev/null | xargs grep -l "%[3]s" 2>/dev/null | grep -q .'
try proc-root       cat /proc/1/root%[1]s
try host-etc-shadow sh -c 'grep -q "^root:[^!*]" /etc/shadow'
try home-dirs       sh -c 'ls /home/* 2>/dev/null | grep -q .'
try mount-host      mount --bind %[2]s /mnt
`, r.sentinel, filepath.Dir(r.sentinel), r.secret))
		requireAllBlocked(t, res, 8)
		if strings.Contains(res.Stdout+res.Stderr, r.secret) {
			t.Fatal("the host secret appeared in the job's output")
		}
	})

	t.Run("(c) writes outside the sandbox are refused; only /tmp and the work dir are writable", func(t *testing.T) {
		res := r.run("writeout", `
try write-root      sh -c 'echo x > /pwned'
try write-etc       sh -c 'echo x > /etc/pwned'
try write-usr-bin   sh -c 'echo x > /usr/bin/pwned'
try write-bin       sh -c 'cp /bin/sh /bin/pwned'
try mkdir-var       mkdir /var/pwned
try proc-sys        sh -c 'echo evil > /proc/sys/kernel/hostname'
try sys-write       sh -c 'echo 1 > /sys/kernel/uevent_helper'
try exec-from-tmp   sh -c 'cp /bin/busybox /tmp/bb && chmod +x /tmp/bb && /tmp/bb true'
try remount-rw      mount -o remount,rw /
echo ok > /tmp/scratch.txt && echo "TMP_WRITABLE"
echo ok > /computehive/output/result.txt && echo "OUT_WRITABLE"
`)
		requireAllBlocked(t, res, 9)
		for _, want := range []string{"TMP_WRITABLE", "OUT_WRITABLE"} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("legitimate writes must still work (%s missing):\n%s", want, res.Stdout)
			}
		}
		if _, err := os.Stat(filepath.Join(res.OutputDirHostPath, "result.txt")); err != nil {
			t.Errorf("the job's legitimate output was not delivered to the work dir: %v", err)
		}
		os.RemoveAll(res.CleanupDir)
	})

	t.Run("privilege escalation attempts all fail", func(t *testing.T) {
		res := r.run("privesc", `
try unshare-user    unshare -r true
try unshare-mount   unshare -m true
try mount-tmpfs     mount -t tmpfs none /mnt
try chown-root      chown root /tmp
try chroot          chroot / true
try mknod-disk      mknod /tmp/sda b 8 0
try su-root         su -c true root
try insmod          insmod /dev/null
try setuid-exec     sh -c 'cp /bin/busybox /tmp/s && chmod u+s /tmp/s && /tmp/s id -u | grep -qx 0'
try read-proc-mem   sh -c 'cat /proc/1/mem'
try read-kcore      sh -c 'head -c 16 /proc/kcore | wc -c | grep -qv "^0$"'
`)
		requireAllBlocked(t, res, 11)
	})

	t.Run("(h) the Docker socket is not reachable", func(t *testing.T) {
		res := r.run("sock", `
try ls-socket-1     ls /var/run/docker.sock
try ls-socket-2     ls /run/docker.sock
try find-socket     sh -c 'find / -xdev -name "docker.sock" 2>/dev/null | grep -q .'
try docker-cli      sh -c 'command -v docker'
try docker-host-env sh -c '[ -n "$DOCKER_HOST" ]'
try connect-unix    nc -U /var/run/docker.sock
`)
		requireAllBlocked(t, res, 6)
	})

	t.Run("(i) cloud metadata endpoints are unreachable", func(t *testing.T) {
		res := r.run("meta", `
try aws-imds        wget -T 2 -q -O- http://169.254.169.254/latest/meta-data/
try aws-imds-nc     nc -w 2 -z 169.254.169.254 80
try gcp-metadata    wget -T 2 -q -O- --header 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/
try azure-imds      wget -T 2 -q -O- --header 'Metadata: true' http://169.254.169.254/metadata/instance
try ecs-task        wget -T 2 -q -O- http://169.254.170.2/v2/credentials
try k8s-api         nc -w 2 -z 10.0.0.1 443
`)
		requireAllBlocked(t, res, 6)
	})

	t.Run("(d) fork bomb is stopped at the PID limit", func(t *testing.T) {
		res := r.run("fork", `ok=0; i=0; while [ $i -lt 500 ]; do if (sleep 5 &) 2>/dev/null; then ok=$((ok+1)); fi; i=$((i+1)); done; echo "spawned=$ok"`)
		var spawned int
		if idx := strings.Index(res.Stdout, "spawned="); idx < 0 {
			t.Fatalf("no result: %+v", res)
		} else if _, err := fmt.Sscanf(res.Stdout[idx:], "spawned=%d", &spawned); err != nil {
			t.Fatal(err)
		}
		if spawned >= 32 || spawned == 0 {
			t.Fatalf("fork bomb created %d processes (PID limit is 32)", spawned)
		}
		t.Logf("fork bomb: 500 attempted, %d allowed", spawned)
	})

	t.Run("(e) allocating all memory is stopped at the memory cap", func(t *testing.T) {
		hostBefore := hostAvailableMB(t)
		res := r.run("mem", `x=$(head -c 900000000 /dev/zero | tr '\0' a); echo "survived=${#x}"`, func(j *domain.Job) { j.MemoryMB = 64 })
		if strings.Contains(res.Stdout, "survived=") || res.Status == domain.JobStatusSucceeded {
			t.Fatalf("the job allocated past its 64 MB cap: %+v", res)
		}
		if res.ExitCode != 137 {
			t.Fatalf("exit code %d, want 137 (OOM kill): %+v", res.ExitCode, res)
		}
		time.Sleep(500 * time.Millisecond)
		if after := hostAvailableMB(t); before(hostBefore, after) > 400 {
			t.Fatalf("host lost %d MB of available memory to a job capped at 64 MB", before(hostBefore, after))
		}
	})

	t.Run("(f) a CPU spin is killed by the wall-clock timeout and capped at its CPU quota", func(t *testing.T) {
		job := domain.Job{ID: "spin", ImageRef: testImage, Command: []string{"sh", "-c", `while :; do :; done`}}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		res := r.exec.Run(ctx, job, artifact.Bundle{})
		r.hostIsHealthy("spin")
		if res.FailureReason != domain.FailureTimeout {
			t.Fatalf("infinite loop not classified as a timeout: %+v", res)
		}
		if took := time.Since(start); took > 15*time.Second {
			t.Fatalf("forced kill took %s", took)
		}
		t.Logf("infinite loop killed after %s", time.Since(start).Round(100*time.Millisecond))

		// And while it runs it cannot take more than its quota (0.5 CPU) even with 4 busy threads.
		res = r.run("spinquota", `
read_usage() { grep '^usage_usec' /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '{print $2}'; }
u0=$(read_usage); [ -z "$u0" ] && { echo "NOCGROUPV2"; exit 0; }
for i in 1 2 3 4; do (timeout 3 sh -c 'while :; do :; done' >/dev/null 2>&1 &); done
sleep 3.2
u1=$(read_usage)
echo "cpu_usec=$((u1-u0))"
`)
		if strings.Contains(res.Stdout, "NOCGROUPV2") {
			t.Skip("cgroup v2 cpu.stat not visible in the container")
		}
		var usec int64
		if idx := strings.Index(res.Stdout, "cpu_usec="); idx < 0 {
			t.Fatalf("no cpu reading: %+v", res)
		} else if _, err := fmt.Sscanf(res.Stdout[idx:], "cpu_usec=%d", &usec); err != nil {
			t.Fatal(err)
		}
		cores := float64(usec) / 3.2e6
		t.Logf("4 spinning threads used %.2f CPU cores (quota 0.5)", cores)
		if cores > 0.75 {
			t.Fatalf("job used %.2f cores with a 0.5 core quota", cores)
		}
	})

	t.Run("(g) filling the disk is stopped; the host disk is unaffected", func(t *testing.T) {
		freeBefore := freeBytes(r.workDir)
		res := r.run("disk", `
i=0
while :; do dd if=/dev/zero of=/computehive/output/f$i bs=1M count=20 2>/dev/null; i=$((i+1)); done
`)
		if res.FailureReason != domain.FailureJobExit || !strings.Contains(res.Error, "work directory exceeded") {
			t.Fatalf("disk fill was not stopped by the quota: %+v", res)
		}
		used, _ := dirUsage(res.OutputDirHostPath)
		t.Logf("disk fill stopped with %d MB written (cap 50 MB)", used>>20)
		if used > 200<<20 {
			t.Fatalf("overshoot too large: %d MB written against a 50 MB cap", used>>20)
		}
		os.RemoveAll(res.CleanupDir)
		time.Sleep(300 * time.Millisecond)
		if freeAfter := freeBytes(r.workDir); freeBefore > freeAfter && freeBefore-freeAfter > 150<<20 {
			t.Fatalf("host lost %d MB of free disk after cleanup", (freeBefore-freeAfter)>>20)
		}

		res = r.run("diskroot", `
try fill-rootfs  sh -c 'dd if=/dev/zero of=/fill bs=1M count=50 2>/dev/null'
try fill-tmp-big sh -c 'dd if=/dev/zero of=/tmp/fill bs=1M count=400 2>/dev/null'
echo "tmp_bytes=$(wc -c < /tmp/fill 2>/dev/null)"
`, func(j *domain.Job) { j.MemoryMB = 256 })
		requireAllBlocked(t, res, 2)
		var n int64
		if idx := strings.Index(res.Stdout, "tmp_bytes="); idx >= 0 {
			_, _ = fmt.Sscanf(res.Stdout[idx:], "tmp_bytes=%d", &n)
		}
		if n > 70<<20 {
			t.Fatalf("/tmp scratch grew to %d MB despite its 64 MB size limit", n>>20)
		}
		os.RemoveAll(res.CleanupDir)
	})

	t.Run("output is bounded: a job printing gigabytes costs a fixed amount of memory", func(t *testing.T) {
		res := r.run("spam", `yes AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA | head -c 300000000`)
		if len(res.Stdout) > (1<<20)+512 {
			t.Fatalf("captured %d bytes of stdout; the cap is 1 MiB", len(res.Stdout))
		}
		if !strings.Contains(res.Stdout, "output truncated") {
			t.Fatal("truncation must be reported")
		}
	})
}

func hostAvailableMB(t *testing.T) int64 {
	t.Helper()
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Skip("no /proc/meminfo")
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			var kb int64
			_, _ = fmt.Sscanf(strings.TrimPrefix(line, "MemAvailable:"), "%d", &kb)
			return kb / 1024
		}
	}
	return 0
}

func before(a, b int64) int64 { return a - b }

// A malicious image archive must not be able to hijack tags the contributor already has.
func TestHostileImageArchiveCannotRetagTheContributorsImages(t *testing.T) {
	needDocker(t)
	// Build an archive of alpine but tagged as something outside the worker's namespace.
	dir := t.TempDir()
	const evil = "alpine:3.21"
	tarPath := filepath.Join(dir, "evil.tar")
	if out, err := exec.Command("docker", "save", "-o", tarPath, evil).CombinedOutput(); err != nil {
		t.Skipf("cannot create archive: %v: %s", err, out)
	}
	sb := DefaultSandbox()
	e := NewDockerExecutor("docker", "hostile", false, "/computehive/output", DefaultLimits(), sb)
	job := domain.Job{ID: "evil-tar", ImageRef: evil, Command: []string{"true"}}
	res := e.Run(context.Background(), job, artifact.Bundle{ArchivePath: tarPath})
	if res.Status == domain.JobStatusSucceeded {
		t.Fatalf("an archive tagged %s was loaded and run: %+v", evil, res)
	}
	if !strings.Contains(res.Error, "outside the allowed namespaces") {
		t.Fatalf("expected a namespace refusal, got: %q", res.Error)
	}
	if res.FailureReason != domain.FailureJobExit {
		t.Fatalf("a hostile archive is the submitter's fault, got reason %q", res.FailureReason)
	}
}

// An image that is not on the allow list is never pulled or run.
func TestHostileUnlistedImageIsRefusedAndNeverPulled(t *testing.T) {
	needDocker(t)
	e := NewDockerExecutor("docker", "hostile", false, "/computehive/output", DefaultLimits(), DefaultSandbox()) // empty allow list
	res := e.Run(context.Background(), domain.Job{ID: "unlisted", ImageRef: "alpine:3.21", Command: []string{"true"}}, artifact.Bundle{})
	if res.Status == domain.JobStatusSucceeded || !strings.Contains(res.Error, "allow list") {
		t.Fatalf("unlisted image ran: %+v", res)
	}
	res = e.Run(context.Background(), domain.Job{ID: "nopull", ImageRef: "registry.invalid/never/pulled:1", Command: []string{"true"}}, artifact.Bundle{})
	if res.Status == domain.JobStatusSucceeded || strings.Contains(res.Error, "Pulling") {
		t.Fatalf("worker tried to pull: %+v", res)
	}
}
