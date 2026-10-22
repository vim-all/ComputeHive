package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

func TestValidateImageRef(t *testing.T) {
	good := []string{"alpine", "alpine:3.21", "computehive-job:1792620584894826381", "registry.example.com:5000/team/app:v1.2",
		"ghcr.io/org/img@sha256:" + strings.Repeat("a", 64)}
	bad := map[string]string{
		"":                       "empty",
		"--privileged":           "flag injection",
		"-v":                     "flag injection",
		"img name":               "space",
		"img;rm -rf /":           "shell metacharacters",
		"img$(id)":               "command substitution",
		"a/../b":                 "path traversal",
		"img\nflag":              "newline",
		strings.Repeat("a", 300): "too long",
	}
	for _, ref := range good {
		if err := ValidateImageRef(ref); err != nil {
			t.Errorf("%q should be valid: %v", ref, err)
		}
	}
	for ref, why := range bad {
		if err := ValidateImageRef(ref); err == nil {
			t.Errorf("%q (%s) must be rejected", ref, why)
		}
	}
}

func TestValidateJobFields(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		cmd  []string
		ok   bool
	}{
		{"fine", map[string]string{"A": "1", "B_2": "x"}, []string{"echo", "hi"}, true},
		{"bad name", map[string]string{"1A": "x"}, nil, false},
		{"name with equals", map[string]string{"A=B": "x"}, nil, false},
		{"reserved prefix", map[string]string{"COMPUTEHIVE_OUTPUT_DIR": "/"}, nil, false},
		{"reserved prefix lowercase", map[string]string{"computehive_x": "1"}, nil, false},
		{"nul in value", map[string]string{"A": "a\x00b"}, nil, false},
		{"huge value", map[string]string{"A": strings.Repeat("x", maxEnvValueLen+1)}, nil, false},
		{"too many args", nil, make([]string, maxCommandArgs+1), false},
		{"nul in arg", nil, []string{"a\x00"}, false},
	}
	for _, tc := range cases {
		err := ValidateJobFields(tc.env, tc.cmd)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestImagePolicyDenyWinsAndEmptyAllowAllowsNothing(t *testing.T) {
	p := ImagePolicy{Allow: []string{"alpine:*", "busybox"}, Deny: []string{"alpine:edge"}}
	for ref, want := range map[string]bool{"alpine:3.21": true, "busybox": true, "alpine:edge": false, "ubuntu:24.04": false, "--privileged": false} {
		if err := p.Check(ref); (err == nil) != want {
			t.Errorf("Check(%q) = %v, want allowed=%v", ref, err, want)
		}
	}
	if err := (ImagePolicy{}).Check("alpine:3.21"); err == nil {
		t.Error("an empty allow list must allow nothing: the worker only runs images it was sent unless the contributor opts in")
	}
}

func hardened(t *testing.T) (*DockerExecutor, domain.Job, string) {
	t.Helper()
	sb := DefaultSandbox()
	sb.DockerConfigDir = t.TempDir()
	e := NewDockerExecutor("docker", "w1", false, "/computehive/output", DefaultLimits(), sb)
	job := domain.Job{ID: "j1", ImageRef: "computehive-job:1", Command: []string{"echo", "hi"}, Env: map[string]string{"A": "1"}}
	return e, job, "/tmp/out"
}

func has(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func TestBuiltArgsCarryEveryHardeningFlag(t *testing.T) {
	e, job, out := hardened(t)
	args, err := e.buildRunArgs(job, out)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{
		{"--network", "none"}, {"--cap-drop", "ALL"}, {"--security-opt", "no-new-privileges"}, {"--pull", "never"},
		{"--pids-limit", "256"}, {"--memory", "512m"}, {"--memory-swap", "512m"}, {"-v", out + ":/computehive/output"},
		{"--ulimit", "core=0:0"},
	} {
		if !has(args, kv[0], kv[1]) {
			t.Errorf("missing %s %s in %v", kv[0], kv[1], args)
		}
	}
	joined := " " + strings.Join(args, " ") + " "
	for _, want := range []string{" --read-only ", " --init ", "noexec", "nofile=1024:1024", "fsize="} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in %v", want, args)
		}
	}
	for _, forbidden := range []string{"--privileged", "--cap-add", "--device", "--pid", "--ipc", "--userns", "--uts", "--volumes-from", "--mount", "--sysctl", "host"} {
		for _, tok := range args { // exact tokens: "--pid" must not match "--pids-limit"
			if tok == forbidden {
				t.Errorf("hardened args contain %q: %v", forbidden, args)
			}
		}
	}
	for _, tok := range args {
		if strings.Contains(tok, "docker.sock") || strings.Contains(tok, "unconfined") {
			t.Errorf("hardened args contain %q", tok)
		}
	}
	user := ""
	for i, a := range args {
		if a == "--user" {
			user = args[i+1]
		}
	}
	if user == "" || isRootUser(user) {
		t.Fatalf("job user %q must be non-root", user)
	}
	if err := auditRunArgs(args, job.ImageRef, e.expect(out, "none")); err != nil {
		t.Fatalf("the builder's own output must pass the audit: %v", err)
	}
}

// Every unsafe variant of an otherwise valid command line must be refused by the audit.
func TestAuditRefusesEveryUnsafeVariant(t *testing.T) {
	e, job, out := hardened(t)
	good, _ := e.buildRunArgs(job, out)
	expect := e.expect(out, "none")

	insertFlags := func(extra ...string) []string { // add flags right after "run"
		return append(append([]string{"run"}, extra...), good[1:]...)
	}
	replace := func(flag, value string) []string {
		out := append([]string(nil), good...)
		for i := 0; i+1 < len(out); i++ {
			if out[i] == flag {
				out[i+1] = value
			}
		}
		return out
	}
	drop := func(flag string, hasValue bool) []string {
		var out []string
		for i := 0; i < len(good); i++ {
			if good[i] == flag {
				if hasValue {
					i++
				}
				continue
			}
			out = append(out, good[i])
		}
		return out
	}
	swapImage := func(args []string, ref, with string) []string {
		out := append([]string(nil), args...)
		for i := range out {
			if out[i] == ref {
				out[i] = with
			}
		}
		return out
	}
	withDockerSocketMount := func() []string {
		out := append([]string(nil), good...)
		for i := range out {
			if out[i] == "-v" {
				out[i+1] = "/var/run/docker.sock:/var/run/docker.sock"
			}
		}
		return out
	}

	bad := map[string][]string{
		"--privileged":              insertFlags("--privileged"),
		"--cap-add":                 insertFlags("--cap-add", "SYS_ADMIN"),
		"--device":                  insertFlags("--device", "/dev/sda"),
		"--pid host":                insertFlags("--pid", "host"),
		"--ipc host":                insertFlags("--ipc", "host"),
		"--userns host":             insertFlags("--userns", "host"),
		"--volumes-from":            insertFlags("--volumes-from", "other"),
		"--mount":                   insertFlags("--mount", "type=bind,src=/,dst=/host"),
		"--sysctl":                  insertFlags("--sysctl", "net.ipv4.ip_forward=1"),
		"second -v":                 insertFlags("-v", "/etc:/etc:ro"),
		"extra security-opt":        insertFlags("--security-opt", "seccomp=unconfined"),
		"apparmor unconfined":       insertFlags("--security-opt", "apparmor=unconfined"),
		"docker socket mount":       withDockerSocketMount(),
		"network host":              replace("--network", "host"),
		"network bridge":            replace("--network", "bridge"),
		"root user":                 replace("--user", "0:0"),
		"root by name":              replace("--user", "root"),
		"no cap-drop":               drop("--cap-drop", true),
		"cap-drop not ALL":          replace("--cap-drop", "NET_RAW"),
		"no no-new-privileges":      drop("--security-opt", true),
		"writable rootfs":           drop("--read-only", false),
		"no pull never":             replace("--pull", "always"),
		"swap beyond memory":        replace("--memory-swap", "4096m"),
		"no pids limit":             drop("--pids-limit", true),
		"unlimited pids":            replace("--pids-limit", "0"),
		"wrong mount":               replace("-v", "/home:/computehive/output"),
		"unknown flag":              insertFlags("--cgroup-parent", "x"),
		"runtime not configured":    insertFlags("--runtime", "nvidia"),
		"gpu when not allowed":      insertFlags("--gpus", "all"),
		"no image":                  good[:len(good)-3],
		"not a run command":         append([]string{"exec"}, good[1:]...),
		"wrong image at image slot": swapImage(good, job.ImageRef, "evil"),
	}
	for name, args := range bad {
		t.Run(name, func(t *testing.T) {
			if err := auditRunArgs(args, job.ImageRef, expect); err == nil {
				t.Fatalf("audit accepted an unsafe command line: %v", args)
			}
		})
	}
}

func TestAuditIgnoresTheContainersOwnCommandLine(t *testing.T) {
	e, job, out := hardened(t)
	// A command that merely *mentions* dangerous strings is just data for the container.
	job.Command = []string{"sh", "-c", "ls /var/run/docker.sock; echo --privileged -v /:/host"}
	args, err := e.buildRunArgs(job, out)
	if err != nil {
		t.Fatal(err)
	}
	if err := auditRunArgs(args, job.ImageRef, e.expect(out, "none")); err != nil {
		t.Fatalf("a command line after the image must not trip the audit: %v", err)
	}
}

func TestBuildArgsRejectsHostileSpecs(t *testing.T) {
	e, job, out := hardened(t)
	for name, mut := range map[string]func(*domain.Job){
		"flag as image":  func(j *domain.Job) { j.ImageRef = "--privileged" },
		"bad env name":   func(j *domain.Job) { j.Env = map[string]string{"A B": "x"} },
		"reserved env":   func(j *domain.Job) { j.Env = map[string]string{"COMPUTEHIVE_OUTPUT_DIR": "/"} },
		"nul in command": func(j *domain.Job) { j.Command = []string{"a\x00b"} },
	} {
		j := job
		mut(&j)
		if _, err := e.buildRunArgs(j, out); err == nil {
			t.Errorf("%s: spec accepted", name)
		}
	}
}

func TestRootJobUserIsNeverAccepted(t *testing.T) {
	for _, u := range []string{"0", "root", "0:0", "root:root"} {
		sb := DefaultSandbox()
		sb.User = u
		e := NewDockerExecutor("docker", "w", false, "/out", DefaultLimits(), sb)
		if _, err := e.resolveUser(); err == nil {
			t.Errorf("user %q accepted", u)
		}
	}
	e := NewDockerExecutor("docker", "w", false, "/out", DefaultLimits())
	u, err := e.resolveUser()
	if err != nil || isRootUser(u) {
		t.Fatalf("auto user = %q, %v", u, err)
	}
}

func TestExecutorEnvIsolatesDockerConfig(t *testing.T) {
	sb := DefaultSandbox()
	sb.DockerConfigDir = t.TempDir()
	e := NewDockerExecutor("docker", "w", false, "/out", DefaultLimits(), sb)
	t.Setenv("DOCKER_CONFIG", "/home/someone/.docker") // the contributor's real config, with registry logins
	found := ""
	for _, kv := range e.cmdEnv() {
		if strings.HasPrefix(kv, "DOCKER_CONFIG=") {
			found = kv // the last assignment wins in exec.Cmd
		}
	}
	if found != "DOCKER_CONFIG="+sb.DockerConfigDir {
		t.Fatalf("docker would use %q, not the empty config dir", found)
	}
	entries, _ := os.ReadDir(sb.DockerConfigDir)
	if len(entries) != 0 {
		t.Fatal("the isolated docker config dir must be empty (no stored credentials)")
	}
}

func TestLimitedBufferCapsMemoryButConsumesEverything(t *testing.T) {
	b := newLimitedBuffer(10)
	for i := 0; i < 1000; i++ {
		n, err := b.Write([]byte("0123456789"))
		if n != 10 || err != nil {
			t.Fatalf("Write must report full consumption (so the job never blocks): %d %v", n, err)
		}
	}
	got := b.String()
	if !strings.HasPrefix(got, "0123456789\n[output truncated") || len(got) > 120 {
		t.Fatalf("unexpected capture %q", got)
	}
	if !strings.Contains(got, "10000 bytes") {
		t.Fatalf("truncation notice should report the total: %q", got)
	}
	small := newLimitedBuffer(100)
	_, _ = small.Write([]byte("hi"))
	if small.String() != "hi" {
		t.Fatal("untruncated output must be returned verbatim")
	}
}

func TestWatchDirSizeFiresOnBytesAndOnEntryCount(t *testing.T) {
	run := func(setup func(dir string)) (fired bool) {
		dir := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var f atomic.Bool
		done := make(chan struct{})
		go func() {
			watchDirSize(ctx, dir, 1000, 20, 10*time.Millisecond, func(int64, int) { f.Store(true) })
			close(done)
		}()
		time.Sleep(30 * time.Millisecond)
		setup(dir)
		select {
		case <-done:
		case <-ctx.Done():
		}
		return f.Load()
	}
	if run(func(string) {}) {
		t.Error("an untouched directory must not trip the watcher")
	}
	if run(func(dir string) { _ = os.WriteFile(filepath.Join(dir, "small"), make([]byte, 500), 0o600) }) {
		t.Error("500 bytes is under the 1000 byte cap")
	}
	if !run(func(dir string) { _ = os.WriteFile(filepath.Join(dir, "big"), make([]byte, 5000), 0o600) }) {
		t.Error("5000 bytes must trip a 1000 byte cap")
	}
	if !run(func(dir string) {
		for i := 0; i < 30; i++ {
			_ = os.WriteFile(filepath.Join(dir, "f"+strings.Repeat("x", i)), nil, 0o600)
		}
	}) {
		t.Error("30 entries must trip a 20 entry cap (inode exhaustion)")
	}
}

func TestDirUsageDoesNotFollowSymlinks(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(t.TempDir(), "huge")
	if err := os.WriteFile(big, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(big, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if b, _ := dirUsage(dir); b != 0 {
		t.Fatalf("a symlink counted as %d bytes: it must be counted as a link", b)
	}
}

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutputDigestIsStableAcrossMachinesAndSensitiveToEverythingThatMatters(t *testing.T) {
	files := map[string]string{"result.csv": "a,b\n1,2\n", "sub/log.txt": "done\n"}
	d1, d2 := t.TempDir(), t.TempDir()
	writeFiles(t, d1, files)
	// Same content written in a different order, with different timestamps and permissions.
	writeFiles(t, d2, map[string]string{"sub/log.txt": "done\n"})
	time.Sleep(10 * time.Millisecond)
	writeFiles(t, d2, map[string]string{"result.csv": "a,b\n1,2\n"})
	_ = os.Chmod(filepath.Join(d2, "result.csv"), 0o600)
	_ = os.Chtimes(filepath.Join(d2, "result.csv"), time.Unix(1, 0), time.Unix(1, 0))

	base := OutputDigest(0, "hello\n", d1)
	if base == "" || len(base) != 64 {
		t.Fatalf("digest %q", base)
	}
	if got := OutputDigest(0, "hello\n", d2); got != base {
		t.Fatalf("identical output hashed differently (timestamps, permissions or order leaked in):\n%s\n%s", base, got)
	}
	for name, got := range map[string]string{
		"different stdout":    OutputDigest(0, "hello!\n", d1),
		"different exit code": OutputDigest(1, "hello\n", d1),
		"empty dir":           OutputDigest(0, "hello\n", t.TempDir()),
	} {
		if got == base {
			t.Errorf("%s produced the same digest", name)
		}
	}
	d3 := t.TempDir()
	writeFiles(t, d3, map[string]string{"result.csv": "a,b\n1,3\n", "sub/log.txt": "done\n"}) // one byte differs
	if OutputDigest(0, "hello\n", d3) == base {
		t.Error("a changed byte in an output file went unnoticed")
	}
	d4 := t.TempDir()
	writeFiles(t, d4, map[string]string{"renamed.csv": "a,b\n1,2\n", "sub/log.txt": "done\n"})
	if OutputDigest(0, "hello\n", d4) == base {
		t.Error("a renamed output file went unnoticed")
	}
}

func TestOutputDigestIgnoresSymlinksAndReturnsEmptyWhenItCannotFingerprint(t *testing.T) {
	d := t.TempDir()
	writeFiles(t, d, map[string]string{"a.txt": "x"})
	before := OutputDigest(0, "", d)
	if err := os.Symlink("/etc/passwd", filepath.Join(d, "link")); err != nil {
		t.Fatal(err)
	}
	if OutputDigest(0, "", d) != before {
		t.Error("a symlink out of the sandbox changed the digest (it must not be read or counted)")
	}
	if got := OutputDigest(0, "", filepath.Join(d, "does-not-exist")); got != "" {
		t.Errorf("an unreadable output directory must give an empty digest (which never matches), got %q", got)
	}
}
