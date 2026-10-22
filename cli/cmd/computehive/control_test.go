package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeWorker serves the worker's control API on a Unix socket.
type fakeWorker struct {
	mu     sync.Mutex
	policy map[string]any
	calls  []string
}

func startFakeWorker(t *testing.T) (*fakeWorker, string) {
	t.Helper()
	fw := &fakeWorker{policy: map[string]any{
		"paused": false, "max_cpus": float64(0), "max_memory_mb": float64(0), "idle_only": false, "schedule": "",
		"allow_network": false, "allowed_hosts": []any{}, "image_allow": []any{}, "require_gvisor": false,
	}}
	mux := http.NewServeMux()
	rec := func(name string) { fw.mu.Lock(); fw.calls = append(fw.calls, name); fw.mu.Unlock() }
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		rec("status")
		_ = json.NewEncoder(w).Encode(map[string]any{"worker_id": "w1", "version": "dev", "isolation": "hardened-runc", "state": "waiting",
			"not_taking": "outside_schedule", "uptime": "5m0s", "policy": fw.policy,
			"metrics": map[string]any{"MemPercent": 41.2, "CPUPercent": 9.0, "WorkDiskUsedPercent": 59.0, "DockerDiskUsedPercent": 60.0}})
	})
	mux.HandleFunc("POST /v1/pause", func(w http.ResponseWriter, r *http.Request) {
		rec("pause?now=" + r.URL.Query().Get("now"))
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "paused"})
	})
	mux.HandleFunc("POST /v1/resume", func(w http.ResponseWriter, _ *http.Request) {
		rec("resume")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "resumed"})
	})
	mux.HandleFunc("POST /v1/kill", func(w http.ResponseWriter, _ *http.Request) {
		rec("kill")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "kill switch engaged"})
	})
	mux.HandleFunc("GET /v1/policy", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(fw.policy) })
	mux.HandleFunc("PUT /v1/policy", func(w http.ResponseWriter, r *http.Request) {
		var p map[string]any
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if hosts, _ := p["allowed_hosts"].([]any); len(hosts) > 0 && strings.HasPrefix(hosts[0].(string), "10.") {
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "allowed host is an IP address"})
			return
		}
		fw.mu.Lock()
		fw.policy = p
		fw.mu.Unlock()
		_ = json.NewEncoder(w).Encode(p)
	})
	mux.HandleFunc("GET /v1/history", func(w http.ResponseWriter, r *http.Request) {
		rec("history?limit=" + r.URL.Query().Get("limit"))
		_ = json.NewEncoder(w).Encode([]map[string]any{{"job_id": "j1", "image": "computehive-job:1", "command": "python train.py", "env_names": []string{"API_KEY"},
			"started_at": "2026-10-21T10:00:00Z", "finished_at": "2026-10-21T10:00:02.5Z", "status": "succeeded", "isolation": "hardened-runc"}})
	})
	sock := filepath.Join(t.TempDir(), "control.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return fw, sock
}

func control(t *testing.T, sock string, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := cmdWorkerControl(append(args, "--socket", sock), &out, &errOut)
	return out.String() + errOut.String(), err
}

func TestPauseResumeKillAreSentToTheWorker(t *testing.T) {
	fw, sock := startFakeWorker(t)
	for _, args := range [][]string{{"pause"}, {"pause", "--now"}, {"resume"}, {"kill"}} {
		if _, err := control(t, sock, args...); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	got := strings.Join(fw.calls, ",")
	if got != "pause?now=,pause?now=true,resume,kill" {
		t.Fatalf("calls: %s", got)
	}
}

func TestStatusExplainsWhyTheWorkerIsNotTakingJobs(t *testing.T) {
	_, sock := startFakeWorker(t)
	out, err := control(t, sock, "status")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"waiting", "not taking new jobs: outside_schedule", "hardened-runc", "RAM 41%", "allow_network"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output lacks %q:\n%s", want, out)
		}
	}
}

func TestHistoryShowsWhatRanWithoutSecrets(t *testing.T) {
	_, sock := startFakeWorker(t)
	out, err := control(t, sock, "history", "--limit", "5")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"job j1", "python train.py", "API_KEY", "values are never recorded", "took 2.5s"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output lacks %q:\n%s", want, out)
		}
	}
}

func TestPolicySetConvertsValuesToTheirTypes(t *testing.T) {
	fw, sock := startFakeWorker(t)
	if _, err := control(t, sock, "policy", "set", "max_cpus=2", "allow_network=true", "schedule=22:00-07:00", "allowed_hosts=pypi.org,*.example.com", "idle_only=true"); err != nil {
		t.Fatal(err)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.policy["max_cpus"] != float64(2) || fw.policy["allow_network"] != true || fw.policy["idle_only"] != true || fw.policy["schedule"] != "22:00-07:00" {
		t.Fatalf("policy %v", fw.policy)
	}
	hosts, _ := fw.policy["allowed_hosts"].([]any)
	if len(hosts) != 2 || hosts[1] != "*.example.com" {
		t.Fatalf("allowed_hosts %v", fw.policy["allowed_hosts"])
	}
}

func TestPolicySetRejectsBadInputClearly(t *testing.T) {
	_, sock := startFakeWorker(t)
	for _, tc := range []struct{ arg, want string }{
		{"nonsense=1", "unknown policy field"},
		{"max_cpus=lots", "must be a number"},
		{"allow_network=maybe", "must be true or false"},
		{"allowed_hosts=10.0.0.1", "IP address"}, // refused by the worker's own validation
		{"noequals", "not KEY=VALUE"},
	} {
		_, err := control(t, sock, "policy", "set", tc.arg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.arg, err, tc.want)
		}
	}
}

func TestWorkerCommandsExplainWhenNoWorkerIsRunning(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("COMPUTEHIVE_SOCKET", "")
	err := cmdWorkerControl([]string{"status"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no running worker found") {
		t.Fatalf("got %v", err)
	}
}
