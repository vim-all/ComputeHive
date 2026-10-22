package policy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func at(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.Local) }

func TestScheduleWindows(t *testing.T) {
	night := Policy{Schedule: "22:00-07:00"} // spans midnight
	day := Policy{Schedule: "09:00-17:30"}
	for _, tc := range []struct {
		name string
		p    Policy
		t    time.Time
		want bool
	}{
		{"night: 23:00 inside", night, at(23, 0), true},
		{"night: 03:00 inside", night, at(3, 0), true},
		{"night: 22:00 boundary inside", night, at(22, 0), true},
		{"night: 07:00 boundary outside", night, at(7, 0), false},
		{"night: noon outside", night, at(12, 0), false},
		{"day: 09:00 inside", day, at(9, 0), true},
		{"day: 17:29 inside", day, at(17, 29), true},
		{"day: 17:30 outside", day, at(17, 30), false},
		{"day: 02:00 outside", day, at(2, 0), false},
		{"no schedule: always", Policy{}, at(4, 4), true},
	} {
		if got := tc.p.InWindow(tc.t); got != tc.want {
			t.Errorf("%s: InWindow = %v, want %v", tc.name, got, tc.want)
		}
	}
	for _, bad := range []string{"22:00", "25:00-07:00", "22:00-07:61", "ab-cd", "08:00-08:00", "8-9"} {
		if _, _, _, err := ParseSchedule(bad); err == nil {
			t.Errorf("schedule %q should be rejected", bad)
		}
	}
}

func TestAdmissionRules(t *testing.T) {
	p := Default()
	if a := p.Admit(at(12, 0), 5); !a.Allowed {
		t.Fatalf("default policy should admit: %+v", a)
	}
	p.Paused = true
	if a := p.Admit(at(12, 0), 5); a.Allowed || a.Reason != "paused" {
		t.Fatalf("paused: %+v", a)
	}
	p = Policy{Schedule: "22:00-07:00"}
	if a := p.Admit(at(12, 0), 5); a.Allowed || a.Reason != "outside_schedule" {
		t.Fatalf("outside schedule: %+v", a)
	}
	if a := p.Admit(at(23, 0), 5); !a.Allowed {
		t.Fatalf("inside schedule: %+v", a)
	}
	p = Policy{IdleOnly: true, IdleCPUPercent: 25}
	if a := p.Admit(at(12, 0), 24.9); !a.Allowed {
		t.Fatalf("idle machine: %+v", a)
	}
	if a := p.Admit(at(12, 0), 25); a.Allowed || a.Reason != "host_busy" {
		t.Fatalf("busy machine: %+v", a)
	}
	// Pause beats everything else; a busy machine is only a reason when idle_only is on.
	if a := (Policy{}).Admit(at(12, 0), 99); !a.Allowed {
		t.Fatalf("idle_only off: a busy machine is fine: %+v", a)
	}
}

func TestNetworkIsOffByDefaultAndHostsMustBeListed(t *testing.T) {
	p := Default()
	if p.HostAllowed("pypi.org") {
		t.Fatal("network must be off by default")
	}
	p.AllowNetwork = true
	if p.HostAllowed("pypi.org") {
		t.Fatal("enabling network with an empty host list must still allow nothing")
	}
	p.AllowedHosts = []string{"pypi.org", "*.githubusercontent.com"}
	for host, want := range map[string]bool{
		"pypi.org": true, "PyPI.org": true, "pypi.org.": true,
		"evil-pypi.org": false, "pypi.org.evil.com": false, "files.pythonhosted.org": false,
		"raw.githubusercontent.com": true, "githubusercontent.com": false, "x.y.githubusercontent.com": true,
		"": false,
	} {
		if got := p.HostAllowed(host); got != want {
			t.Errorf("HostAllowed(%q) = %v, want %v", host, got, want)
		}
	}
	p.AllowNetwork = false
	if p.HostAllowed("pypi.org") {
		t.Fatal("allow_network=false must override the host list")
	}
}

func TestValidateRejectsDangerousOrMalformedSettings(t *testing.T) {
	for name, p := range map[string]Policy{
		"negative cpus":      {MaxCPUs: -1},
		"negative memory":    {MaxMemoryMB: -5},
		"idle percent > 100": {IdleCPUPercent: 120},
		"bad schedule":       {Schedule: "whenever"},
		"IP as allowed host": {AllowedHosts: []string{"169.254.169.254"}},
		"IP with dots":       {AllowedHosts: []string{"10.0.0.1"}},
		"host with port":     {AllowedHosts: []string{"example.com:443"}},
		"host with path":     {AllowedHosts: []string{"example.com/x"}},
		"bare wildcard":      {AllowedHosts: []string{"*"}},
		"empty host":         {AllowedHosts: []string{""}},
		"bad image glob":     {ImageAllow: []string{"[abc"}},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("%s: policy accepted", name)
		}
	}
	if err := (Policy{AllowedHosts: []string{"pypi.org", "*.example.com"}, ImageAllow: []string{"alpine:*"}, Schedule: "22:00-07:00"}).Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
}

func TestManagerPersistsAtomicallyAndKeepsFilePrivate(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state", "policy.json")
	m, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(file); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("policy file must exist with mode 0600: %v %v", st, err)
	}
	if err := m.Update(func(p *Policy) { p.Paused = true; p.AllowNetwork = true; p.AllowedHosts = []string{"pypi.org"} }); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Get(); !got.Paused || !got.AllowNetwork || len(got.AllowedHosts) != 1 {
		t.Fatalf("policy did not survive a restart: %+v", got)
	}

	// An invalid update is refused and changes nothing, in memory or on disk.
	if err := m.Update(func(p *Policy) { p.AllowedHosts = []string{"10.0.0.1"} }); err == nil {
		t.Fatal("invalid update accepted")
	}
	if got := m.Get(); len(got.AllowedHosts) != 1 || got.AllowedHosts[0] != "pypi.org" {
		t.Fatalf("invalid update leaked into memory: %+v", got)
	}
	again, _ := Open(file)
	if got := again.Get(); got.AllowedHosts[0] != "pypi.org" {
		t.Fatalf("invalid update leaked to disk: %+v", got)
	}

	// Get returns a copy: callers cannot mutate the live policy.
	g := m.Get()
	g.AllowedHosts[0] = "evil.example"
	if m.Get().AllowedHosts[0] != "pypi.org" {
		t.Fatal("Get exposed internal state")
	}
}

func TestOpenRefusesACorruptOrInvalidPolicyFile(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(bad, []byte("{not json"), 0o600)
	if _, err := Open(bad); err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("corrupt file: %v", err)
	}
	invalid := filepath.Join(dir, "invalid.json")
	_ = os.WriteFile(invalid, []byte(`{"allowed_hosts":["10.0.0.1"]}`), 0o600)
	if _, err := Open(invalid); err == nil {
		t.Fatal("a policy that fails validation must not be loaded: failing closed beats silently ignoring it")
	}
}
