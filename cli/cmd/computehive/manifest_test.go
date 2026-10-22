package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func write(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestManifestReadsYAMLAndDockerfileLabelsWithYAMLWinning(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "Dockerfile", "FROM alpine\nLABEL computehive.cpu=2 computehive.mem=512 maintainer=me\nLABEL computehive.arch=arm64\n")
	write(t, dir, "computehive.yaml", "# job defaults\nmem: 2048   # more than the Dockerfile says\nverify: true\nlabels: fast-disk, rack-1\nnetwork_hosts: pypi.org\n")
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cpu, _ := m.integer("cpu", 9); cpu != 2 {
		t.Errorf("cpu from Dockerfile label = %d", cpu)
	}
	if mem, _ := m.integer("mem", 9); mem != 2048 {
		t.Errorf("yaml should override the Dockerfile: mem = %d", mem)
	}
	if got := m.str("arch", ""); got != "arm64" {
		t.Errorf("arch = %q", got)
	}
	if v, err := m.boolean("verify", false); err != nil || !v {
		t.Errorf("verify = %v, %v", v, err)
	}
	if got := m.list("labels"); !reflect.DeepEqual(got, []string{"fast-disk", "rack-1"}) {
		t.Errorf("labels = %v", got)
	}
	if got := m.list("network_hosts"); !reflect.DeepEqual(got, []string{"pypi.org"}) {
		t.Errorf("network_hosts = %v", got)
	}
	if gpu, _ := m.integer("gpu", 7); gpu != 7 {
		t.Errorf("an absent key must return the fallback, got %d", gpu)
	}
}

func TestManifestWithoutFilesIsEmptyNotAnError(t *testing.T) {
	m, err := loadManifest(t.TempDir())
	if err != nil || len(m.set) != 0 {
		t.Fatalf("%v %v", m, err)
	}
}

func TestManifestRejectsTyposAndBadValuesInsteadOfIgnoringThem(t *testing.T) {
	cases := map[string]struct{ file, body, want string }{
		"unknown yaml key": {"computehive.yaml", "mem_mb: 4096\n", "unknown key"},
		"not key value":    {"computehive.yaml", "just words\n", "key: value"},
		"unknown label":    {"Dockerfile", "LABEL computehive.memory=4\n", "unknown label"},
	}
	for name, tc := range cases {
		dir := t.TempDir()
		write(t, dir, tc.file, tc.body)
		if _, err := loadManifest(dir); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error mentioning %q", name, err, tc.want)
		}
	}
	dir := t.TempDir()
	write(t, dir, "computehive.yaml", "cpu: lots\nverify: maybe\n")
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.integer("cpu", 1); err == nil {
		t.Error("a non-numeric cpu was accepted")
	}
	if _, err := m.boolean("verify", false); err == nil {
		t.Error("a non-boolean verify was accepted")
	}
}
