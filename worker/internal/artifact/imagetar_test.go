package artifact

import (
	"archive/tar"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeImageTar(t *testing.T, manifest any, extra map[string]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	for name, body := range extra {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))})
		_, _ = tw.Write([]byte(body))
	}
	if manifest != nil {
		raw, _ := json.Marshal(manifest)
		_ = tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o644, Size: int64(len(raw))})
		_, _ = tw.Write(raw)
	}
	_ = tw.Close()
	return path
}

type entry struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

func TestInspectImageTarReadsTagsWithoutLoading(t *testing.T) {
	p := writeImageTar(t, []entry{{Config: "c.json", RepoTags: []string{"computehive-job:123"}}}, map[string]string{"blobs/x": strings.Repeat("a", 4096)})
	info, err := InspectImageTar(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.RepoTags) != 1 || info.RepoTags[0] != "computehive-job:123" {
		t.Fatalf("tags %v", info.RepoTags)
	}
	if err := info.AllowedTags([]string{"computehive-job:"}, "computehive-job:123"); err != nil {
		t.Fatalf("own namespace must be allowed: %v", err)
	}
}

func TestAllowedTagsRefusesArchivesThatRetagTheContributorsImages(t *testing.T) {
	prefixes := []string{"computehive-job:", "computehive/"}
	for name, tags := range map[string][]string{
		"hijack ubuntu":          {"ubuntu:latest"},
		"hijack alpine":          {"alpine:3.21"},
		"own tag plus a hijack":  {"computehive-job:1", "ubuntu:24.04"},
		"registry impersonation": {"docker.io/library/debian:12"},
		"prefix lookalike":       {"computehive-jobs:1"},
	} {
		info := ImageTar{RepoTags: tags}
		if err := info.AllowedTags(prefixes, tags[0]); err == nil {
			t.Errorf("%s: archive with tags %v was accepted", name, tags)
		}
	}
	if err := (ImageTar{}).AllowedTags(prefixes, "computehive-job:1"); err == nil {
		t.Error("an untagged archive must be refused")
	}
	if err := (ImageTar{RepoTags: []string{"computehive-job:1"}}).AllowedTags(prefixes, "computehive-job:2"); err == nil {
		t.Error("an archive that lacks the job's image_ref must be refused")
	}
}

func TestInspectImageTarRejectsGarbage(t *testing.T) {
	if _, err := InspectImageTar(filepath.Join(t.TempDir(), "missing.tar")); err == nil {
		t.Error("missing file")
	}
	bad := filepath.Join(t.TempDir(), "bad.tar")
	_ = os.WriteFile(bad, []byte("this is not a tar archive at all, really"), 0o600)
	if _, err := InspectImageTar(bad); err == nil {
		t.Error("non-tar input must be rejected")
	}
	if _, err := InspectImageTar(writeImageTar(t, nil, map[string]string{"x": "y"})); err == nil {
		t.Error("an archive without manifest.json must be rejected")
	}
	if _, err := InspectImageTar(writeImageTar(t, "not-a-list", nil)); err == nil {
		t.Error("a malformed manifest must be rejected")
	}
}
