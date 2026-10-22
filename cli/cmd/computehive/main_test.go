package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "coordinator/pkg/pb"
)

func TestRunUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no command", nil, 2},
		{"unknown command", []string{"frobnicate"}, 2},
		{"status needs id", []string{"status"}, 1},
		{"submit needs dir", []string{"submit"}, 1},
		{"worker needs join", []string{"worker"}, 1},
		{"join needs coordinator", []string{"worker", "join"}, 1},
		{"join validates host:port", []string{"worker", "join", "--coordinator", "nohostport"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if got := run(tc.args, &out, &errOut); got != tc.want {
				t.Fatalf("run(%v) = %d, want %d (stderr: %s)", tc.args, got, tc.want, errOut.String())
			}
		})
	}
}

func TestStatusAgainstUnreachableCoordinatorFailsCleanly(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"--coordinator", "127.0.0.1:1", "status", "some-job"}, &out, &errOut)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if errOut.Len() == 0 {
		t.Fatal("expected an error message on stderr")
	}
}

func TestHelpers(t *testing.T) {
	if got := orDash(""); got != "-" {
		t.Errorf("orDash(\"\") = %q", got)
	}
	if got := firstNonEmpty("", "b", "c"); got != "b" {
		t.Errorf("firstNonEmpty = %q", got)
	}
}

func TestDownloadRefusesPathsThatEscapeTheTargetDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{"../escape.txt", "a/../../escape.txt", "/etc/cron.d/x", ""} {
		a := []*pb.OutputArtifact{{RelativePath: rel, Uri: "computehive-output://j/1/x"}}
		err := downloadOutputs(context.Background(), globals{httpURL: "http://127.0.0.1:1"}, a, dir, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "refusing output path") {
			t.Errorf("%q: got %v, want a refusal before any request is made", rel, err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape.txt")); err == nil {
		t.Fatal("a hostile output name wrote outside the directory")
	}
}

func TestDownloadVerifiesTheHashAndRemovesBadFiles(t *testing.T) {
	body := []byte("good bytes")
	sum := sha256.Sum256(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	g := globals{httpURL: srv.URL, token: "tok"}
	dir := t.TempDir()
	ok := []*pb.OutputArtifact{{RelativePath: "sub/r.txt", Uri: "computehive-output://j/1/sub/r.txt", Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body))}}
	var out bytes.Buffer
	if err := downloadOutputs(context.Background(), g, ok, dir, &out); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "sub", "r.txt")); !bytes.Equal(got, body) || !strings.Contains(out.String(), "sha256 verified") {
		t.Fatalf("file %q out %q", got, out.String())
	}
	bad := []*pb.OutputArtifact{{RelativePath: "bad.txt", Uri: "computehive-output://j/1/bad.txt", Sha256: strings.Repeat("0", 64)}}
	if err := downloadOutputs(context.Background(), g, bad, dir, io.Discard); err == nil || !strings.Contains(err.Error(), "failed verification") {
		t.Fatalf("a corrupted download was accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bad.txt")); err == nil {
		t.Fatal("the corrupted file was left on disk")
	}
}
