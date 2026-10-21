package main

import (
	"bytes"
	"testing"
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
