package history

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func entry(i int) Entry {
	return Entry{JobID: fmt.Sprintf("job-%03d", i), Image: "computehive-job:1", Command: "echo hi", Status: "succeeded",
		StartedAt: time.Unix(int64(1000+i), 0), FinishedAt: time.Unix(int64(1001+i), 0)}
}

func TestRecentReturnsNewestFirst(t *testing.T) {
	s := Open(filepath.Join(t.TempDir(), "h.jsonl"), 100)
	for i := 0; i < 5; i++ {
		if err := s.Append(entry(i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Recent(3)
	if err != nil || len(got) != 3 {
		t.Fatalf("recent: %v %v", got, err)
	}
	if got[0].JobID != "job-004" || got[2].JobID != "job-002" {
		t.Fatalf("order: %v", got)
	}
	all, _ := s.Recent(0)
	if len(all) != 5 {
		t.Fatalf("0 means all, got %d", len(all))
	}
}

func TestHistoryIsBoundedAndSurvivesRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.jsonl")
	s := Open(path, 10)
	for i := 0; i < 55; i++ {
		_ = s.Append(entry(i))
	}
	all, _ := s.Recent(0)
	if len(all) > 30 { // trimmed once it passes 2x max; never grows without bound
		t.Fatalf("history grew to %d entries with max 10", len(all))
	}
	if all[0].JobID != "job-054" {
		t.Fatalf("newest entry lost: %s", all[0].JobID)
	}
	again := Open(path, 10)
	got, _ := again.Recent(1)
	if len(got) != 1 || got[0].JobID != "job-054" {
		t.Fatalf("history did not survive a restart: %v", got)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("history file mode %v, want 0600", st.Mode().Perm())
	}
}

func TestTornLastLineFromACrashIsSkipped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.jsonl")
	s := Open(path, 10)
	_ = s.Append(entry(1))
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString(`{"job_id":"half-writ`) // power cut mid-write
	f.Close()
	got, err := s.Recent(0)
	if err != nil || len(got) != 1 || got[0].JobID != "job-001" {
		t.Fatalf("recent after torn write: %v %v", got, err)
	}
}

func TestSummariesNeverRecordSecrets(t *testing.T) {
	cmd := Summarize([]string{"python", strings.Repeat("x", 500)})
	if len(cmd) > 210 || !strings.HasSuffix(cmd, "...") {
		t.Fatalf("command not truncated: %d bytes", len(cmd))
	}
	names := SortedNames(map[string]string{"B": "secret-b", "A": "secret-a"})
	if strings.Join(names, ",") != "A,B" {
		t.Fatalf("names %v", names)
	}
	for _, n := range names {
		if strings.Contains(n, "secret") {
			t.Fatal("environment values must never be recorded")
		}
	}
}
