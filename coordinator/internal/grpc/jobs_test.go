package grpcserver

import (
	"context"
	"strings"
	"testing"

	"coordinator/internal/config"
	pb "coordinator/pkg/pb"
)

func (cl *cluster) listJobs(t *testing.T, req *pb.ListJobsRequest) []*pb.JobSummary {
	t.Helper()
	r, err := cl.c.ListJobs(context.Background(), req)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	return r.GetJobs()
}

func (cl *cluster) status(t *testing.T, id string) *pb.GetJobStatusResponse {
	t.Helper()
	r, err := cl.c.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: &pb.JobID{Value: id}})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	return r
}

// beatWithTails sends a heartbeat that also reports live output for jobs.
func (cl *cluster) beatWithTails(t *testing.T, id string, b box, tails map[string]string) {
	t.Helper()
	b.defaults()
	if _, err := cl.w.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: wid(id), MaxJobs: 1,
		AvailableResources: &pb.ResourceSpec{CpuCores: b.cpu, MemoryMb: b.memMB}, Usage: b.usage(),
		Os: b.osName, Arch: b.arch, LogTails: tails}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
}

func TestListJobsIsNewestFirstFilterableAndHidesVerificationReplicas(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0 })
	cl.join(t, "w", box{cpu: 8, memMB: 16384, memUsedMB: 2000})
	a := cl.submit(t, func(r *pb.SubmitJobRequest) { r.User = "alice" })
	b := cl.submit(t, nil)
	v := cl.submit(t, func(r *pb.SubmitJobRequest) { r.Verify = true }) // creates a hidden second replica
	if got := cl.ask(t, "w"); got != a {
		t.Fatalf("got %q", got)
	}
	cl.finish(t, "w", a)

	all := cl.listJobs(t, &pb.ListJobsRequest{})
	if len(all) != 3 {
		t.Fatalf("3 jobs were submitted (one of them verified); got %d listed: the hidden replica must not appear", len(all))
	}
	if all[0].GetJobId().GetValue() != v || all[1].GetJobId().GetValue() != b || all[2].GetJobId().GetValue() != a {
		t.Fatalf("not newest first: %v %v %v", all[0].GetJobId().GetValue(), all[1].GetJobId().GetValue(), all[2].GetJobId().GetValue())
	}
	if all[2].GetUser() != "alice" || all[2].GetContainerImage() != "alpine:3" || all[2].GetStatus() != pb.Status_STATUS_SUCCEEDED {
		t.Fatalf("summary fields wrong: %+v", all[2])
	}
	if all[2].GetAssignedWorkerId() != "w" || cl.status(t, a).GetAssignedWorkerId() != "w" {
		t.Fatalf("a finished job must still say which worker ran it, got %q", all[2].GetAssignedWorkerId())
	}
	if all[1].GetAssignedWorkerId() != "" {
		t.Fatalf("a job that never ran has no worker, got %q", all[1].GetAssignedWorkerId())
	}
	if got := cl.listJobs(t, &pb.ListJobsRequest{Limit: 2}); len(got) != 2 || got[0].GetJobId().GetValue() != v {
		t.Fatalf("limit 2: %v", got)
	}
	only := cl.listJobs(t, &pb.ListJobsRequest{Status: pb.Status_STATUS_SUCCEEDED})
	if len(only) != 1 || only[0].GetJobId().GetValue() != a {
		t.Fatalf("status filter: %v", only)
	}
	if got := cl.listJobs(t, &pb.ListJobsRequest{Status: pb.Status_STATUS_QUEUED}); len(got) != 2 {
		t.Fatalf("two jobs are still queued, got %d", len(got))
	}
	for _, bad := range []int32{-1, 201} {
		if _, err := cl.c.ListJobs(context.Background(), &pb.ListJobsRequest{Limit: bad}); code(err).String() != "InvalidArgument" {
			t.Errorf("limit %d: %v", bad, err)
		}
	}
}

func TestLiveLogTailIsServedFromStatusAndOnlyFromTheWorkerHoldingTheJob(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0 })
	hw := box{cpu: 8, memMB: 16384, memUsedMB: 2000}
	cl.join(t, "holder", hw)
	cl.join(t, "other", hw)
	job := cl.submit(t, nil)
	if got := cl.ask(t, "holder"); got != job {
		t.Fatalf("got %q", got)
	}

	if tail := cl.status(t, job).GetLogTail(); tail != "" {
		t.Fatalf("no output was reported yet, got %q", tail)
	}
	cl.beatWithTails(t, "holder", hw, map[string]string{job: "step 1\nstep 2\n"})
	if got := cl.status(t, job).GetLogTail(); got != "step 1\nstep 2\n" {
		t.Fatalf("log tail %q", got)
	}

	// A worker that does not hold the job cannot write output onto it.
	cl.beatWithTails(t, "other", hw, map[string]string{job: "FORGED OUTPUT"})
	if got := cl.status(t, job).GetLogTail(); strings.Contains(got, "FORGED") {
		t.Fatalf("a worker without the lease overwrote the log: %q", got)
	}
	// ...nor onto a job that does not exist.
	cl.beatWithTails(t, "holder", hw, map[string]string{"no-such-job": "x"})

	// Newer output replaces older.
	cl.beatWithTails(t, "holder", hw, map[string]string{job: "step 3\n"})
	if got := cl.status(t, job).GetLogTail(); got != "step 3\n" {
		t.Fatalf("log tail %q", got)
	}
}

func TestLogTailIsTruncatedToTheEnd(t *testing.T) {
	cl := startCluster(t, func(c *config.Config) { c.SchedPatience = 0 })
	hw := box{cpu: 8, memMB: 16384, memUsedMB: 2000}
	cl.join(t, "w", hw)
	job := cl.submit(t, nil)
	cl.ask(t, "w")

	big := strings.Repeat("a", 10_000) + "THE-END"
	cl.beatWithTails(t, "w", hw, map[string]string{job: big})
	got := cl.status(t, job).GetLogTail()
	if len(got) != 4096 || !strings.HasSuffix(got, "THE-END") {
		t.Fatalf("expected the last 4096 bytes (ending THE-END), got %d bytes ending %q", len(got), got[max(0, len(got)-10):])
	}
}
