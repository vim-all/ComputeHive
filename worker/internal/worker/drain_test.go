package worker

import (
	"context"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

// runUntilDone starts the agent and returns a channel that yields Run's result.
func runUntilDone(ctx context.Context, r *rig) <-chan error {
	done := make(chan error, 1)
	go func() { done <- r.agent.Run(ctx) }()
	return done
}

func drainJob(id string) *domain.Job {
	return &domain.Job{ID: id, ImageRef: "img", CPUCores: 1, MemoryMB: 64, Attempt: 1}
}

func TestDrainLetsTheRunningJobFinishAndTakesNoMoreWork(t *testing.T) {
	r := newRig(t, drainJob("first"), drainJob("second"))
	r.exec.runFor = 300 * time.Millisecond
	r.mon.Observe(healthy(50))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runUntilDone(ctx, r)

	select {
	case <-r.exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the first job never started")
	}
	r.agent.BeginDrain()

	eventually(t, time.Second, func() bool {
		hb, ok := r.store.lastHeartbeat()
		return ok && hb.Draining
	}, "heartbeats report draining, so the coordinator stops assigning")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a completed drain must exit cleanly, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the worker did not exit after finishing its job")
	}

	res := r.store.resultsCopy()
	if len(res) != 1 || res[0].JobID != "first" || res[0].Status != domain.JobStatusSucceeded {
		t.Fatalf("the running job must complete normally during a drain: %+v", res)
	}
	r.store.mu.Lock()
	left := len(r.store.jobs)
	r.store.mu.Unlock()
	if left != 1 {
		t.Fatalf("the second job must stay in the queue for another worker, %d left", left)
	}
}

func TestDrainWhileIdleExitsPromptly(t *testing.T) {
	r := newRig(t)
	r.mon.Observe(healthy(50))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runUntilDone(ctx, r)
	eventually(t, time.Second, func() bool { return r.store.pulls.Load() > 2 }, "worker polling")

	r.agent.BeginDrain()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("an idle worker did not exit when asked to drain")
	}
}

func TestDrainTimeoutHandsTheJobBackWithoutBurningAnAttempt(t *testing.T) {
	r := newRig(t, drainJob("long"))
	r.mon.Observe(healthy(50)) // the executor runs until its context ends
	ctx, cancel := context.WithCancel(context.Background())
	done := runUntilDone(ctx, r)

	select {
	case <-r.exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the job never started")
	}
	r.agent.BeginDrain()
	time.Sleep(100 * time.Millisecond)
	cancel() // what main does when DRAIN_TIMEOUT expires

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop after the drain timeout")
	}
	eventually(t, 2*time.Second, func() bool { return len(r.store.resultsCopy()) == 1 }, "the unfinished job is reported back")
	got := r.store.resultsCopy()[0]
	if got.Status == domain.JobStatusSucceeded || got.FailureReason != ReasonEvicted {
		t.Fatalf("the job must be handed back as evicted (retried elsewhere, no attempt burned): %+v", got)
	}
}
