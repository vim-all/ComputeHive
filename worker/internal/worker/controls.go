package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/control"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
	"github.com/vim-all/ComputeHive/worker/internal/executor"
	"github.com/vim-all/ComputeHive/worker/internal/history"
	"github.com/vim-all/ComputeHive/worker/internal/policy"
)

// This file is the contributor's side of the agent: the local policy (donation, schedule, idle,
// network, images), the pause / kill-switch controls, and the record of what ran. The coordinator
// cannot override any of it: the checks run here, on the contributor's machine.

var (
	errPaused = errors.New("paused by the contributor")
	errKilled = errors.New("kill switch engaged by the contributor")
)

type policyAPI interface {
	Get() policy.Policy
	Update(func(*policy.Policy)) error
}

type historyAPI interface {
	Append(history.Entry) error
	Recent(n int) ([]history.Entry, error)
}

type sandboxAPI interface {
	Configure(func(*executor.Sandbox))
	SetStopGrace(time.Duration)
}

// WithPolicy installs the contributor's policy.
func (a *Agent) WithPolicy(p policyAPI) *Agent { a.pol = p; return a }

// WithHistory installs the job history.
func (a *Agent) WithHistory(h historyAPI) *Agent { a.hist = h; return a }

// WithSandbox lets the agent apply policy changes (disk quota, image rules) to the executor.
func (a *Agent) WithSandbox(s sandboxAPI) *Agent { a.sandbox = s; return a }

// WithIsolation sets the isolation level the worker advertises ("hardened-runc" or "gvisor").
func (a *Agent) WithIsolation(level string) *Agent { a.isolation = level; return a }

// WithStop gives the kill switch a way to shut the whole worker down.
func (a *Agent) WithStop(stop context.CancelFunc) *Agent { a.stopRoot = stop; return a }

func (a *Agent) currentPolicy() policy.Policy {
	if a.pol == nil {
		return policy.Default()
	}
	return a.pol.Get()
}

// admission decides whether a new job may be taken right now: the contributor's pause, schedule,
// idle rule and isolation requirement.
func (a *Agent) admission() policy.Admission {
	pol := a.currentPolicy()
	adm := pol.Admit(time.Now(), a.health.Status().Metrics.CPUPercent)
	if adm.Allowed && pol.RequireGVisor && a.isolation != "gvisor" {
		return policy.Admission{Reason: "gvisor_required"}
	}
	return adm
}

func (a *Agent) noteNotTaking(reason string) {
	a.mu.Lock()
	changed := a.notTaking != reason
	a.notTaking = reason
	a.mu.Unlock()
	if changed && reason != "" {
		a.logger.Info("not taking new jobs", "event", "intake_paused_by_policy", "reason", reason)
	}
	if changed && reason == "" {
		a.logger.Info("taking jobs again", "event", "intake_resumed")
	}
}

// donated is the CPU and memory this worker advertises: the machine's, capped by the contributor.
func (a *Agent) donated(cpus int, memMB int64) (int, int64) {
	pol := a.currentPolicy()
	if pol.MaxCPUs > 0 {
		cpus = min(cpus, max(int(pol.MaxCPUs), 1))
	}
	if pol.MaxMemoryMB > 0 {
		memMB = min(memMB, pol.MaxMemoryMB)
	}
	return cpus, memMB
}

// policyViolation explains why this worker must not run job, or "" if it may. The coordinator
// normally never sends such a job (it filters on the advertised capabilities); this is the
// contributor-side backstop that does not trust the coordinator.
func policyViolation(pol policy.Policy, job domain.Job) string {
	if pol.MaxCPUs > 0 && job.CPUCores > pol.MaxCPUs {
		return fmt.Sprintf("job wants %.1f CPUs but this contributor donates at most %.1f", job.CPUCores, pol.MaxCPUs)
	}
	if pol.MaxMemoryMB > 0 && job.MemoryMB > pol.MaxMemoryMB {
		return fmt.Sprintf("job wants %d MB but this contributor donates at most %d MB", job.MemoryMB, pol.MaxMemoryMB)
	}
	for _, h := range job.NetworkHosts {
		if !pol.HostAllowed(h) {
			return fmt.Sprintf("network access to %q is not permitted by this contributor's policy", h)
		}
	}
	return ""
}

// applyPolicy pushes the policy's sandbox-related settings to the executor before a job starts.
func (a *Agent) applyPolicy(pol policy.Policy) {
	if a.sandbox == nil {
		return
	}
	a.sandbox.Configure(func(sb *executor.Sandbox) {
		if pol.MaxJobDiskMB > 0 {
			sb.OutputMaxBytes = pol.MaxJobDiskMB << 20
		}
		sb.Images = executor.ImagePolicy{Allow: pol.ImageAllow, Deny: pol.ImageDeny}
	})
}

// record appends to the contributor-visible history of what ran.
func (a *Agent) record(job domain.Job, started time.Time, status string, exit int, reason, errText string) {
	if a.hist == nil {
		return
	}
	if len(errText) > 200 {
		errText = errText[:200] + "..."
	}
	e := history.Entry{
		JobID: job.ID, Attempt: job.Attempt, Image: job.ImageRef, Command: history.Summarize(job.Command),
		EnvNames: history.SortedNames(job.Env), ArtifactSHA256: job.ArtifactSHA256, CPUs: job.CPUCores, MemoryMB: job.MemoryMB,
		NetworkHosts: job.NetworkHosts, Isolation: a.isolation, StartedAt: started, FinishedAt: time.Now().UTC(),
		Status: status, ExitCode: exit, FailureReason: reason, Error: errText,
	}
	if err := a.hist.Append(e); err != nil {
		a.logger.Warn("could not record job history", "error", err)
	}
}

// evictCurrent aborts the running job with cause; the job is handed back and requeued, not blamed.
func (a *Agent) evictCurrent(cause error) {
	a.mu.RLock()
	abort := a.abortCurrent
	a.mu.RUnlock()
	if abort != nil {
		abort(cause)
	}
}

func isEvictCause(err error) bool {
	return errors.Is(err, errPressureEvict) || errors.Is(err, errPaused) || errors.Is(err, errKilled)
}

// ---- control.Backend ----

var _ control.Backend = (*Agent)(nil)

func (a *Agent) Status() control.Status {
	st := a.health.Status()
	_, job, _, _, _, _ := a.state()
	a.mu.RLock()
	notTaking, killed := a.notTaking, a.killed.Load()
	a.mu.RUnlock()
	pol := a.currentPolicy()
	state := "ready"
	switch {
	case killed:
		state = "killed"
	case pol.Paused:
		state = "paused"
	case st.Overloaded:
		state = "overloaded"
		notTaking = strings.Join(st.Reasons, ",")
	case job != "":
		state = "running"
	case notTaking != "":
		state = "waiting"
	}
	mx := st.Metrics
	return control.Status{
		WorkerID: a.cfg.WorkerID, Version: a.cfg.Version, Isolation: a.isolation, State: state, NotTaking: notTaking,
		CurrentJob: job, Uptime: time.Since(a.started).Round(time.Second).String(), Policy: pol, Metrics: &mx,
	}
}

func (a *Agent) Policy() policy.Policy { return a.currentPolicy() }

func (a *Agent) SetPolicy(p policy.Policy) error {
	if a.pol == nil {
		return errors.New("no policy store configured")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	return a.pol.Update(func(cur *policy.Policy) { *cur = p })
}

func (a *Agent) Pause(now bool) error {
	if a.pol == nil {
		return errors.New("no policy store configured")
	}
	if err := a.pol.Update(func(p *policy.Policy) { p.Paused = true }); err != nil {
		return err
	}
	a.logger.Warn("paused by the contributor", "event", "paused", "evict_running_job", now)
	if now {
		a.evictCurrent(errPaused)
	}
	return nil
}

func (a *Agent) Resume() error {
	if a.pol == nil {
		return errors.New("no policy store configured")
	}
	a.logger.Info("resumed by the contributor", "event", "resumed")
	return a.pol.Update(func(p *policy.Policy) { p.Paused = false })
}

// Kill is the kill switch: stop the running job at once (no grace period), hand it back to the
// cluster, and shut the worker down.
func (a *Agent) Kill() error {
	if !a.killed.CompareAndSwap(false, true) {
		return nil
	}
	a.logger.Warn("KILL SWITCH engaged by the contributor", "event", "kill_switch")
	if a.sandbox != nil {
		a.sandbox.SetStopGrace(0)
	}
	a.evictCurrent(errKilled)
	go func() {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, job, _, _, _, _ := a.state(); job == "" {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if a.stopRoot != nil {
			a.stopRoot()
		}
	}()
	return nil
}

func (a *Agent) History(n int) ([]history.Entry, error) {
	if a.hist == nil {
		return nil, nil
	}
	return a.hist.Recent(n)
}
