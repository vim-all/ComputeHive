package grpcserver

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"coordinator/internal/config"
	"coordinator/internal/metrics"
	"coordinator/internal/queue"
	"coordinator/internal/scheduler"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scanPending is how many queue-head jobs the scheduler looks at per request. It is deliberately
// bounded: a worker asking for work must get an answer in milliseconds even with a huge backlog.
const scanPending = 50

// snapshot is one consistent view of the cluster and the head of the queue. It is immutable once published:
// updates replace it with a modified copy, so a request that is in the middle of a decision is never disturbed.
type snapshot struct {
	workers     []queue.WorkerState
	pending     []queue.PendingJob
	userRunning map[string]int64
	nowMs       int64
	at          time.Time
	decisions   *sync.Map // job id -> scheduler.Decision, valid only for THIS snapshot
}

// placer turns the scheduler's pure decisions into claims.
//
// Reading the whole cluster and the first jobs of the queue costs a few dozen Redis commands, and Redis runs
// commands one at a time. Doing that on every poll from every idle worker (a worker that is not the best fit for
// the head job gets nothing, and asks again 15-500 ms later) measurably capped throughput: with eight workers the
// control plane spent about 177 Redis commands per job. So the view is cached for SchedSnapshotTTL, shared by all
// pollers, updated in place when a claim succeeds (the job leaves the queue, its worker becomes busy), and the
// per-job scoring is memoised per snapshot generation. Staleness can only make a placement suboptimal: the claim
// script re-checks every safety rule atomically.
type placer struct {
	q   *queue.Queue
	cfg scheduler.Config
	ttl time.Duration

	metrics *metrics.Metrics

	mu   sync.Mutex
	snap *snapshot
	held map[string]heldJob // jobs the cache marked running, so a completion can undo exactly that
}

type heldJob struct {
	user  string
	memMB int64
}

func newPlacer(q *queue.Queue, c config.Config) *placer {
	sc := scheduler.DefaultConfig()
	sc.Patience = c.SchedPatience
	sc.UserLimit = c.UserLimit
	sc.LimitMemPct = c.LimitRAMPct
	sc.RequireUsage = c.RequireUsage
	return &placer{q: q, cfg: sc, ttl: c.SchedSnapshotTTL, held: map[string]heldJob{}}
}

// view returns the current snapshot, refreshing it from Redis when it is older than the TTL.
func (p *placer) view(ctx context.Context) (*snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snap != nil && time.Since(p.snap.at) < p.ttl {
		if len(p.snap.pending) > 0 {
			return p.snap, nil
		}
		// An empty queue is cheap to re-read (one command) and must not hide a job that was just submitted.
		pending, err := p.q.PendingJobs(ctx, scanPending)
		if err != nil {
			return nil, err
		}
		if len(pending) == 0 {
			return p.snap, nil
		}
	}
	ws, err := p.q.WorkerStates(ctx)
	if err != nil {
		return nil, err
	}
	pending, err := p.q.PendingJobs(ctx, scanPending)
	if err != nil {
		return nil, err
	}
	now, err := p.q.Now(ctx)
	if err != nil {
		return nil, err
	}
	users := map[string]int64{}
	if p.cfg.UserLimit > 0 {
		for _, j := range pending {
			if u := j.Spec.User; u != "" {
				if _, seen := users[u]; !seen {
					if users[u], err = p.q.UserRunning(ctx, u); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	p.snap = &snapshot{workers: ws, pending: pending, userRunning: users, nowMs: now, at: time.Now(), decisions: &sync.Map{}}
	return p.snap, nil
}

// decide scores job j against the snapshot's workers, once per snapshot generation.
func (p *placer) decide(snap *snapshot, j queue.PendingJob) scheduler.Decision {
	if v, ok := snap.decisions.Load(j.ID); ok {
		return v.(scheduler.Decision)
	}
	elapsed := time.Since(snap.at).Milliseconds()
	d := scheduler.Evaluate(j, snap.workers, snap.userRunning[j.Spec.User], snap.nowMs+elapsed, p.cfg)
	snap.decisions.Store(j.ID, d)
	return d
}

// consumed records, in the cached snapshot, that jobID was just assigned to workerID: the job leaves the queue
// head and the worker becomes busy, so the next poller sees the cluster as it now is without another Redis round
// trip. It is idempotent: if a refresh already reflects the claim, nothing changes.
func (p *placer) consumed(workerID, jobID, user string, memMB int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.snap
	if cur == nil {
		return
	}
	next := *cur
	next.decisions = &sync.Map{} // the old scores no longer describe the cluster
	next.pending = make([]queue.PendingJob, 0, len(cur.pending))
	for _, j := range cur.pending {
		if j.ID != jobID {
			next.pending = append(next.pending, j)
		}
	}
	next.workers = append([]queue.WorkerState(nil), cur.workers...)
	for i, w := range next.workers {
		if w.ID != workerID {
			continue
		}
		already := false
		for _, r := range w.Running {
			already = already || r == jobID
		}
		if !already {
			next.workers[i].Running = append(append([]string(nil), w.Running...), jobID)
			next.workers[i].DeclaredRunningMemMB += memMB
			if len(p.held) > 100_000 { // jobs reaped or cancelled elsewhere are never released; bound the bookkeeping
				p.held = map[string]heldJob{}
			}
			p.held[jobID] = heldJob{user: user, memMB: memMB}
			if user != "" && p.cfg.UserLimit > 0 {
				next.userRunning = make(map[string]int64, len(cur.userRunning)+1)
				for k, v := range cur.userRunning {
					next.userRunning[k] = v
				}
				next.userRunning[user]++
			}
		}
	}
	if len(next.pending) == 0 {
		next.at = time.Time{} // the cached head is used up: read the queue again on the next request
	}
	p.snap = &next
}

// released records, in the cached snapshot, that jobID is no longer running on workerID (it finished, failed or was
// handed back through this coordinator), so the worker's next request sees the room it has.
func (p *placer) released(workerID, jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.held[jobID]
	delete(p.held, jobID)
	cur := p.snap
	if cur == nil || !ok {
		return
	}
	next := *cur
	next.decisions = &sync.Map{}
	next.workers = append([]queue.WorkerState(nil), cur.workers...)
	for i, w := range next.workers {
		if w.ID != workerID {
			continue
		}
		running := make([]string, 0, len(w.Running))
		for _, r := range w.Running {
			if r != jobID {
				running = append(running, r)
			}
		}
		if len(running) != len(w.Running) {
			next.workers[i].Running = running
			next.workers[i].DeclaredRunningMemMB = max(0, w.DeclaredRunningMemMB-h.memMB)
			if h.user != "" && p.cfg.UserLimit > 0 && cur.userRunning[h.user] > 0 {
				next.userRunning = make(map[string]int64, len(cur.userRunning))
				for k, v := range cur.userRunning {
					next.userRunning[k] = v
				}
				next.userRunning[h.user]--
			}
		}
	}
	p.snap = &next
}

// dropPending removes a job the claim script says is no longer claimable from the cached head of the queue.
func (p *placer) dropPending(jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snap == nil {
		return
	}
	next := *p.snap
	next.pending = make([]queue.PendingJob, 0, len(p.snap.pending))
	for _, j := range p.snap.pending {
		if j.ID != jobID {
			next.pending = append(next.pending, j)
		}
	}
	next.decisions = &sync.Map{}
	p.snap = &next
}

// recordedCandidate and recordedDecision are the stored (JSON) form of a placement decision.
type recordedCandidate struct {
	WorkerID string             `json:"worker_id"`
	Eligible bool               `json:"eligible"`
	Busy     bool               `json:"busy,omitempty"`
	Reasons  []string           `json:"reasons,omitempty"`
	Score    float64            `json:"score,omitempty"`
	Parts    map[string]float64 `json:"parts,omitempty"`
}

type recordedDecision struct {
	Chosen     string              `json:"chosen"`
	Summary    string              `json:"summary"`
	AtUnix     int64               `json:"at_unix"`
	WaitedMs   int64               `json:"waited_ms"`
	Overridden bool                `json:"patience_override,omitempty"`
	Candidates []recordedCandidate `json:"candidates"`
	Weights    map[string]float64  `json:"weights"`
}

func (p *placer) record(d scheduler.Decision, chosen string, waitedMs int64) recordedDecision {
	r := recordedDecision{Chosen: chosen, Summary: d.Summary(), AtUnix: d.At.Unix(), WaitedMs: waitedMs, Weights: p.cfg.Weights.Map()}
	if chosen != d.Chosen { // the best-fit worker was not the one that asked: patience let another take it
		r.Overridden = true
		r.Summary = chosen + " took the job after it waited past the patience window (" + p.cfg.Patience.String() +
			"); the best-fit worker was " + orNone(d.Chosen)
	}
	for _, c := range d.Candidates {
		r.Candidates = append(r.Candidates, recordedCandidate{WorkerID: c.WorkerID, Eligible: c.Eligible, Busy: c.Busy, Reasons: c.Reasons, Score: c.Total, Parts: c.Parts})
	}
	return r
}

func orNone(s string) string {
	if s == "" {
		return "(none idle)"
	}
	return s
}

// claimFor finds the job worker should run and claims it atomically. (nil, reason, nil) means there is
// nothing suitable for this worker right now. It records how long the decision took and how long the
// assigned job had waited.
func (p *placer) claimFor(ctx context.Context, workerID string) (*queue.Claimed, string, error) {
	start := time.Now()
	claimed, reason, waited, err := p.pick(ctx, workerID)
	if err == nil {
		if claimed != nil {
			p.metrics.JobAssigned(waited, time.Since(start))
		} else {
			p.metrics.NoJob(time.Since(start))
		}
	}
	return claimed, reason, err
}

func (p *placer) pick(ctx context.Context, workerID string) (*queue.Claimed, string, time.Duration, error) {
	c, r, w, stale, err := p.pickOnce(ctx, workerID)
	if stale { // the worker is asking, so it has room: a cached view that says otherwise predates a completion
		p.invalidate()
		c, r, w, _, err = p.pickOnce(ctx, workerID)
	}
	return c, r, w, err
}

// invalidate makes the next view() read Redis again.
func (p *placer) invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snap != nil {
		next := *p.snap
		next.at = time.Time{}
		p.snap = &next
	}
}

// pickOnce decides from the current snapshot. stale is true when it found nothing to offer while the cached view
// still shows the asking worker busy, which means the view may be out of date.
func (p *placer) pickOnce(ctx context.Context, workerID string) (claimed *queue.Claimed, reason string, waited time.Duration, stale bool, err error) {
	snap, err := p.view(ctx)
	if err != nil {
		return nil, "", 0, false, err
	}
	known, busy := false, false
	for _, w := range snap.workers {
		if w.ID == workerID {
			known, busy = true, len(w.Running) > 0
			break
		}
	}
	if !known { // the snapshot may predate its registration: the claim script gives the authoritative answer
		c, r, err := p.q.Claim(ctx, workerID)
		return c, r, 0, false, err
	}
	elapsed := time.Since(snap.at)
	nowMs := snap.nowMs + elapsed.Milliseconds()
	for _, j := range snap.pending {
		if j.NotBeforeMs > nowMs { // still backing off after a failure
			continue
		}
		d := p.decide(snap, j)
		jobWaited := time.Duration(j.WaitMs)*time.Millisecond + elapsed
		if !d.Offer(workerID, jobWaited, p.cfg) {
			continue
		}
		cl, why, err := p.q.ClaimJob(ctx, workerID, j.ID)
		if err != nil {
			return nil, "", 0, false, err
		}
		if cl == nil {
			switch why {
			case "unknown_worker":
				return nil, why, 0, false, nil
			case "empty": // no longer pending (taken, cancelled) or not claimable by this worker: stop offering it from the cache
				p.dropPending(j.ID)
			}
			continue // lost a race for this job, or a safety rule the snapshot did not see: try the next one
		}
		mem := int64(cl.Spec.MemoryMB)
		if mem == 0 {
			mem = p.cfg.DefaultJobMemMB
		}
		p.consumed(workerID, j.ID, cl.Spec.User, mem)
		if b, err := json.Marshal(p.record(d, workerID, jobWaited.Milliseconds())); err == nil {
			_ = p.q.RecordDecision(ctx, j.ID, string(b)) // best effort: losing the explanation must not lose the job
		}
		return cl, "", jobWaited, false, nil
	}
	return nil, "empty", 0, busy, nil
}

func toPBEvals(cs []recordedCandidate, chosen string) []*pb.WorkerEvaluation {
	out := make([]*pb.WorkerEvaluation, 0, len(cs))
	for _, c := range cs {
		out = append(out, &pb.WorkerEvaluation{WorkerId: &pb.WorkerID{Value: c.WorkerID}, Eligible: c.Eligible, Reasons: c.Reasons,
			Score: c.Score, Parts: c.Parts, Chosen: c.WorkerID == chosen, Busy: c.Busy})
	}
	return out
}

// ExplainJob shows why a job was placed where it was, or, for a job still waiting, a live evaluation
// of every worker against it.
func (s *Server) ExplainJob(ctx context.Context, req *pb.ExplainJobRequest) (*pb.ExplainJobResponse, error) {
	id := req.GetJobId().GetValue()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	job, err := s.queue.Get(ctx, id)
	if err != nil {
		return nil, internal("get job", err)
	}
	if job == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	resp := &pb.ExplainJobResponse{JobId: &pb.JobID{Value: id}, Status: stateToPB(job.State), AssignedWorkerId: job.Worker}

	raw, err := s.queue.JobDecision(ctx, id)
	if err != nil {
		return nil, internal("read decision", err)
	}
	if raw != "" {
		var rec recordedDecision
		if json.Unmarshal([]byte(raw), &rec) == nil {
			resp.Recorded, resp.Summary, resp.EvaluatedAtUnix, resp.Weights = true, rec.Summary, rec.AtUnix, rec.Weights
			resp.Evaluations = toPBEvals(rec.Candidates, rec.Chosen)
			return resp, nil
		}
	}
	if job.State != queue.StatePending && job.State != queue.StateRequeued {
		resp.Summary = "no placement decision was recorded for this job (it is " + job.State + ")"
		return resp, nil
	}

	// Still waiting: evaluate it live against fresh data (an operator is asking, not a hot path).
	ws, err := s.queue.WorkerStates(ctx)
	if err != nil {
		return nil, internal("read workers", err)
	}
	now, err := s.queue.Now(ctx)
	if err != nil {
		return nil, internal("read time", err)
	}
	var ur int64
	if job.Spec.User != "" {
		ur, _ = s.queue.UserRunning(ctx, job.Spec.User)
	}
	dec := scheduler.Evaluate(queue.PendingJob{Job: *job}, ws, ur, now, s.placer.cfg)
	rec := s.placer.record(dec, dec.Chosen, 0)
	resp.EvaluatedAtUnix, resp.Summary, resp.Weights = now/1000, dec.Summary(), rec.Weights
	resp.Evaluations = toPBEvals(rec.Candidates, dec.Chosen)
	return resp, nil
}
