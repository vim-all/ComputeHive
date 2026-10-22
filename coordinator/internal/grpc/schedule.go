package grpcserver

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"coordinator/internal/config"
	"coordinator/internal/queue"
	"coordinator/internal/scheduler"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scanPending is how many queue-head jobs the scheduler looks at per request. It is deliberately
// bounded: a worker asking for work must get an answer in milliseconds even with a huge backlog.
const scanPending = 50

// placer turns the scheduler's pure decisions into claims. It keeps a very short-lived snapshot of
// the cluster so a burst of idle workers polling at once costs one Redis read, not one each. A stale
// snapshot can only make a placement suboptimal: the claim script re-checks every safety rule.
type placer struct {
	q   *queue.Queue
	cfg scheduler.Config
	ttl time.Duration

	mu     sync.Mutex
	snap   []queue.WorkerState
	snapAt time.Time
	nowMs  int64
}

func newPlacer(q *queue.Queue, c config.Config) *placer {
	sc := scheduler.DefaultConfig()
	sc.Patience = c.SchedPatience
	sc.UserLimit = c.UserLimit
	sc.LimitMemPct = c.LimitRAMPct
	sc.RequireUsage = c.RequireUsage
	return &placer{q: q, cfg: sc, ttl: c.SchedSnapshotTTL}
}

// workers returns the cluster snapshot and the (Redis) time, in ms, it corresponds to.
func (p *placer) workers(ctx context.Context) ([]queue.WorkerState, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snap != nil && time.Since(p.snapAt) < p.ttl {
		return p.snap, p.nowMs + time.Since(p.snapAt).Milliseconds(), nil
	}
	ws, err := p.q.WorkerStates(ctx)
	if err != nil {
		return nil, 0, err
	}
	t, err := p.q.Now(ctx)
	if err != nil {
		return nil, 0, err
	}
	p.snap, p.snapAt, p.nowMs = ws, time.Now(), t
	return ws, t, nil
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
// nothing suitable for this worker right now.
func (p *placer) claimFor(ctx context.Context, workerID string) (*queue.Claimed, string, error) {
	ws, nowMs, err := p.workers(ctx)
	if err != nil {
		return nil, "", err
	}
	known := false
	for _, w := range ws {
		if w.ID == workerID {
			known = true
			break
		}
	}
	if !known { // the snapshot may predate its registration: the claim script gives the authoritative answer
		return p.q.Claim(ctx, workerID)
	}
	pending, err := p.q.PendingJobs(ctx, scanPending)
	if err != nil {
		return nil, "", err
	}
	userRunning := map[string]int64{}
	for _, j := range pending {
		if j.NotBeforeMs > nowMs { // still backing off after a failure
			continue
		}
		var ur int64
		if u := j.Spec.User; u != "" && p.cfg.UserLimit > 0 {
			n, seen := userRunning[u]
			if !seen {
				if n, err = p.q.UserRunning(ctx, u); err != nil {
					return nil, "", err
				}
				userRunning[u] = n
			}
			ur = n
		}
		d := scheduler.Evaluate(j, ws, ur, nowMs, p.cfg)
		if !d.Offer(workerID, time.Duration(j.WaitMs)*time.Millisecond, p.cfg) {
			continue
		}
		claimed, reason, err := p.q.ClaimJob(ctx, workerID, j.ID)
		if err != nil {
			return nil, "", err
		}
		if claimed == nil {
			if reason == "unknown_worker" {
				return nil, reason, nil
			}
			continue // lost a race for this job, or a safety rule the snapshot did not see: try the next one
		}
		if b, err := json.Marshal(p.record(d, workerID, j.WaitMs)); err == nil {
			_ = p.q.RecordDecision(ctx, j.ID, string(b)) // best effort: losing the explanation must not lose the job
		}
		return claimed, "", nil
	}
	return nil, "empty", nil
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
