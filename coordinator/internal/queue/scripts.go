package queue

import "github.com/redis/go-redis/v9"

// Every state transition is a single Lua script, so Redis executes it atomically:
// there is no window in which a job is "popped but not yet recorded" (the
// failure mode of the original LPOP-then-write design), and no two callers can
// observe the same pending job.
//
// Keys (single-node Redis/Valkey; scripts address job keys dynamically, so a
// Redis Cluster deployment would need hash tags):
//
//	ch:seq               INCR counter: the FIFO sequence number
//	ch:pending           ZSET  jobID -> seq        (PENDING and REQUEUED jobs; lowest seq first)
//	ch:jobs              ZSET  jobID -> seq        (every job, for listing)
//	ch:leases            ZSET  jobID -> deadline ms (ASSIGNED/RUNNING jobs)
//	ch:workers           ZSET  workerID -> last heartbeat ms
//	ch:worker:<id>       HASH  worker capabilities / flags
//	ch:wrun:<id>         SET   jobs currently leased to a worker
//	ch:job:<id>          HASH  job state (state, seq, attempt, token, worker, ...)
//	ch:result:<id>       STRING JSON result of the committed attempt
//	ch:dead              ZSET  jobID -> time it died (dead-letter set)
//	ch:counts            HASH  state -> number of jobs in that state
//
// All timestamps come from Redis TIME inside the script, so the clock authority is
// the datastore rather than any one coordinator replica.
const prelude = `
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function setstate(jk, new)
  local old = redis.call('HGET', jk, 'state')
  if old then redis.call('HINCRBY', 'ch:counts', old, -1) end
  redis.call('HSET', jk, 'state', new)
  redis.call('HINCRBY', 'ch:counts', new, 1)
end

local function is_terminal(st)
  return st == 'SUCCEEDED' or st == 'FAILED' or st == 'DEAD' or st == 'CANCELED'
end

-- Drop the lease: remove from the worker's running set and the lease index.
local function release(id, jk)
  local w = redis.call('HGET', jk, 'worker')
  if w and w ~= '' then redis.call('SREM', 'ch:wrun:' .. w, id) end
  redis.call('ZREM', 'ch:leases', id)
  redis.call('HSET', jk, 'worker', '', 'token', '')
end

-- Fail the current attempt of an ASSIGNED/RUNNING job. If attempts remain the job
-- goes back into ch:pending under its ORIGINAL sequence number (so it keeps its
-- FIFO place) after an exponential backoff; otherwise it is parked as DEAD.
-- count=false (evictions) does not burn an attempt.
local function requeue(id, jk, reason, count)
  release(id, jk)
  local now = now_ms()
  local attempt = tonumber(redis.call('HGET', jk, 'attempt'))
  if not count then
    attempt = attempt - 1
    redis.call('HSET', jk, 'attempt', attempt)
  end
  redis.call('HSET', jk, 'last_failure', reason, 'last_failure_ms', now)
  local max = tonumber(redis.call('HGET', jk, 'max_attempts'))
  if count and attempt >= max then
    setstate(jk, 'DEAD')
    redis.call('HSET', jk, 'finished_ms', now)
    redis.call('ZADD', 'ch:dead', now, id)
    return 'DEAD'
  end
  local backoff = 0
  if count then
    local base = tonumber(redis.call('HGET', jk, 'backoff_base'))
    local cap = tonumber(redis.call('HGET', jk, 'backoff_max'))
    backoff = math.min(math.floor(base * (2 ^ (attempt - 1))), cap)
  end
  redis.call('HSET', jk, 'not_before', now + backoff)
  setstate(jk, 'REQUEUED')
  redis.call('ZADD', 'ch:pending', tonumber(redis.call('HGET', jk, 'seq')), id)
  return 'REQUEUED'
end
`

// enqueue: ARGV id, max_attempts, req_cpu, req_mem_mb, req_gpu, timeout_s,
// backoff_base_ms, backoff_max_ms, user, verify, spec_json.
// Idempotent: re-enqueueing an existing id is a no-op that returns its seq.
var enqueueScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
if redis.call('EXISTS', jk) == 1 then
  return {'EXISTS', redis.call('HGET', jk, 'seq')}
end
local seq = redis.call('INCR', 'ch:seq')
local now = now_ms()
redis.call('HSET', jk,
  'id', id, 'state', 'PENDING', 'seq', seq, 'attempt', 0, 'max_attempts', ARGV[2],
  'req_cpu', ARGV[3], 'req_mem', ARGV[4], 'req_gpu', ARGV[5], 'timeout_s', ARGV[6],
  'backoff_base', ARGV[7], 'backoff_max', ARGV[8], 'user', ARGV[9], 'verify', ARGV[10],
  'spec', ARGV[11], 'created_ms', now, 'not_before', 0, 'worker', '', 'token', '')
redis.call('ZADD', 'ch:pending', seq, id)
redis.call('ZADD', 'ch:jobs', seq, id)
redis.call('HINCRBY', 'ch:counts', 'PENDING', 1)
return {'OK', tostring(seq)}
`)

// claim: ARGV worker_id, new_token, dead_after_ms, default_timeout_ms, lease_grace_ms, scan_limit.
// Moves the lowest-sequence eligible job from pending to ASSIGNED-for-this-worker,
// in one step, or returns why nothing was assigned.
var claimScript = redis.NewScript(prelude + `
local w = ARGV[1]
local token = ARGV[2]
local dead_after = tonumber(ARGV[3])
local default_timeout = tonumber(ARGV[4])
local grace = tonumber(ARGV[5])
local limit = tonumber(ARGV[6])
local now = now_ms()

local hb = redis.call('ZSCORE', 'ch:workers', w)
if not hb then return {'REJECT', 'unknown_worker'} end
if now - tonumber(hb) > dead_after then return {'REJECT', 'stale_heartbeat'} end

local wk = 'ch:worker:' .. w
local f = redis.call('HMGET', wk, 'overloaded', 'draining', 'max_jobs', 'cpu', 'mem_mb', 'gpu')
if f[1] == '1' then return {'REJECT', 'overloaded'} end
if f[2] == '1' then return {'REJECT', 'draining'} end
local max_jobs = tonumber(f[3]) or 1
if redis.call('SCARD', 'ch:wrun:' .. w) >= max_jobs then return {'REJECT', 'at_capacity'} end
local wcpu, wmem, wgpu = tonumber(f[4]) or 0, tonumber(f[5]) or 0, tonumber(f[6]) or 0

local ids = redis.call('ZRANGE', 'ch:pending', 0, limit - 1)
for _, id in ipairs(ids) do
  local jk = 'ch:job:' .. id
  local j = redis.call('HMGET', jk, 'state', 'not_before', 'req_cpu', 'req_mem', 'req_gpu', 'timeout_s', 'attempt', 'spec')
  local st = j[1]
  if st ~= 'PENDING' and st ~= 'REQUEUED' then
    redis.call('ZREM', 'ch:pending', id) -- defensive: never leave a non-pending id in the queue
  elseif tonumber(j[2]) <= now
     and tonumber(j[3]) <= wcpu and tonumber(j[4]) <= wmem and tonumber(j[5]) <= wgpu then
    local attempt = tonumber(j[7]) + 1
    local timeout = tonumber(j[6]) * 1000
    if timeout <= 0 then timeout = default_timeout end
    redis.call('ZREM', 'ch:pending', id)
    redis.call('HSET', jk, 'attempt', attempt, 'worker', w, 'token', token, 'assigned_ms', now)
    setstate(jk, 'ASSIGNED')
    redis.call('ZADD', 'ch:leases', now + timeout + grace, id)
    redis.call('SADD', 'ch:wrun:' .. w, id)
    return {'OK', id, tostring(attempt), j[8], token}
  end
end
return {'NONE'}
`)

// markRunning: ARGV id, token.
var markRunningScript = redis.NewScript(prelude + `
local jk = 'ch:job:' .. ARGV[1]
local st = redis.call('HGET', jk, 'state')
if not st then return 'UNKNOWN' end
if is_terminal(st) then return 'DUP' end
if st == 'PENDING' or st == 'REQUEUED' or redis.call('HGET', jk, 'token') ~= ARGV[2] then return 'STALE' end
if st == 'ASSIGNED' then
  setstate(jk, 'RUNNING')
  redis.call('HSET', jk, 'started_ms', now_ms())
end
return 'OK'
`)

// complete: ARGV id, token, outcome (SUCCEEDED|FAILED), result_json.
// The first valid completion wins; everything later is a no-op ("DUP"), and a
// completion from a superseded attempt is discarded ("STALE").
var completeScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
local st = redis.call('HGET', jk, 'state')
if not st then return 'UNKNOWN' end
if is_terminal(st) then return 'DUP' end
if st == 'PENDING' or st == 'REQUEUED' or redis.call('HGET', jk, 'token') ~= ARGV[2] then return 'STALE' end
release(id, jk)
setstate(jk, ARGV[3])
redis.call('HSET', jk, 'finished_ms', now_ms())
redis.call('HINCRBY', jk, 'commits', 1) -- audit counter: must end at exactly 1
redis.call('SET', 'ch:result:' .. id, ARGV[4])
return 'OK'
`)

// fail: ARGV id, token, reason, count_attempt ("1"/"0"). Returns OK:<new state>.
var failScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
local st = redis.call('HGET', jk, 'state')
if not st then return 'UNKNOWN' end
if is_terminal(st) then return 'DUP' end
if st == 'PENDING' or st == 'REQUEUED' or redis.call('HGET', jk, 'token') ~= ARGV[2] then return 'STALE' end
return 'OK:' .. requeue(id, jk, ARGV[3], ARGV[4] == '1')
`)

// cancel: ARGV id.
var cancelScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
local st = redis.call('HGET', jk, 'state')
if not st then return 'UNKNOWN' end
if is_terminal(st) then return 'DUP' end
if st == 'ASSIGNED' or st == 'RUNNING' then
  release(id, jk)
else
  redis.call('ZREM', 'ch:pending', id)
end
setstate(jk, 'CANCELED')
redis.call('HSET', jk, 'finished_ms', now_ms())
return 'OK'
`)

// revive moves a DEAD job back to PENDING with a fresh attempt budget, keeping its sequence.
var reviveScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
if redis.call('HGET', jk, 'state') ~= 'DEAD' then return 'NOT_DEAD' end
redis.call('ZREM', 'ch:dead', id)
redis.call('HSET', jk, 'attempt', 0, 'not_before', 0)
setstate(jk, 'PENDING')
redis.call('ZADD', 'ch:pending', tonumber(redis.call('HGET', jk, 'seq')), id)
return 'OK'
`)

// heartbeat: ARGV worker, cpu, mem_mb, gpu, overloaded, draining, max_jobs, version,
// orphan_grace_ms, running_job_id...
//
// Returns the reported jobs that the worker no longer owns (it must abort them).
//
// It also reconciles the other direction: a lease the coordinator holds for this
// worker that the worker does not report is an orphan, typically because the reply
// to its claim was lost (a crash or network failure between the atomic claim and the
// worker receiving the job). Such a job would otherwise sit leased to a live worker
// until its lease deadline. Leases younger than orphan_grace are left alone, since
// the reply may simply still be in flight. The attempt is not counted: it never ran.
var heartbeatScript = redis.NewScript(prelude + `
local w = ARGV[1]
local grace = tonumber(ARGV[9])
local now = now_ms()
local wk = 'ch:worker:' .. w
if redis.call('EXISTS', wk) == 0 then redis.call('HSET', wk, 'registered_ms', now) end
redis.call('ZADD', 'ch:workers', now, w)
redis.call('HSET', wk, 'cpu', ARGV[2], 'mem_mb', ARGV[3], 'gpu', ARGV[4], 'overloaded', ARGV[5],
  'draining', ARGV[6], 'max_jobs', ARGV[7], 'version', ARGV[8], 'last_hb_ms', now)
local wrun = 'ch:wrun:' .. w
local reported = {}
local cancel = {}
for i = 10, #ARGV do
  reported[ARGV[i]] = true
  if redis.call('SISMEMBER', wrun, ARGV[i]) == 0 then cancel[#cancel + 1] = ARGV[i] end
end
for _, id in ipairs(redis.call('SMEMBERS', wrun)) do
  if not reported[id] then
    local jk = 'ch:job:' .. id
    local st = redis.call('HGET', jk, 'state')
    if st == 'ASSIGNED' or st == 'RUNNING' then
      if now - tonumber(redis.call('HGET', jk, 'assigned_ms')) > grace then
        requeue(id, jk, 'assignment_lost', false)
      end
    else
      redis.call('SREM', wrun, id)
    end
  end
end
return cancel
`)

// reapWorkers: ARGV dead_after_ms, forget_after_ms. Requeues every job leased to a
// worker whose heartbeat is older than dead_after. Returns {jobs requeued, workers affected}.
var reapWorkersScript = redis.NewScript(prelude + `
local dead_after = tonumber(ARGV[1])
local forget_after = tonumber(ARGV[2])
local now = now_ms()
local dead = redis.call('ZRANGEBYSCORE', 'ch:workers', '-inf', now - dead_after)
local jobs, workers = 0, 0
for _, w in ipairs(dead) do
  local ids = redis.call('SMEMBERS', 'ch:wrun:' .. w)
  for _, id in ipairs(ids) do
    local jk = 'ch:job:' .. id
    local st = redis.call('HGET', jk, 'state')
    if st == 'ASSIGNED' or st == 'RUNNING' then
      requeue(id, jk, 'worker_dead', true)
      jobs = jobs + 1
    else
      redis.call('SREM', 'ch:wrun:' .. w, id)
    end
  end
  if #ids > 0 then workers = workers + 1 end
  local last = tonumber(redis.call('ZSCORE', 'ch:workers', w))
  if now - last > forget_after then
    redis.call('ZREM', 'ch:workers', w)
    redis.call('DEL', 'ch:worker:' .. w, 'ch:wrun:' .. w)
  end
end
return {jobs, workers}
`)

// reapLeases: ARGV limit. Requeues jobs whose lease deadline (job timeout + grace)
// passed without a result, e.g. a worker that heartbeats but never finishes.
var reapLeasesScript = redis.NewScript(prelude + `
local now = now_ms()
local ids = redis.call('ZRANGEBYSCORE', 'ch:leases', '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local n = 0
for _, id in ipairs(ids) do
  local jk = 'ch:job:' .. id
  local st = redis.call('HGET', jk, 'state')
  if st == 'ASSIGNED' or st == 'RUNNING' then
    requeue(id, jk, 'lease_expired', true)
    n = n + 1
  else
    redis.call('ZREM', 'ch:leases', id)
  end
end
return n
`)
