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
  if w and w ~= '' then
    redis.call('SREM', 'ch:wrun:' .. w, id)
    local u = redis.call('HGET', jk, 'user')
    if u and u ~= '' then
      if redis.call('DECR', 'ch:urun:' .. u) < 0 then redis.call('SET', 'ch:urun:' .. u, 0) end
    end
  end
  redis.call('ZREM', 'ch:leases', id)
  redis.call('HSET', jk, 'worker', '', 'token', '')
end

-- worker_labels is ",k=v,k2=v2,"; required is ",k=v,k2=v2,". Every required label must be present.
local function has_labels(worker_labels, required)
  if not required or required == '' then return true end
  for lab in string.gmatch(required, '[^,]+') do
    if not string.find(worker_labels, ',' .. lab .. ',', 1, true) then return false end
  end
  return true
end

-- Reliability record of a worker: an exponentially decayed count of jobs it completed vs. jobs it
-- failed through its own fault (crash, silence, expired lease, infrastructure error). Half-life 1h.
local function wstat(w, ok)
  if not w or w == '' then return end
  local key = 'ch:wstat:' .. w
  local now = now_ms()
  local v = redis.call('HMGET', key, 'ok', 'fail', 'ts')
  local f = 1
  if v[3] then f = 2 ^ (-(now - tonumber(v[3])) / 3600000) end
  local o, x = (tonumber(v[1]) or 0) * f, (tonumber(v[2]) or 0) * f
  if ok then o = o + 1 else x = x + 1 end
  redis.call('HSET', key, 'ok', o, 'fail', x, 'ts', now)
end

local settle -- defined below (verification pairs); requeue needs it for DEAD replicas

-- Fail the current attempt of an ASSIGNED/RUNNING job. If attempts remain the job
-- goes back into ch:pending under its ORIGINAL sequence number (so it keeps its
-- FIFO place) after an exponential backoff; otherwise it is parked as DEAD.
-- count=false (evictions) does not burn an attempt.
local function requeue(id, jk, reason, count)
  local prev_worker = redis.call('HGET', jk, 'worker')
  release(id, jk)
  if count then wstat(prev_worker, false) end
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
    settle(id, jk)
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

-- Redundant execution ("verify"): a job runs as two replicas that are linked by a 'sibling' field and
-- never run on the same worker. A replica that finishes OK waits in VERIFYING until its sibling has
-- finished too; the pair then both SUCCEED (digests equal) or both FAIL.
local function finish_verified(k, i, state, verdict, reason)
  redis.call('ZREM', 'ch:verifying', i)
  setstate(k, state)
  local now = now_ms()
  redis.call('HSET', k, 'verified', verdict, 'finished_ms', now)
  if reason then redis.call('HSET', k, 'last_failure', reason, 'last_failure_ms', now) end
end

settle = function(id, jk)
  local sib = redis.call('HGET', jk, 'sibling')
  if not sib or sib == '' then return end
  local sk = 'ch:job:' .. sib
  local mine, theirs = redis.call('HGET', jk, 'state'), redis.call('HGET', sk, 'state')
  local function dead(st) return st == 'FAILED' or st == 'DEAD' or st == 'CANCELED' end
  if mine == 'VERIFYING' and theirs == 'VERIFYING' then
    local a, b = redis.call('HGET', jk, 'digest'), redis.call('HGET', sk, 'digest')
    if a and a ~= '' and a == b then
      finish_verified(jk, id, 'SUCCEEDED', 'match')
      finish_verified(sk, sib, 'SUCCEEDED', 'match')
    else
      finish_verified(jk, id, 'FAILED', 'mismatch', 'verification_mismatch')
      finish_verified(sk, sib, 'FAILED', 'mismatch', 'verification_mismatch')
    end
  elseif mine == 'VERIFYING' and dead(theirs) then
    finish_verified(jk, id, 'FAILED', 'unavailable', 'verification_peer_' .. string.lower(theirs))
  elseif theirs == 'VERIFYING' and dead(mine) then
    finish_verified(sk, sib, 'FAILED', 'unavailable', 'verification_peer_' .. string.lower(mine))
  end
end

local function enqueue_one(base, sibling)
  local id = ARGV[base]
  local jk = 'ch:job:' .. id
  if redis.call('EXISTS', jk) == 1 then
    return {'EXISTS', redis.call('HGET', jk, 'seq')}
  end
  local seq = redis.call('INCR', 'ch:seq')
  local now = now_ms()
  redis.call('HSET', jk,
    'id', id, 'state', 'PENDING', 'seq', seq, 'attempt', 0, 'max_attempts', ARGV[base + 1],
    'req_cpu', ARGV[base + 2], 'req_mem', ARGV[base + 3], 'req_gpu', ARGV[base + 4], 'timeout_s', ARGV[base + 5],
    'backoff_base', ARGV[base + 6], 'backoff_max', ARGV[base + 7], 'user', ARGV[base + 8], 'verify', ARGV[base + 9],
    'spec', ARGV[base + 10], 'req_isolation', ARGV[base + 11], 'needs_net', ARGV[base + 12],
    'req_disk', ARGV[base + 13], 'req_os', ARGV[base + 14], 'req_arch', ARGV[base + 15], 'req_labels', ARGV[base + 16],
    'sibling', sibling, 'exclude', '', 'created_ms', now, 'not_before', 0, 'worker', '', 'token', '')
  redis.call('ZADD', 'ch:pending', seq, id)
  redis.call('ZADD', 'ch:jobs', seq, id)
  redis.call('HINCRBY', 'ch:counts', 'PENDING', 1)
  return {'OK', tostring(seq)}
end
`

// enqueue: ARGV id, max_attempts, req_cpu, req_mem_mb, req_gpu, timeout_s,
// backoff_base_ms, backoff_max_ms, user, verify, spec_json, required_isolation, needs_net.
// Idempotent: re-enqueueing an existing id is a no-op that returns its seq.
var enqueueScript = redis.NewScript(prelude + `
return enqueue_one(1, '')
`)

// enqueuePair: ARGV is two enqueue argument blocks of 17 (primary then shadow). Both replicas are
// created in one atomic step, linked to each other, so a crash can never leave half a pair.
var enqueuePairScript = redis.NewScript(prelude + `
local pid, sid = ARGV[1], ARGV[18]
local first = enqueue_one(1, sid)
if first[1] == 'EXISTS' then return first end
enqueue_one(18, pid)
return first
`)

// claim: ARGV worker_id, new_token, dead_after_ms, default_timeout_ms, lease_grace_ms, scan_limit,
// limit_mem_pct, default_job_mem_mb, require_usage, specific_job_id, user_limit.
// Moves the lowest-sequence eligible job from pending to ASSIGNED-for-this-worker,
// in one step, or returns why nothing was assigned. This is where the coordinator
// enforces the resource-safety rules independently of the worker: stale or overloaded
// workers, workers that report no metrics, and jobs whose declared memory would push
// the host past the limit all get nothing.
var claimScript = redis.NewScript(prelude + `
local w = ARGV[1]
local token = ARGV[2]
local dead_after = tonumber(ARGV[3])
local default_timeout = tonumber(ARGV[4])
local grace = tonumber(ARGV[5])
local limit = tonumber(ARGV[6])
local limit_mem = tonumber(ARGV[7])
local default_job_mem = tonumber(ARGV[8])
local require_usage = ARGV[9] == '1'
local specific = ARGV[10]
local user_limit = tonumber(ARGV[11]) or 0
local now = now_ms()

local hb = redis.call('ZSCORE', 'ch:workers', w)
if not hb then return {'REJECT', 'unknown_worker'} end
if now - tonumber(hb) > dead_after then return {'REJECT', 'stale_heartbeat'} end

local wk = 'ch:worker:' .. w
local f = redis.call('HMGET', wk, 'overloaded', 'draining', 'max_jobs', 'cpu', 'mem_mb', 'gpu', 'mem_pct', 'mem_total', 'mem_used', 'jobs_mem', 'isolation', 'allows_net', 'os', 'arch', 'disk_free_mb', 'labels')
if f[1] == '1' then return {'REJECT', 'overloaded'} end
if f[2] == '1' then return {'REJECT', 'draining'} end
if require_usage and not f[7] then return {'REJECT', 'no_metrics'} end
local max_jobs = tonumber(f[3]) or 1
local wrun = 'ch:wrun:' .. w
if redis.call('SCARD', wrun) >= max_jobs then return {'REJECT', 'at_capacity'} end
local wcpu, wmem, wgpu = tonumber(f[4]) or 0, tonumber(f[5]) or 0, tonumber(f[6]) or 0
local mem_total, mem_used, jobs_mem = tonumber(f[8]) or 0, tonumber(f[9]) or 0, tonumber(f[10]) or 0
local isolation = f[11] or ''
local allows_net = f[12] == '1'
local w_os, w_arch, w_disk, w_labels = f[13] or '', f[14] or '', tonumber(f[15]) or 0, f[16] or ','

-- memory already promised to running jobs but not yet visible in the host's usage
local unramped = 0
if mem_total > 0 and limit_mem > 0 then
  local declared = 0
  for _, rid in ipairs(redis.call('SMEMBERS', wrun)) do
    local rm = tonumber(redis.call('HGET', 'ch:job:' .. rid, 'req_mem')) or 0
    if rm == 0 then rm = default_job_mem end
    declared = declared + rm
  end
  unramped = math.max(0, declared - jobs_mem)
end

local skipped_headroom = false
local ids
if specific ~= '' then ids = {specific} else ids = redis.call('ZRANGE', 'ch:pending', 0, limit - 1) end
local skipped_user = false
for _, id in ipairs(ids) do
  local jk = 'ch:job:' .. id
  local j = redis.call('HMGET', jk, 'state', 'not_before', 'req_cpu', 'req_mem', 'req_gpu', 'timeout_s', 'attempt', 'spec', 'req_isolation', 'needs_net', 'exclude', 'sibling', 'req_disk', 'req_os', 'req_arch', 'req_labels', 'user')
  local st = j[1]
  if st ~= 'PENDING' and st ~= 'REQUEUED' then
    redis.call('ZREM', 'ch:pending', id) -- defensive: never leave a non-pending id in the queue
  elseif tonumber(j[2]) <= now
     and tonumber(j[3]) <= wcpu and tonumber(j[4]) <= wmem and tonumber(j[5]) <= wgpu
     and (j[9] == nil or j[9] == '' or j[9] == isolation)
     and (j[10] ~= '1' or allows_net)
     and j[11] ~= w -- a verification replica never runs on the worker that ran its sibling
     and (j[13] == nil or j[13] == '' or (tonumber(j[13]) or 0) == 0 or (tonumber(j[13]) or 0) <= w_disk)
     and (j[14] == nil or j[14] == '' or j[14] == w_os)
     and (j[15] == nil or j[15] == '' or j[15] == w_arch)
     and has_labels(w_labels, j[16]) then
    local fits = true
    local ju = j[17]
    if ju and ju ~= '' and user_limit > 0 and (tonumber(redis.call('GET', 'ch:urun:' .. ju)) or 0) >= user_limit then
      fits = false
      skipped_user = true
    end
    if fits and mem_total > 0 and limit_mem > 0 then
      local req = tonumber(j[4])
      if req == 0 then req = default_job_mem end
      if (mem_used + unramped + req) * 100 / mem_total >= limit_mem then fits = false end
    end
    if not fits then
      skipped_headroom = true
    else
      local attempt = tonumber(j[7]) + 1
      local timeout = tonumber(j[6]) * 1000
      if timeout <= 0 then timeout = default_timeout end
      redis.call('ZREM', 'ch:pending', id)
      redis.call('HSET', jk, 'attempt', attempt, 'worker', w, 'last_worker', w, 'token', token, 'assigned_ms', now)
      setstate(jk, 'ASSIGNED')
      redis.call('ZADD', 'ch:leases', now + timeout + grace, id)
      redis.call('SADD', wrun, id)
      if ju and ju ~= '' then redis.call('INCR', 'ch:urun:' .. ju) end
      if j[12] and j[12] ~= '' then redis.call('HSET', 'ch:job:' .. j[12], 'exclude', w) end
      return {'OK', id, tostring(attempt), j[8], token}
    end
  end
end
if skipped_headroom then return {'REJECT', 'no_headroom'} end
if skipped_user then return {'REJECT', 'user_limit'} end
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

// complete: ARGV id, token, outcome (SUCCEEDED|FAILED), result_json, output_digest.
// The first valid completion wins; everything later is a no-op ("DUP"), and a
// completion from a superseded attempt is discarded ("STALE").
var completeScript = redis.NewScript(prelude + `
local id = ARGV[1]
local jk = 'ch:job:' .. id
local st = redis.call('HGET', jk, 'state')
if not st then return 'UNKNOWN' end
if is_terminal(st) or st == 'VERIFYING' then return 'DUP' end
if st == 'PENDING' or st == 'REQUEUED' or redis.call('HGET', jk, 'token') ~= ARGV[2] then return 'STALE' end
local done_by = redis.call('HGET', jk, 'worker')
release(id, jk)
if ARGV[3] == 'SUCCEEDED' then wstat(done_by, true) end
redis.call('HINCRBY', jk, 'commits', 1) -- audit counter: must end at exactly 1
redis.call('SET', 'ch:result:' .. id, ARGV[4])
if redis.call('HGET', jk, 'verify') == '1' and ARGV[3] == 'SUCCEEDED' then
  -- hold the result back until the other replica has produced the same output
  setstate(jk, 'VERIFYING')
  redis.call('HSET', jk, 'digest', ARGV[5])
  redis.call('ZADD', 'ch:verifying', now_ms(), id)
else
  setstate(jk, ARGV[3])
  redis.call('HSET', jk, 'finished_ms', now_ms())
end
settle(id, jk)
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
local function cancel_one(id)
  local jk = 'ch:job:' .. id
  local st = redis.call('HGET', jk, 'state')
  if not st then return 'UNKNOWN' end
  if is_terminal(st) then return 'DUP' end
  if st == 'ASSIGNED' or st == 'RUNNING' then
    release(id, jk)
  elseif st == 'VERIFYING' then
    redis.call('ZREM', 'ch:verifying', id)
  else
    redis.call('ZREM', 'ch:pending', id)
  end
  setstate(jk, 'CANCELED')
  redis.call('HSET', jk, 'finished_ms', now_ms())
  return 'OK'
end
local r = cancel_one(ARGV[1])
local sib = redis.call('HGET', 'ch:job:' .. ARGV[1], 'sibling')
if r == 'OK' and sib and sib ~= '' then cancel_one(sib) end -- cancelling a verified job cancels both replicas
return r
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

// heartbeat: ARGV worker, cpu, mem_mb, gpu, worker_overloaded, draining, max_jobs, version,
// orphan_grace_ms, has_usage, mem_total_mb, mem_used_mb, jobs_mem_mb, mem_pct, cpu_pct, disk_pct,
// limit_mem, limit_cpu, limit_disk, release_pct, release_after_ms, usage_json, isolation, allows_net, caps_json, running_job_id...
//
// Returns the reported jobs that the worker no longer owns (it must abort them).
//
// Overload is derived here from the raw numbers, with hysteresis: a worker becomes
// overloaded the moment RAM, CPU or disk reaches its limit, and recovers only after
// all three stay below release_pct for release_after. The effective flag is the worker's
// own claim OR this derived state, so a worker cannot opt out by lying about its flag.
// Omitting metrics erases the stored ones, so stale "healthy" numbers cannot be replayed.
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
redis.call('HSET', wk, 'cpu', ARGV[2], 'mem_mb', ARGV[3], 'gpu', ARGV[4], 'draining', ARGV[6],
  'max_jobs', ARGV[7], 'version', ARGV[8], 'last_hb_ms', now, 'w_overloaded', ARGV[5], 'isolation', ARGV[23], 'allows_net', ARGV[24])

-- scheduling facts: os/arch, labels, preference, free disk, load, and which artifacts are cached here
local caps = cjson.decode(ARGV[25])
redis.call('HSET', wk, 'os', caps.os or '', 'arch', caps.arch or '', 'preference', caps.preference or 50,
  'disk_free_mb', caps.disk_free_mb or 0, 'load1', caps.load1 or 0,
  'labels', ',' .. table.concat(caps.labels or {}, ',') .. ',')
local ck = 'ch:wcache:' .. w
redis.call('DEL', ck)
for _, sha in ipairs(caps.cached or {}) do redis.call('SADD', ck, sha) end

local cp = redis.call('HGET', wk, 'cp_over') == '1'
if ARGV[10] == '1' then
  local mem, cpu, disk = tonumber(ARGV[14]), tonumber(ARGV[15]), tonumber(ARGV[16])
  local rel = tonumber(ARGV[20])
  local rel_after = tonumber(ARGV[21])
  local below_since = tonumber(redis.call('HGET', wk, 'below_since')) or 0
  local reasons = {}
  if mem >= tonumber(ARGV[17]) then reasons[#reasons + 1] = 'ram' end
  if cpu >= tonumber(ARGV[18]) then reasons[#reasons + 1] = 'cpu' end
  if disk >= tonumber(ARGV[19]) then reasons[#reasons + 1] = 'disk' end
  if #reasons > 0 then
    cp = true
    below_since = 0
    redis.call('HSET', wk, 'over_reasons', table.concat(reasons, ','))
  elseif cp then
    -- each metric must be one hysteresis band below its own limit (the band is the gap between the
    -- lowest limit and release_pct: 10 points with the 90/80 defaults)
    local band = math.min(tonumber(ARGV[17]), tonumber(ARGV[18]), tonumber(ARGV[19])) - rel
    if mem < tonumber(ARGV[17]) - band and cpu < tonumber(ARGV[18]) - band and disk < tonumber(ARGV[19]) - band then
      if below_since == 0 then below_since = now end
      if now - below_since >= rel_after then
        cp = false
        below_since = 0
        redis.call('HSET', wk, 'over_reasons', '')
      end
    else
      below_since = 0
    end
  end
  redis.call('HSET', wk, 'cp_over', cp and '1' or '0', 'below_since', below_since,
    'mem_total', ARGV[11], 'mem_used', ARGV[12], 'jobs_mem', ARGV[13],
    'mem_pct', ARGV[14], 'cpu_pct', ARGV[15], 'disk_pct', ARGV[16], 'usage_json', ARGV[22])
else
  redis.call('HDEL', wk, 'mem_total', 'mem_used', 'jobs_mem', 'mem_pct', 'cpu_pct', 'disk_pct', 'usage_json')
end
redis.call('HSET', wk, 'overloaded', (ARGV[5] == '1' or cp) and '1' or '0')

local wrun = 'ch:wrun:' .. w
local reported = {}
local cancel = {}
for i = 26, #ARGV do
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

// releaseWorker: ARGV worker_id. Used when a worker is revoked: marks it dead immediately (so it is
// never assigned anything) and requeues everything it held. A revoked worker's jobs were not the
// jobs' fault, so no attempt is counted.
var releaseWorkerScript = redis.NewScript(prelude + `
local w = ARGV[1]
redis.call('ZADD', 'ch:workers', 0, w)
local n = 0
for _, id in ipairs(redis.call('SMEMBERS', 'ch:wrun:' .. w)) do
  local jk = 'ch:job:' .. id
  local st = redis.call('HGET', jk, 'state')
  if st == 'ASSIGNED' or st == 'RUNNING' then
    requeue(id, jk, 'worker_revoked', false)
    n = n + 1
  else
    redis.call('SREM', 'ch:wrun:' .. w, id)
  end
end
return n
`)

// reapVerify: ARGV timeout_ms, limit. A replica that finished but whose sibling never did (for
// example a cluster with a single worker, which can never run both) must not wait forever.
var reapVerifyScript = redis.NewScript(prelude + `
local cutoff = now_ms() - tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', 'ch:verifying', '-inf', cutoff, 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  local jk = 'ch:job:' .. id
  if redis.call('HGET', jk, 'state') == 'VERIFYING' then
    finish_verified(jk, id, 'FAILED', 'timeout', 'verification_timeout')
  else
    redis.call('ZREM', 'ch:verifying', id)
  end
end
return #ids
`)
