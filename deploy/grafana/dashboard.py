#!/usr/bin/env python3
"""Generates deploy/grafana/dashboards/computehive.json (the provisioned cluster dashboard).

Grafana dashboards are verbose JSON; this keeps the source of truth short and reviewable. Every query uses
series exported by coordinator/internal/metrics. Re-generate with:  python3 deploy/grafana/dashboard.py
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
_id = 0


def target(expr, legend="", instant=False, ref="A"):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t.update({"instant": True, "range": False, "format": "table"})
    return t


def panel(kind, title, x, y, w, h, targets, unit="short", desc="", extra=None, thresholds=None):
    global _id
    _id += 1
    p = {
        "id": _id, "type": kind, "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {},
    }
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"}
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
    if extra:
        p.update(extra)
    panels.append(p)


GREEN_RED = [{"color": "green", "value": None}, {"color": "red", "value": 1}]


def ok_warn_bad(warn, bad):
    return [{"color": "green", "value": None}, {"color": "orange", "value": warn}, {"color": "red", "value": bad}]


LIMIT90 = ok_warn_bad(75, 90)

# Row 1: the numbers you glance at first.
stats = [
    ("Workers alive", 'computehive_workers{state="alive"}', "short", [{"color": "red", "value": None}, {"color": "green", "value": 1}]),
    ("Overloaded", 'computehive_workers{state="overloaded"}', "short", GREEN_RED),
    ("Queue depth", "computehive_queue_depth", "short", ok_warn_bad(20, 100)),
    ("Oldest queued job", "computehive_oldest_pending_age_seconds", "s", ok_warn_bad(30, 120)),
    ("Running", 'computehive_jobs{state="RUNNING"}', "short", None),
    ("Dead-letter", 'computehive_jobs{state="DEAD"}', "short", GREEN_RED),
    ("Redis", "computehive_redis_up", "short", [{"color": "red", "value": None}, {"color": "green", "value": 1}]),
]
for i, (title, expr, unit, th) in enumerate(stats):
    panel("stat", title, i * 3, 0, 3, 4, [target(expr)], unit=unit, thresholds=th,
          extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}})
panel("text", "Where to look", 21, 0, 3, 4, [], extra={"options": {"mode": "markdown", "content":
      "`computehive workers`\n\n`computehive explain <job>`\n\nalerts: `/alerts` in Prometheus"}})

# Row 2: flow.
panel("timeseries", "Jobs by state", 0, 4, 8, 8, [target("sum by (state) (computehive_jobs)", "{{state}}")],
      desc="Counts straight from Redis (not an estimate).")
panel("timeseries", "Completions per minute", 8, 4, 8, 8,
      [target('sum by (status, reason) (rate(computehive_job_completions_total[1m])) * 60', "{{status}} {{reason}}")],
      desc="Committed results by final status and failure reason.")
panel("timeseries", "Queue depth and oldest job age", 16, 4, 8, 8,
      [target("computehive_queue_depth", "queued jobs"), target("computehive_oldest_pending_age_seconds", "oldest wait (s)", ref="B")])

# Row 3: the 90% rule, per worker.
for i, (title, metric) in enumerate([("RAM used %", "computehive_worker_ram_used_percent"),
                                      ("CPU used %", "computehive_worker_cpu_used_percent"),
                                      ("Disk used %", "computehive_worker_disk_used_percent")]):
    panel("timeseries", title + " per worker", i * 8, 12, 8, 8, [target(metric, "{{worker}}")], unit="percent",
          desc="Workers stop taking jobs at 90%.",
          extra={"fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100,
                                             "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never",
                                                        "thresholdsStyle": {"mode": "line"}},
                                             "thresholds": {"mode": "absolute", "steps": LIMIT90}}, "overrides": []}})

# Row 4: the live cluster view (one row per worker).
cluster = [
    ("computehive_worker_ram_used_percent", "RAM %"), ("computehive_worker_cpu_used_percent", "CPU %"),
    ("computehive_worker_disk_used_percent", "Disk %"), ("computehive_worker_running_jobs", "Jobs"),
    ("computehive_worker_reliability", "Reliability"), ("computehive_worker_heartbeat_age_seconds", "Heartbeat age (s)"),
    ("computehive_worker_overloaded", "Overloaded"),
]
refs = "ABCDEFG"
panel("table", "Cluster view: workers right now", 0, 20, 24, 7,
      # max by (worker) drops __name__/instance/job, so the joined table has one clean column per metric
      [target(f"max by (worker) ({m})", "", instant=True, ref=refs[i]) for i, (m, _) in enumerate(cluster)],
      desc="One row per worker, straight from the coordinator's view of the heartbeats.",
      extra={"transformations": [
          {"id": "joinByField", "options": {"byField": "worker", "mode": "outer"}},
          {"id": "organize", "options": {
              "excludeByName": {**{f"Time {r}": True for r in refs}, "Time": True},
              "renameByName": {**{f"Value #{refs[i]}": name for i, (_, name) in enumerate(cluster)}, "worker": "Worker"}}}]})


# Row 5: latency histograms.
def quantiles(metric):
    return [target(f"histogram_quantile(0.5, sum by (le) (rate({metric}_bucket[5m])))", "p50"),
            target(f"histogram_quantile(0.95, sum by (le) (rate({metric}_bucket[5m])))", "p95", ref="B")]


panel("timeseries", "Time in queue", 0, 27, 8, 8, quantiles("computehive_job_time_in_queue_seconds"), unit="s",
      desc="From submission to assignment (a requeued job counts its whole life).")
panel("timeseries", "Job runtime", 8, 27, 8, 8, quantiles("computehive_job_runtime_seconds"), unit="s")
panel("timeseries", "Scheduling decision time", 16, 27, 8, 8, quantiles("computehive_scheduling_decision_seconds"), unit="s",
      desc="Time to answer a worker's RequestJob: snapshot, scoring and the atomic claim.")

# Row 6: things going wrong.
panel("timeseries", "Requeues by reason", 0, 35, 6, 8, [target("sum by (reason) (rate(computehive_job_requeues_total[5m])) * 60", "{{reason}}")],
      desc="per minute. worker_dead / lease_expired = failures; infra = worker problem; evicted = host overload.")
panel("timeseries", "Jobs killed", 6, 35, 6, 8, [target("sum by (kind) (increase(computehive_jobs_killed_total[5m]))", "{{kind}}")],
      desc="kill = exit 137/143 (OOM, disk or PID quota, timeout kill); evicted = overload.")
panel("timeseries", "Overload events", 12, 35, 6, 8, [target("sum by (worker) (increase(computehive_worker_overload_events_total[5m]))", "{{worker}}")],
      desc="Times a worker crossed the 90% rule in the last 5 minutes.")
panel("timeseries", "Workers lost", 18, 35, 6, 8, [target("increase(computehive_workers_lost_total[5m])", "lost (5m)")])

# Row 7: the API.
panel("timeseries", "RPC rate by method", 0, 43, 8, 8, [target("sum by (method) (rate(computehive_grpc_requests_total[1m]))", "{{method}}")], unit="reqps")
panel("timeseries", "RPC errors", 8, 43, 8, 8,
      [target('sum by (method, code) (rate(computehive_grpc_requests_total{code!="OK"}[1m]))', "{{method}} {{code}}")], unit="reqps",
      desc="Everything except OK. NotFound from Heartbeat means a worker must re-register; Unauthenticated is a bad token.")
panel("timeseries", "RPC p95 latency", 16, 43, 8, 8,
      [target("histogram_quantile(0.95, sum by (le, method) (rate(computehive_grpc_request_duration_seconds_bucket[5m])))", "{{method}}")], unit="s")

dashboard = {
    "uid": "computehive", "title": "ComputeHive cluster", "tags": ["computehive"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
    "panels": panels, "templating": {"list": []}, "annotations": {"list": []},
}

out = pathlib.Path(__file__).parent / "dashboards" / "computehive.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(dashboard, indent=1) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
