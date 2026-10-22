import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { openPath } from "@tauri-apps/plugin-opener";

import {
  call,
  inTauri,
  type ClusterOverview,
  type ContributorSharingStatus,
  type ControlStatus,
  type DockerImageResult,
  type DownloadedFile,
  type ExplainView,
  type JobProgress,
  type RunOptions,
  type RunRequestJobResult,
  type RunRequestResult,
  type WorkerPolicy,
  type WorkerView,
} from "./api";
import { $, clamp, clean, clockTime, fact, formatAge, formatBytes, formatTime, h, meter, pill, replaceChildren } from "./ui";

type Tab = "run" | "cluster" | "contributor";
type Tone = "info" | "success" | "error";

// ---------------------------------------------------------------------------------------------------
// shared helpers

const errMsg = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/** Re-runs fn every `ms` after the previous run finished (no overlap), skipping beats while the window is hidden. */
function every(ms: number, fn: () => Promise<void>): { stop(): void; now(): void } {
  let timer: number | undefined;
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    if (!document.hidden) {
      try {
        await fn();
      } catch {
        /* each poller reports its own errors */
      }
    }
    if (!stopped) timer = window.setTimeout(tick, ms);
  };
  timer = window.setTimeout(tick, 0);
  return {
    stop() {
      stopped = true;
      if (timer !== undefined) window.clearTimeout(timer);
    },
    now() {
      if (timer !== undefined) window.clearTimeout(timer);
      timer = window.setTimeout(tick, 0);
    },
  };
}

const activity = $<HTMLOListElement>("#process-log");
function log(tone: Tone, message: string): void {
  const first = activity.firstElementChild;
  if (first && first.classList.contains("muted")) first.remove();
  activity.prepend(h("li", { class: tone === "info" ? "" : `is-${tone}` }, h("time", {}, clockTime()), h("span", {}, message)));
  while (activity.children.length > 100) activity.lastElementChild?.remove();
}

async function pickFolder(): Promise<string | null> {
  if (!inTauri) return "/home/you/projects/sample-app"; // preview mode has no native dialog
  const picked = await openDialog({ directory: true, multiple: false });
  return typeof picked === "string" ? picked : null;
}

// ---------------------------------------------------------------------------------------------------
// tabs

const tabButtons = Array.from(document.querySelectorAll<HTMLButtonElement>("[data-tab]"));
const tabPanels = Array.from(document.querySelectorAll<HTMLElement>("[data-panel]"));
let currentTab: Tab = "run";

function showTab(tab: Tab, updateHash = true): void {
  currentTab = tab;
  for (const b of tabButtons) {
    const on = b.dataset.tab === tab;
    b.setAttribute("aria-selected", String(on));
    b.tabIndex = on ? 0 : -1;
  }
  for (const p of tabPanels) p.hidden = p.dataset.panel !== tab;
  if (updateHash && location.hash !== `#${tab}`) history.replaceState(null, "", `#${tab}`);
  if (tab === "cluster") overviewPoll.now();
  if (tab === "contributor") contributorPoll.now();
  window.scrollTo({ top: 0 });
}

for (const b of tabButtons) {
  b.addEventListener("click", () => showTab(b.dataset.tab as Tab));
  b.addEventListener("keydown", (e) => {
    const order: Tab[] = ["run", "cluster", "contributor"];
    const i = order.indexOf(currentTab);
    const next = e.key === "ArrowRight" ? order[(i + 1) % 3] : e.key === "ArrowLeft" ? order[(i + 2) % 3] : null;
    if (next) {
      e.preventDefault();
      showTab(next);
      tabButtons.find((t) => t.dataset.tab === next)?.focus();
    }
  });
}
window.addEventListener("hashchange", () => {
  const t = location.hash.slice(1);
  if (t === "run" || t === "cluster" || t === "contributor") showTab(t, false);
});

if (!inTauri) {
  const banner = $<HTMLElement>("#banner");
  banner.hidden = false;
  banner.textContent = "Preview mode: this page is not running inside the desktop app, so it shows sample data and cannot reach a real cluster.";
}

// ---------------------------------------------------------------------------------------------------
// cluster overview (shared by the header chip and the Cluster tab)

const chip = $<HTMLElement>("#cluster-chip");
let overview: ClusterOverview | null = null;

const overviewPoll = every(3000, async () => {
  overview = await call<ClusterOverview>("cluster_overview");
  renderChip();
  if (currentTab === "cluster") renderCluster();
});

function renderChip(): void {
  if (!overview) return;
  const alive = overview.workers.filter((w) => w.state !== "dead").length;
  if (overview.reachable) {
    chip.className = "chip chip-ok";
    chip.textContent = `● ${alive} worker${alive === 1 ? "" : "s"} online`;
    chip.title = `Connected to ${overview.coordinatorAddr}${overview.tls ? " (TLS)" : " (no TLS)"}`;
  } else {
    chip.className = "chip chip-bad";
    chip.textContent = "● Cluster unreachable";
    chip.title = overview.error ?? "";
  }
}

function renderCluster(): void {
  const o = overview;
  if (!o) return;
  const alive = o.workers.filter((w) => w.state !== "dead");
  const overloaded = o.workers.filter((w) => w.state === "overloaded").length;
  const running = o.jobs.filter((j) => j.status === "running" || j.status === "assigned").length;
  const queued = o.jobs.filter((j) => j.status === "queued" || j.status === "requeued").length;
  const stat = (label: string, value: number, tone = "") => h("div", { class: `stat ${tone}`.trim() }, h("b", {}, String(value)), h("span", {}, label));
  replaceChildren(
    $("#cluster-summary"),
    stat("Workers online", alive.length, alive.length > 0 ? "is-ok" : "is-bad"),
    stat("Overloaded", overloaded, overloaded > 0 ? "is-bad" : ""),
    stat("Running jobs", running),
    stat("Queued jobs", queued),
  );

  const problem = $("#cluster-problem");
  problem.hidden = !o.error;
  problem.textContent = o.error ?? "";

  const grid = $("#worker-grid");
  if (o.workers.length === 0) {
    replaceChildren(grid, h("article", { class: "card" }, h("p", { class: "muted" }, o.reachable ? "No workers have joined this cluster yet. Use the Contributor tab to add this machine." : "No data: the cluster is not reachable.")));
  } else {
    replaceChildren(grid, ...o.workers.map(workerCard));
  }

  renderJobsTable(o);
  $("#cluster-meta").textContent = `Coordinator ${o.coordinatorAddr} · ${o.tls ? "TLS on" : "TLS OFF (development only)"} · credentials from ${o.configSource}`;
}

function workerCard(w: WorkerView): HTMLElement {
  const dead = w.state === "dead";
  return h(
    "article",
    { class: "card worker-card" },
    h("header", {}, h("h3", {}, w.id), pill(w.state)),
    h("p", { class: "muted small" }, `${w.os || "?"}/${w.arch || "?"} · ${w.cpu} CPU · ${formatBytes(w.memoryMb * 1024 * 1024)}${w.gpu ? ` · ${w.gpu} GPU` : ""} · ${w.isolation || "runc"}`),
    dead
      ? h("p", { class: "muted" }, `No heartbeat for ${w.heartbeatAgeSecs}s`)
      : h("div", { class: "meters" }, meter("RAM", w.ramPercent), meter("CPU", w.cpuPercent), meter("Disk", w.diskPercent)),
    w.overloadReasons.length > 0 ? h("p", { class: "error" }, `Overloaded: ${w.overloadReasons.join(", ")}. It takes no new jobs until it recovers.`) : null,
    h("p", { class: "small" }, `${w.runningJobs} running · reliability ${Math.round(w.reliability * 100)}%`),
    w.labels.length > 0 ? h("div", { class: "labels" }, ...w.labels.map((l) => h("span", {}, l))) : null,
  );
}

function renderJobsTable(o: ClusterOverview): void {
  const table = $<HTMLTableElement>("#jobs-table");
  if (o.jobs.length === 0) {
    replaceChildren(table, h("tbody", {}, h("tr", {}, h("td", { class: "muted" }, "No jobs yet."))));
    return;
  }
  const head = h("thead", {}, h("tr", {}, ...["Job", "Status", "Attempt", "Worker", "Age", "Image"].map((t) => h("th", {}, t))));
  const rows = o.jobs.map((j) => {
    const open = h("button", { class: "link", type: "button", title: "Follow this job" }, j.id);
    open.addEventListener("click", () => followJob(j.id));
    return h(
      "tr",
      {},
      h("td", {}, open),
      h("td", {}, pill(j.status), j.lastFailure ? h("div", { class: "muted small" }, j.lastFailure) : null),
      h("td", { class: "num" }, `${j.attempt}/${j.maxAttempts}`),
      h("td", {}, j.worker || "–"),
      h("td", { class: "num" }, formatAge(j.createdUnix)),
      h("td", { class: "wrap" }, j.image),
    );
  });
  replaceChildren(table, head, h("tbody", {}, ...rows));
}

// ---------------------------------------------------------------------------------------------------
// run view

const run = {
  path: "",
  commands: [""] as string[],
  busy: false,
  jobId: "",
  terminal: false,
  explainFor: "",
  result: null as RunRequestJobResult | null,
  poll: null as { stop(): void } | null,
};

const pickBtn = $<HTMLButtonElement>("#pick-folder");
const revealBtn = $<HTMLButtonElement>("#reveal-folder");
const clearBtn = $<HTMLButtonElement>("#clear-selection");
const runBtn = $<HTMLButtonElement>("#request-run");
const selectedPath = $<HTMLElement>("#selected-path");
const runError = $<HTMLElement>("#run-error");

function setPath(path: string): void {
  run.path = path;
  selectedPath.textContent = path || "No folder selected yet";
  selectedPath.classList.toggle("is-empty", !path);
  revealBtn.disabled = !path;
  syncRunButton();
}

function syncRunButton(): void {
  runBtn.disabled = run.busy || run.path === "";
  runBtn.textContent = run.busy ? "Working…" : "Run on the cluster";
}

function showRunError(message: string): void {
  runError.hidden = message === "";
  runError.textContent = message;
}

function renderCommands(): void {
  const list = $("#command-list");
  replaceChildren(
    list,
    ...run.commands.map((value, i) => {
      const input = h("input", { type: "text", placeholder: i === 0 ? "e.g. python main.py (optional)" : "another command", "aria-label": `Command line ${i + 1}`, autocomplete: "off", spellcheck: false });
      input.value = value;
      input.addEventListener("input", () => (run.commands[i] = input.value));
      const remove = h("button", { class: "btn btn-icon", type: "button", "aria-label": `Remove command line ${i + 1}` }, "×");
      remove.addEventListener("click", () => {
        run.commands.splice(i, 1);
        if (run.commands.length === 0) run.commands.push("");
        renderCommands();
      });
      return h("div", { class: "command-row" }, input, run.commands.length > 1 ? remove : null);
    }),
  );
}

function readOptions(): RunOptions {
  const num = (id: string): number | undefined => {
    const raw = $<HTMLInputElement>(id).value.trim();
    if (raw === "") return undefined;
    const n = Number(raw);
    if (!Number.isFinite(n) || !Number.isInteger(n)) throw new Error(`"${raw}" is not a whole number`);
    return n;
  };
  const labels = $<HTMLInputElement>("#opt-labels").value.split(/[,\s]+/).filter(Boolean);
  for (const l of labels) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._=-]{0,63}$/.test(l)) throw new Error(`label "${l}" may only contain letters, digits and . _ = -`);
  }
  return {
    cpuCores: num("#opt-cpu"),
    memoryMb: num("#opt-mem"),
    gpuCount: num("#opt-gpu"),
    timeoutSeconds: num("#opt-timeout"),
    minDiskMb: num("#opt-disk"),
    verify: $<HTMLInputElement>("#opt-verify").checked,
    os: $<HTMLSelectElement>("#opt-os").value || undefined,
    arch: $<HTMLSelectElement>("#opt-arch").value || undefined,
    labels,
  };
}

function setSteps(active: "prepare" | "queued" | "running" | "done" | "failed" | "idle"): void {
  const order = ["prepare", "queued", "running", "done"];
  const at = active === "failed" ? -1 : order.indexOf(active);
  for (const li of Array.from($("#run-steps").children) as HTMLElement[]) {
    const i = order.indexOf(li.dataset.step ?? "");
    li.className = active === "idle" ? "" : active === "failed" ? (i === 0 ? "is-failed" : "") : i < at ? "is-done" : i === at ? (active === "done" ? "is-done" : "is-active") : "";
  }
}

function renderImageFacts(img: DockerImageResult): void {
  $("#image-facts").hidden = false;
  $("#detected-stack").textContent = img.detectedStack;
  $("#image-tag").textContent = img.imageTag;
  $("#image-size").textContent = formatBytes(img.imageSizeBytes);
  $("#docker-setup-source").textContent = img.dockerSetupSource;
  $("#build-details").hidden = img.notes.length === 0;
  replaceChildren($("#build-notes"), ...img.notes.map((n) => h("li", {}, n)));
}

async function requestRun(): Promise<void> {
  showRunError("");
  let options: RunOptions;
  try {
    options = readOptions();
  } catch (e) {
    showRunError(errMsg(e));
    return;
  }
  run.busy = true;
  syncRunButton();
  setSteps("prepare");
  resetJobView();
  log("info", `Building ${run.path} and uploading it to the cluster…`);
  try {
    const res = await call<RunRequestResult>("request_project_run", { projectPath: run.path, command: run.commands, options });
    renderImageFacts(res.image);
    log("success", res.summary);
    followJob(res.jobId);
  } catch (e) {
    setSteps("failed");
    showRunError(errMsg(e));
    log("error", errMsg(e));
  } finally {
    run.busy = false;
    syncRunButton();
  }
}

function resetJobView(): void {
  run.poll?.stop();
  run.poll = null;
  run.jobId = "";
  run.terminal = false;
  run.explainFor = "";
  run.result = null;
  $("#job-card").hidden = true;
  $("#job-result").hidden = true;
  $("#outputs-box").hidden = true;
  $("#download-status").textContent = "";
  $("#job-log").textContent = "(no output yet)";
}

/** Shows a job (from a new run, or picked from the Cluster tab) and follows it until it finishes. */
function followJob(jobId: string): void {
  resetJobView();
  showTab("run");
  run.jobId = jobId;
  $("#job-card").hidden = false;
  $("#job-id").textContent = jobId;
  const status = $("#job-status");
  status.className = "pill pill-queued";
  status.textContent = "queued";
  $("#job-meta").textContent = "Waiting for a worker…";
  $("#cancel-job").hidden = false;
  setSteps("queued");
  run.poll = every(1000, () => pollJob(jobId));
  $("#job-card").scrollIntoView({ behavior: "smooth", block: "start" });
}

async function pollJob(jobId: string): Promise<void> {
  if (jobId !== run.jobId) return;
  let p: JobProgress;
  try {
    p = await call<JobProgress>("get_job_progress", { jobId });
  } catch (e) {
    $("#job-meta").textContent = `Cannot read the job status: ${errMsg(e)}`;
    return;
  }
  if (jobId !== run.jobId) return;

  const status = $("#job-status");
  status.className = `pill pill-${p.status}`;
  status.textContent = p.status;
  const where = p.worker ? `on ${p.worker}` : "waiting for a worker";
  $("#job-meta").textContent = `${where} · attempt ${Math.max(p.attempt, 1)} of ${p.maxAttempts || 3}${p.lastFailure ? ` · last failure: ${p.lastFailure}` : ""}${p.verification ? ` · verification: ${p.verification}` : ""}`;

  const logBox = $<HTMLElement>("#job-log");
  const nearBottom = logBox.scrollHeight - logBox.scrollTop - logBox.clientHeight < 40;
  const text = clean(p.logTail);
  if (text !== "" && logBox.textContent !== text) {
    logBox.textContent = text;
    if (nearBottom) logBox.scrollTop = logBox.scrollHeight;
  }

  if (p.status === "running" || p.status === "assigned") setSteps("running");
  if (p.worker && p.worker !== run.explainFor) {
    run.explainFor = p.worker;
    void loadExplain(jobId);
  }
  if (p.terminal) {
    run.poll?.stop();
    run.terminal = true;
    $("#cancel-job").hidden = true;
    setSteps(p.status === "succeeded" ? "done" : "failed");
    await loadResult(jobId);
    void loadExplain(jobId);
    log(p.status === "succeeded" ? "success" : "error", `Job ${jobId.slice(0, 8)} ${p.status}.`);
  }
}

async function loadExplain(jobId: string): Promise<void> {
  try {
    const e = await call<ExplainView>("explain_job", { jobId });
    if (jobId !== run.jobId) return;
    $("#explain-summary").textContent = e.summary + (e.recorded ? "" : " (live view of the cluster right now)");
    const parts = Object.keys(e.weights).sort();
    const head = h("thead", {}, h("tr", {}, h("th", {}, "Worker"), h("th", {}, "Verdict"), h("th", { class: "num" }, "Score"), ...parts.map((k) => h("th", { class: "num", title: `weight ${e.weights[k]}` }, k.replace("_", " ")))));
    const rows = e.evaluations.map((v) =>
      h(
        "tr",
        { class: v.chosen ? "is-chosen" : "" },
        h("td", {}, v.worker + (v.chosen ? " ✓" : "")),
        h("td", {}, v.eligible ? (v.busy ? "eligible, busy" : "eligible") : v.reasons.join("; ") || "rejected"),
        h("td", { class: "num" }, v.eligible ? v.score.toFixed(3) : "–"),
        ...parts.map((k) => h("td", { class: "num" }, v.eligible && v.parts[k] !== undefined ? v.parts[k].toFixed(3) : "")),
      ),
    );
    replaceChildren($("#explain-table"), head, h("tbody", {}, ...rows));
  } catch (e) {
    $("#explain-summary").textContent = `Could not load the explanation: ${errMsg(e)}`;
  }
}

async function loadResult(jobId: string): Promise<void> {
  try {
    const r = await call<RunRequestJobResult | null>("get_run_request_result", { jobId });
    if (jobId !== run.jobId || !r) return;
    run.result = r;
    $("#job-result").hidden = false;
    replaceChildren(
      $("#result-facts"),
      fact("Status", pill(r.resultStatus || r.status)),
      fact("Exit code", String(r.exitCode)),
      r.failureReason ? fact("Failure reason", r.failureReason) : null,
      fact("Finished", formatTime(r.finishedAtUnix)),
    );
    const streams: HTMLElement[] = [];
    const out = clean(r.stdoutExcerpt);
    const err = clean(r.stderrExcerpt);
    if (out) streams.push(h("p", { class: "stream-label" }, "stdout"), h("pre", { class: "stream" }, out));
    if (err) streams.push(h("p", { class: "stream-label" }, "stderr"), h("pre", { class: "stream is-err" }, err));
    replaceChildren($("#result-streams"), ...streams);
    $("#outputs-box").hidden = r.outputArtifacts.length === 0;
    replaceChildren(
      $("#job-outputs"),
      ...r.outputArtifacts.map((a) => h("li", {}, h("span", {}, a.relativePath), h("span", { class: "muted" }, `${formatBytes(a.sizeBytes)}${a.sha256 ? ` · sha256 ${a.sha256.slice(0, 12)}…` : ""}`))),
    );
  } catch (e) {
    log("error", `Could not load the result: ${errMsg(e)}`);
  }
}

async function downloadOutputs(): Promise<void> {
  const status = $("#download-status");
  try {
    const dest = inTauri ? await openDialog({ directory: true, multiple: false, title: "Save the results into…" }) : "/home/you/results";
    if (typeof dest !== "string") return;
    status.textContent = "Downloading and verifying…";
    const files = await call<DownloadedFile[]>("download_job_outputs", { jobId: run.jobId, destDir: dest });
    status.textContent = `Saved ${files.length} file${files.length === 1 ? "" : "s"} to ${dest} (each checked against its SHA-256).`;
    log("success", status.textContent);
  } catch (e) {
    status.textContent = errMsg(e);
    log("error", errMsg(e));
  }
}

pickBtn.addEventListener("click", async () => {
  try {
    const picked = await pickFolder();
    if (picked) {
      setPath(picked);
      showRunError("");
      log("info", `Selected ${picked}`);
    }
  } catch (e) {
    showRunError(errMsg(e));
  }
});
clearBtn.addEventListener("click", () => {
  setPath("");
  run.commands = [""];
  renderCommands();
  showRunError("");
  setSteps("idle");
  $("#image-facts").hidden = true;
  $("#build-details").hidden = true;
  resetJobView();
});
revealBtn.addEventListener("click", () => {
  if (run.path && inTauri) void openPath(run.path).catch((e) => showRunError(errMsg(e)));
});
runBtn.addEventListener("click", () => void requestRun());
$("#add-command").addEventListener("click", () => {
  run.commands.push("");
  renderCommands();
  ($("#command-list").lastElementChild?.querySelector("input") as HTMLInputElement | null)?.focus();
});
$("#cancel-job").addEventListener("click", async () => {
  try {
    await call<string>("cancel_job", { jobId: run.jobId });
    log("info", `Cancel requested for ${run.jobId.slice(0, 8)}.`);
  } catch (e) {
    showRunError(errMsg(e));
  }
});
$("#download-outputs").addEventListener("click", () => void downloadOutputs());
$("#explain-box").addEventListener("toggle", () => {
  if (($("#explain-box") as HTMLDetailsElement).open && run.jobId) void loadExplain(run.jobId);
});

// ---------------------------------------------------------------------------------------------------
// contributor view

const contrib = { running: false, dirty: false, policy: null as WorkerPolicy | null, maxMem: 16384 };
const cState = $<HTMLElement>("#contrib-state");
const cButton = $<HTMLButtonElement>("#contribute-button");
const cMessage = $<HTMLElement>("#contrib-message");
const cError = $<HTMLElement>("#contrib-error");
const form = $<HTMLFormElement>("#policy-form");
const formControls = Array.from(form.querySelectorAll<HTMLInputElement | HTMLTextAreaElement | HTMLButtonElement>("input, textarea, button"));
const pauseBtn = $<HTMLButtonElement>("#contrib-pause");
const pauseNowBtn = $<HTMLButtonElement>("#contrib-pause-now");
const resumeBtn = $<HTMLButtonElement>("#contrib-resume");

function setControlsEnabled(on: boolean): void {
  for (const c of formControls) c.disabled = !on;
  for (const b of [pauseBtn, pauseNowBtn, resumeBtn]) b.disabled = !on;
  $("#controls-note").hidden = on;
}

function syncOutputs(): void {
  const cpus = Number($<HTMLInputElement>("#pol-cpus").value);
  $("#out-cpus").textContent = cpus === 0 ? "all" : String(cpus);
  const mem = Number($<HTMLInputElement>("#pol-mem").value);
  $("#out-mem").textContent = mem === 0 ? "all" : formatBytes(mem * 1024 * 1024);
  $("#out-idle").textContent = `${$<HTMLInputElement>("#pol-idle-cpu").value}%`;
}

function fillPolicyForm(p: WorkerPolicy, metrics: ControlStatus["metrics"]): void {
  const cpuSlider = $<HTMLInputElement>("#pol-cpus");
  cpuSlider.max = String(Math.max(navigator.hardwareConcurrency || 4, Math.ceil(p.max_cpus)));
  cpuSlider.value = String(Math.round(p.max_cpus));
  contrib.maxMem = Math.max(1024, Math.floor((metrics?.MemTotalMB ?? 16384) / 256) * 256, p.max_memory_mb);
  const memSlider = $<HTMLInputElement>("#pol-mem");
  memSlider.max = String(contrib.maxMem);
  memSlider.value = String(p.max_memory_mb);
  $<HTMLInputElement>("#pol-disk").value = String(p.max_job_disk_mb);
  $<HTMLInputElement>("#pol-idle").checked = p.idle_only;
  $<HTMLInputElement>("#pol-idle-cpu").value = String(p.idle_cpu_percent || 25);
  $<HTMLInputElement>("#pol-schedule").value = p.schedule ?? "";
  $<HTMLInputElement>("#pol-net").checked = p.allow_network;
  $<HTMLTextAreaElement>("#pol-hosts").value = (p.allowed_hosts ?? []).join("\n");
  syncOutputs();
}

function renderContributorLive(c: ControlStatus | null): void {
  const meters = $("#contrib-metrics");
  const facts = $("#contrib-facts");
  if (!c) {
    replaceChildren(meters, h("p", { class: "muted" }, "Start sharing to see this machine's load."));
    replaceChildren(facts);
    return;
  }
  const m = c.metrics ?? {};
  replaceChildren(
    meters,
    meter("RAM", m.MemPercent ?? 0),
    meter("CPU", m.CPUPercent ?? 0),
    meter("Disk", Math.max(m.WorkDiskUsedPercent ?? 0, m.DockerDiskUsedPercent ?? 0)),
  );
  replaceChildren(
    facts,
    fact("State", pill(c.state)),
    fact("Worker", c.worker_id),
    fact("Isolation", c.isolation || "–"),
    fact("Uptime", c.uptime || "–"),
    c.current_job ? fact("Running job", c.current_job.slice(0, 8)) : null,
    c.not_taking ? fact("Not taking jobs because", c.not_taking) : null,
  );
}

async function refreshContributor(): Promise<void> {
  let sharing: ContributorSharingStatus;
  try {
    sharing = await call<ContributorSharingStatus>("get_contributor_sharing_status");
  } catch (e) {
    cError.hidden = false;
    cError.textContent = errMsg(e);
    return;
  }
  contrib.running = sharing.running;
  cButton.textContent = sharing.running ? "Stop sharing" : "Start sharing";
  cButton.classList.toggle("btn-danger", sharing.running);
  cButton.classList.toggle("btn-primary", !sharing.running);

  if (!sharing.running) {
    cState.className = "pill pill-stopped";
    cState.textContent = "stopped";
    renderContributorLive(null);
    setControlsEnabled(false);
    $("#controls-note").textContent = "Start sharing to change these.";
    return;
  }
  try {
    const c = await call<ControlStatus>("contributor_control_status");
    cState.className = `pill pill-${c.state}`;
    cState.textContent = c.state;
    renderContributorLive(c);
    contrib.policy = c.policy;
    const editing = form.contains(document.activeElement);
    if (!contrib.dirty && !editing) fillPolicyForm(c.policy, c.metrics);
    setControlsEnabled(true);
    pauseBtn.hidden = c.policy.paused;
    pauseNowBtn.hidden = c.policy.paused;
    resumeBtn.hidden = !c.policy.paused;
    cError.hidden = true;
  } catch (e) {
    cState.className = "pill pill-queued";
    cState.textContent = "starting";
    renderContributorLive(null);
    setControlsEnabled(false);
    $("#controls-note").hidden = false;
    $("#controls-note").textContent = `The worker is starting… (${errMsg(e)})`;
  }
}

const contributorPoll = every(2000, async () => {
  if (currentTab === "contributor") await refreshContributor();
});

cButton.addEventListener("click", async () => {
  cError.hidden = true;
  cButton.disabled = true;
  try {
    if (contrib.running) {
      await call<ContributorSharingStatus>("stop_contributor_sharing");
      cMessage.textContent = "Sharing stopped. Any running job was handed back to the cluster.";
      contrib.dirty = false;
    } else {
      cMessage.textContent = "Starting the worker…";
      const token = $<HTMLInputElement>("#enroll-token").value.trim();
      const res = await call<ContributorSharingStatus>("start_contributor_sharing", { enrollmentToken: token || null });
      cMessage.textContent = res.details;
      $<HTMLInputElement>("#enroll-token").value = "";
    }
    log("info", cMessage.textContent ?? "");
  } catch (e) {
    cError.hidden = false;
    cError.textContent = errMsg(e);
    cMessage.textContent = "";
    log("error", errMsg(e));
  } finally {
    cButton.disabled = false;
    await refreshContributor();
  }
});

for (const id of ["#pol-cpus", "#pol-mem", "#pol-idle-cpu"]) $(id).addEventListener("input", syncOutputs);
form.addEventListener("input", () => (contrib.dirty = true));
form.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const status = $("#pol-status");
  if (!contrib.policy) {
    status.textContent = "The worker is not ready yet.";
    return;
  }
  if (!form.reportValidity()) return;
  const next: WorkerPolicy = {
    ...contrib.policy, // keep everything this form does not edit (image allow/deny, gVisor...)
    max_cpus: Number($<HTMLInputElement>("#pol-cpus").value),
    max_memory_mb: clamp(Number($<HTMLInputElement>("#pol-mem").value), 0, contrib.maxMem),
    max_job_disk_mb: Math.max(16, Number($<HTMLInputElement>("#pol-disk").value) || 256),
    idle_only: $<HTMLInputElement>("#pol-idle").checked,
    idle_cpu_percent: Number($<HTMLInputElement>("#pol-idle-cpu").value),
    schedule: $<HTMLInputElement>("#pol-schedule").value.trim(),
    allow_network: $<HTMLInputElement>("#pol-net").checked,
    allowed_hosts: $<HTMLTextAreaElement>("#pol-hosts").value.split(/\r?\n/).map((s) => s.trim()).filter(Boolean),
  };
  try {
    await call("contributor_set_policy", { policy: next });
    contrib.dirty = false;
    status.textContent = "Applied.";
    log("success", "Contributor limits updated.");
    await refreshContributor();
  } catch (e) {
    status.textContent = errMsg(e);
  }
});

async function control(action: "pause" | "pauseNow" | "resume"): Promise<void> {
  try {
    if (action === "resume") await call("contributor_resume");
    else await call("contributor_pause", { now: action === "pauseNow" });
    log("info", action === "resume" ? "Sharing resumed." : action === "pauseNow" ? "Paused; the running job was handed back." : "Paused; the running job will finish.");
  } catch (e) {
    cError.hidden = false;
    cError.textContent = errMsg(e);
  }
  await refreshContributor();
}
pauseBtn.addEventListener("click", () => void control("pause"));
pauseNowBtn.addEventListener("click", () => void control("pauseNow"));
resumeBtn.addEventListener("click", () => void control("resume"));

// ---------------------------------------------------------------------------------------------------
// start

renderCommands();
setControlsEnabled(false);
renderContributorLive(null);
setSteps("idle");
syncOutputs();
{
  const t = location.hash.slice(1);
  showTab(t === "cluster" || t === "contributor" ? t : "run", false);
}
