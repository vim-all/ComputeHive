import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { openPath } from "@tauri-apps/plugin-opener";

type Mode = "user" | "contributor";
type BuildStatus = "idle" | "ready" | "working" | "done" | "error";
type ProcessLogTone = "info" | "working" | "success" | "error";

interface DockerImageResult {
  projectName: string;
  projectRoot: string;
  dockerfilePath: string;
  imageArchivePath: string;
  imageTag: string;
  imageSizeBytes: number;
  detectedStack: string;
  dockerSetupSource: string;
  generatedFiles: string[];
  summary: string;
  notes: string[];
}

interface RunRequestResult {
  image: DockerImageResult;
  gzipArchivePath: string;
  gzipSizeBytes: number;
  artifactSha256: string;
  artifactObjectKey: string;
  artifactUri: string;
  artifactApiUrl: string;
  artifactPublicUrl: string;
  artifactRecordKey: string;
  artifactEtag?: string | null;
  redisJobId: string;
  redisJobKey: string;
  redisStatusKey: string;
  redisQueueKey: string;
  summary: string;
  notes: string[];
}

interface RunRequestJobResult {
  jobId: string;
  workerId: string;
  status: string;
  resultStatus: string;
  partial: boolean;
  exitCode: number;
  stdoutExcerpt: string;
  stderrExcerpt: string;
  outputUri: string;
  finishedAtUnix: number;
  completed: boolean;
}

interface ContributorWorkerRecord {
  id: string;
  status: string;
  resources: {
    cpu_cores: number;
    memory_mb: number;
    gpu: boolean;
  };
  current_load: {
    cpu_used: number;
    memory_used: number;
  };
  capabilities: {
    docker: boolean;
    gpu_supported: boolean;
  };
  last_heartbeat: number;
  stats: {
    jobs_completed: number;
    jobs_failed: number;
  };
}

interface ProcessLogEntry {
  id: number;
  tone: ProcessLogTone;
  message: string;
  timestamp: string;
}

interface InterfaceCopy {
  eyebrow: string;
  heading: string;
  copy: string;
}

const interfaceCopy: Record<Mode, InterfaceCopy> = {
  user: {
    eyebrow: "Project flow",
    heading: "Compute Hive",
    copy:
      "Choose a project directory. ComputeHive prepares Docker when needed, builds the image, compresses the artifact, uploads it, and writes the queue record.",
  },
  contributor: {
    eyebrow: "Contributor flow",
    heading: "Compute Hive",
    copy:
      "Register or log in with a worker name and password. The contributor surface unlocks only when the local worker hash matches this device.",
  },
};

const modeBurstTimers = new WeakMap<HTMLButtonElement, number>();
const processLogTimers = new Set<number>();
let nextProcessLogId = 1;
let renderedProcessLogCount = 0;
let jobResultPollTimer: number | null = null;

function getElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) {
    throw new Error(`Missing element for selector: ${selector}`);
  }
  return element;
}

const modeEyebrow = getElement<HTMLElement>("#mode-eyebrow");
const modeHeading = getElement<HTMLElement>("#mode-heading");
const modeCopy = getElement<HTMLElement>("#mode-copy");
const modeButtons = Array.from(
  document.querySelectorAll<HTMLButtonElement>("[data-mode]"),
);
const workflowPanels = Array.from(
  document.querySelectorAll<HTMLElement>("[data-panel]"),
);
const selectedPath = getElement<HTMLElement>("#selected-path");
const pickFolderButton = getElement<HTMLButtonElement>("#pick-folder");
const requestRunButton = getElement<HTMLButtonElement>("#request-run");
const revealFolderButton =
  getElement<HTMLButtonElement>("#reveal-folder");
const clearSelectionButton =
  getElement<HTMLButtonElement>("#clear-selection");
const processLog = getElement<HTMLElement>("#process-log");
const processLogStatus =
  getElement<HTMLElement>("#process-log-status");
const bundleStatus = getElement<HTMLElement>("#bundle-status");
const bundleSummaryTitle =
  getElement<HTMLElement>("#bundle-summary-title");
const bundleSummary = getElement<HTMLElement>("#bundle-summary");
const imageOutputPath = getElement<HTMLElement>("#image-output-path");
const gzipOutputPath = getElement<HTMLElement>("#gzip-output-path");
const artifactHash = getElement<HTMLElement>("#artifact-hash");
const runRequestDetails =
  getElement<HTMLElement>("#run-request-details");
const runResultStatus =
  getElement<HTMLElement>("#run-result-status");
const runResultOutput =
  getElement<HTMLElement>("#run-result-output");
const artifactPublicLink =
  getElement<HTMLAnchorElement>("#artifact-public-link");
const detectedStack = getElement<HTMLElement>("#detected-stack");
const imageTag = getElement<HTMLElement>("#image-tag");
const imageSize = getElement<HTMLElement>("#image-size");
const dockerSetupSource =
  getElement<HTMLElement>("#docker-setup-source");
const buildNotes = getElement<HTMLUListElement>("#build-notes");
const addCommandButton =
  getElement<HTMLButtonElement>("#add-command");
const commandList =
  getElement<HTMLElement>("#command-list");
const contributorWorkerNameInput =
  getElement<HTMLInputElement>("#contributor-worker-name");
const contributorWorkerPasswordInput =
  getElement<HTMLInputElement>("#contributor-worker-password");
const contributorRegisterSubmit =
  getElement<HTMLButtonElement>("#contributor-register-submit");
const contributorLoginSubmit =
  getElement<HTMLButtonElement>("#contributor-login-submit");
const contributorAuthCard =
  getElement<HTMLElement>("#contributor-auth-card");
const contributorSetupStatus =
  getElement<HTMLElement>("#contributor-setup-status");
const contributorProfileCard =
  getElement<HTMLElement>("#contributor-profile-card");
const contributeButton =
  getElement<HTMLButtonElement>("#contribute-button");
const profileWorkerStatus =
  getElement<HTMLElement>("#profile-worker-status");
const profileWorkerResources =
  getElement<HTMLElement>("#profile-worker-resources");
const profileWorkerLoad =
  getElement<HTMLElement>("#profile-worker-load");
const profileWorkerCapabilities =
  getElement<HTMLElement>("#profile-worker-capabilities");
const profileWorkerHeartbeat =
  getElement<HTMLElement>("#profile-worker-heartbeat");
const profileWorkerStats =
  getElement<HTMLElement>("#profile-worker-stats");

const state: {
  mode: Mode;
  selectedPath: string;
  buildStatus: BuildStatus;
  buildResult: DockerImageResult | null;
  runResult: RunRequestResult | null;
  jobResult: RunRequestJobResult | null;
  resultTerminalLogged: boolean;
  error: string;
  cmdlist: string[];
  contributorWorker: ContributorWorkerRecord | null;
  contributorWorkerLoaded: boolean;
  contributorSetupLoading: boolean;
  contributorSetupMessage: string;
  contributorSetupError: string;
  logEntries: ProcessLogEntry[];
  logStage: string;
} = {
  mode: "user",
  selectedPath: "",
  buildStatus: "idle",
  buildResult: null,
  runResult: null,
  jobResult: null,
  resultTerminalLogged: false,
  error: "",
  cmdlist: [""],
  contributorWorker: null,
  contributorWorkerLoaded: false,
  contributorSetupLoading: false,
  contributorSetupMessage: "",
  contributorSetupError: "",
  logEntries: [],
  logStage: "Idle",
};

function formatBytes(bytes: number): string {
  if (bytes === 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1,
  );
  const value = bytes / 1024 ** index;
  const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${units[index]}`;
}

function formatUnixTime(unixSeconds: number): string {
  if (!unixSeconds) {
    return "Unknown time";
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(unixSeconds * 1000));
}

function triggerModeChipBurst(button: HTMLButtonElement): void {
  const activeTimer = modeBurstTimers.get(button);
  if (activeTimer) {
    window.clearTimeout(activeTimer);
  }

  button.classList.remove("is-bursting");
  void button.offsetWidth;
  button.classList.add("is-bursting");

  const timer = window.setTimeout(() => {
    button.classList.remove("is-bursting");
    modeBurstTimers.delete(button);
  }, 720);

  modeBurstTimers.set(button, timer);
}

function renderLink(
  element: HTMLAnchorElement,
  url: string,
  placeholder: string,
): void {
  if (url) {
    element.href = url;
    element.textContent = url;
    element.classList.remove("is-placeholder", "muted");
    return;
  }

  element.removeAttribute("href");
  element.textContent = placeholder;
  element.classList.add("is-placeholder", "muted");
}

function renderCommandList(): void {
  commandList.replaceChildren();

  state.cmdlist.forEach((command, index) => {
    const row = document.createElement("div");
    row.className = "command-row";

    const removeButton = document.createElement("button");
    removeButton.className = "command-remove";
    removeButton.type = "button";
    removeButton.setAttribute("aria-label", `Delete command ${index + 1}`);
    removeButton.textContent = "×";
    removeButton.addEventListener("click", () => {
      state.cmdlist.splice(index, 1);
      if (state.cmdlist.length === 0) {
        state.cmdlist.push("");
      }
      renderCommandList();
    });

    const input = document.createElement("input");
    input.className = "command-input";
    input.type = "text";
    input.placeholder = `Command ${index + 1}`;
    input.value = command;
    input.setAttribute("aria-label", `Command ${index + 1}`);
    input.addEventListener("input", (event) => {
      const target = event.currentTarget as HTMLInputElement;
      state.cmdlist[index] = target.value;
    });

    row.append(removeButton, input);
    commandList.append(row);
  });
}

function statusLabel(status: BuildStatus): string {
  switch (status) {
    case "idle":
      return "Idle";
    case "ready":
      return "Ready";
    case "working":
      return "Processing";
    case "done":
      return "Queued";
    case "error":
      return "Attention";
  }
}

function statusHeadline(status: BuildStatus): string {
  switch (status) {
    case "idle":
      return "Waiting for a folder";
    case "ready":
      return "Ready to request a run";
    case "working":
      return "Building the Docker image and queueing the run";
    case "done":
      return "Run request queued successfully";
    case "error":
      return "Run request needs attention";
  }
}

function statusSummary(): string {
  if (state.error) {
    return "There is an error. Check the terminal for more details.";
  }

  if (state.runResult) {
    return state.runResult.summary;
  }

  if (state.buildResult) {
    return state.buildResult.summary;
  }

  if (state.buildStatus === "ready") {
    return "The folder is selected. Request for run will build the image, compress it, upload it, and queue the run.";
  }

  if (state.buildStatus === "working") {
    return "ComputeHive is preparing the Docker image, packaging the artifact, and queueing the run.";
  }

  return "Pick a project directory to prepare the Docker image and queue the run.";
}

function renderBuildNotes(notes: string[]): void {
  buildNotes.replaceChildren();

  if (notes.length === 0) {
    const item = document.createElement("li");
    item.textContent =
      "ComputeHive generates Docker setup automatically when it is missing.";
    buildNotes.append(item);
    return;
  }

  notes.forEach((note) => {
    const item = document.createElement("li");
    item.textContent = note;
    buildNotes.append(item);
  });
}

function processLogToneForStatus(status: BuildStatus): string {
  switch (status) {
    case "working":
      return "working";
    case "done":
      return "done";
    case "error":
      return "error";
    default:
      return "idle";
  }
}

function processLogTimestamp(): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date());
}

function createProcessLogLine(entry: ProcessLogEntry): HTMLElement {
  const line = document.createElement("div");
  line.className = `terminal-line terminal-line-${entry.tone}`;

  const time = document.createElement("span");
  time.className = "terminal-time";
  time.textContent = entry.timestamp;

  const prompt = document.createElement("span");
  prompt.className = "terminal-prompt";
  prompt.textContent = entry.tone === "error" ? "!" : entry.tone === "success" ? "+" : ">";

  const message = document.createElement("p");
  message.className = "terminal-message";
  message.textContent = entry.message;

  line.append(time, prompt, message);
  return line;
}

function renderProcessLog(forceReset = false): void {
  processLogStatus.textContent = state.logStage;
  processLogStatus.className =
    `terminal-status terminal-status-${processLogToneForStatus(state.buildStatus)}`;

  if (forceReset || state.logEntries.length < renderedProcessLogCount) {
    processLog.replaceChildren();
    renderedProcessLogCount = 0;
  }

  if (state.logEntries.length === 0) {
    const placeholder = document.createElement("p");
    placeholder.className = "terminal-placeholder";
    placeholder.textContent =
      state.selectedPath.length > 0
        ? "Request for run will build the Docker image, compress it, upload it, and queue the job."
        : "Select a project folder to let ComputeHive build the Docker image and request the run.";
    processLog.replaceChildren(placeholder);
    renderedProcessLogCount = 0;
    return;
  }

  if (renderedProcessLogCount === 0) {
    processLog.replaceChildren();
  }

  while (renderedProcessLogCount < state.logEntries.length) {
    processLog.append(
      createProcessLogLine(state.logEntries[renderedProcessLogCount]),
    );
    renderedProcessLogCount += 1;
  }

  processLog.scrollTop = processLog.scrollHeight;
}

function addProcessLog(
  tone: ProcessLogTone,
  message: string,
): void {
  state.logEntries.push({
    id: nextProcessLogId,
    tone,
    message,
    timestamp: processLogTimestamp(),
  });
  nextProcessLogId += 1;
  renderProcessLog();
}

function clearProcessLogTimers(): void {
  processLogTimers.forEach((timer) => window.clearTimeout(timer));
  processLogTimers.clear();
}

function scheduleProcessLog(
  delayMs: number,
  tone: ProcessLogTone,
  message: string,
): void {
  const timer = window.setTimeout(() => {
    processLogTimers.delete(timer);
    addProcessLog(tone, message);
  }, delayMs);

  processLogTimers.add(timer);
}

function resetProcessLog(stage: string): void {
  clearProcessLogTimers();
  state.logStage = stage;
  state.logEntries = [];
  renderProcessLog(true);
}

function stageRequestLog(): void {
  resetProcessLog("Preparing");
  addProcessLog("info", `Target folder: ${state.selectedPath}`);
  addProcessLog("working", "Inspecting the selected project root.");
  scheduleProcessLog(
    180,
    "working",
    "Checking whether a Dockerfile already exists.",
  );
  scheduleProcessLog(
    380,
    "working",
    "Checking whether a .dockerignore file already exists.",
  );
  addProcessLog(
    "working",
    "Resolving the project stack and build strategy.",
  );
  scheduleProcessLog(
    720,
    "working",
    "Generating missing Docker setup files when required.",
  );
  scheduleProcessLog(
    1240,
    "working",
    "Building the Docker image.",
  );
  scheduleProcessLog(
    1820,
    "working",
    "Exporting the Docker image archive into the project root.",
  );
  scheduleProcessLog(
    2420,
    "working",
    "Compressing the archive to tar.gz.",
  );
  scheduleProcessLog(
    3020,
    "working",
    "Calculating the SHA-256 artifact hash.",
  );
  scheduleProcessLog(
    3620,
    "working",
    "Uploading the artifact to object storage.",
  );
  scheduleProcessLog(
    4220,
    "working",
    "Submitting the job to the coordinator gRPC server.",
  );
}

function appendResultLogs(result: RunRequestResult): void {
  clearProcessLogTimers();
  state.logStage = "Queued";
  addProcessLog("success", `Docker image ready: ${result.image.imageTag}`);
  addProcessLog(
    "success",
    `Compressed artifact stored at ${result.gzipArchivePath}.`,
  );
  addProcessLog(
    "success",
    `Artifact SHA-256 recorded as ${result.artifactSha256}.`,
  );
  addProcessLog(
    "success",
    `Coordinator accepted job ${result.redisJobId} with object key ${result.artifactObjectKey}.`,
  );

  const seen = new Set<string>();
  result.notes.forEach((note) => {
    if (seen.has(note)) {
      return;
    }
    seen.add(note);
    addProcessLog(note.includes("No container") ? "info" : "success", note);
  });
}

function stopJobResultPolling(): void {
  if (jobResultPollTimer !== null) {
    window.clearInterval(jobResultPollTimer);
    jobResultPollTimer = null;
  }
}

async function refreshRunRequestResult(jobId: string): Promise<void> {
  try {
    const result = await invoke<RunRequestJobResult | null>(
      "get_run_request_result",
      { jobId },
    );

    if (!result) {
      return;
    }

    state.jobResult = result;

    if (result.completed && !state.resultTerminalLogged) {
      state.resultTerminalLogged = true;

      if (result.resultStatus === "STATUS_SUCCEEDED") {
        state.logStage = "Completed";
        addProcessLog("success", `Job ${result.jobId} finished successfully.`);
      } else if (result.resultStatus === "STATUS_FAILED") {
        state.logStage = "Failed";
        addProcessLog(
          "error",
          `Job ${result.jobId} failed with exit code ${result.exitCode}.`,
        );
      } else {
        addProcessLog(
          "info",
          `Job ${result.jobId} status changed to ${result.status || "unknown"}.`,
        );
      }

      const excerpt = result.stdoutExcerpt.trim() || result.stderrExcerpt.trim();
      if (excerpt) {
        addProcessLog(
          result.resultStatus === "STATUS_FAILED" ? "error" : "success",
          `Worker output: ${excerpt}`,
        );
      }
    }

    if (result.completed) {
      stopJobResultPolling();
    }

    renderUserState();
  } catch {
    // Keep polling; transient errors should not break the flow.
  }
}

function startJobResultPolling(jobId: string): void {
  stopJobResultPolling();
  void refreshRunRequestResult(jobId);
  jobResultPollTimer = window.setInterval(() => {
    void refreshRunRequestResult(jobId);
  }, 3000);
}

function renderContributorSetup(): void {
  contributorRegisterSubmit.disabled = state.contributorSetupLoading;
  contributorLoginSubmit.disabled = state.contributorSetupLoading;
  contributeButton.disabled =
    state.contributorSetupLoading || !state.contributorWorker;
  contributorRegisterSubmit.textContent = state.contributorSetupLoading
    ? "Registering worker..."
    : "Register worker";
  contributorLoginSubmit.textContent = state.contributorSetupLoading
    ? "Checking device..."
    : "Login worker";

  if (state.contributorSetupError) {
    contributorSetupStatus.textContent = state.contributorSetupError;
    contributorSetupStatus.classList.add("contributor-status-error");
    contributorSetupStatus.classList.remove("contributor-status-success");
  } else if (state.contributorSetupMessage) {
    contributorSetupStatus.textContent = state.contributorSetupMessage;
    contributorSetupStatus.classList.add("contributor-status-success");
    contributorSetupStatus.classList.remove("contributor-status-error");
  } else {
    contributorSetupStatus.textContent =
      "Register this machine to create a worker record in Redis.";
    contributorSetupStatus.classList.remove(
      "contributor-status-success",
      "contributor-status-error",
    );
  }

  const worker = state.contributorWorker;
  contributorAuthCard.hidden = !!worker;
  contributorProfileCard.hidden = !worker;
  if (!worker) {
    profileWorkerStatus.textContent = "Pending";
    profileWorkerResources.textContent =
      "CPU: - · Memory: - · GPU: -";
    profileWorkerLoad.textContent =
      "CPU used: - · Memory used: -";
    profileWorkerCapabilities.textContent =
      "Docker: - · GPU supported: -";
    profileWorkerHeartbeat.textContent = "Pending";
    profileWorkerStats.textContent =
      "Completed: - · Failed: -";
    return;
  }

  profileWorkerStatus.textContent = worker.status;
  profileWorkerResources.textContent =
    `CPU: ${worker.resources.cpu_cores} cores · Memory: ${worker.resources.memory_mb} MB · GPU: ${worker.resources.gpu ? "Yes" : "No"}`;
  profileWorkerLoad.textContent =
    `CPU used: ${worker.current_load.cpu_used} · Memory used: ${worker.current_load.memory_used} MB`;
  profileWorkerCapabilities.textContent =
    `Docker: ${worker.capabilities.docker ? "Yes" : "No"} · GPU supported: ${worker.capabilities.gpu_supported ? "Yes" : "No"}`;
  profileWorkerHeartbeat.textContent =
    formatUnixTime(worker.last_heartbeat);
  profileWorkerStats.textContent =
    `Completed: ${worker.stats.jobs_completed} · Failed: ${worker.stats.jobs_failed}`;
}

function renderMode(): void {
  const copy = interfaceCopy[state.mode];
  modeEyebrow.textContent = copy.eyebrow;
  modeHeading.textContent = copy.heading;
  modeCopy.textContent = copy.copy;

  modeButtons.forEach((button) => {
    const active = button.dataset.mode === state.mode;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", String(active));
  });

  workflowPanels.forEach((panel) => {
    const active = panel.dataset.panel === state.mode;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  });

  renderContributorSetup();
}

function renderUserState(): void {
  selectedPath.textContent = state.selectedPath || "No folder selected yet";
  selectedPath.classList.toggle("is-empty", state.selectedPath.length === 0);

  const canAct =
    state.selectedPath.length > 0 && state.buildStatus !== "working";
  requestRunButton.disabled = !canAct;
  revealFolderButton.disabled = state.selectedPath.length === 0;
  pickFolderButton.disabled = state.buildStatus === "working";
  clearSelectionButton.disabled = state.buildStatus === "working";

  requestRunButton.textContent =
    state.buildStatus === "working"
      ? "Requesting run..."
      : "Request for run";

  bundleStatus.className = `status-pill status-${state.buildStatus}`;
  bundleStatus.textContent = statusLabel(state.buildStatus);
  bundleSummaryTitle.textContent = statusHeadline(state.buildStatus);
  bundleSummary.textContent = statusSummary();
  renderProcessLog();

  const imageDetails = state.buildResult ?? state.runResult?.image ?? null;
  if (imageDetails) {
    imageOutputPath.textContent = imageDetails.imageArchivePath;
    detectedStack.textContent = imageDetails.detectedStack;
    imageTag.textContent = imageDetails.imageTag;
    imageSize.textContent = formatBytes(imageDetails.imageSizeBytes);
    dockerSetupSource.textContent = imageDetails.dockerSetupSource;
  } else {
    imageOutputPath.textContent =
      "The Docker image archive path will appear here after the build.";
    detectedStack.textContent = "Not detected yet";
    imageTag.textContent = "Not built yet";
    imageSize.textContent = "0 B";
    dockerSetupSource.textContent = "Auto-generate if needed";
  }

  if (state.runResult) {
    gzipOutputPath.textContent = `${state.runResult.gzipArchivePath} (${formatBytes(state.runResult.gzipSizeBytes)})`;
    artifactHash.textContent = state.runResult.artifactSha256;
    runRequestDetails.textContent =
      `${state.runResult.redisJobId} · ${state.runResult.artifactObjectKey}`;

    if (state.jobResult) {
      const finishedText = state.jobResult.finishedAtUnix
        ? ` · finished ${formatUnixTime(state.jobResult.finishedAtUnix)}`
        : "";
      runResultStatus.textContent =
        `${state.jobResult.resultStatus || state.jobResult.status || "queued"} · exit ${state.jobResult.exitCode}${finishedText}`;

      const outputText =
        state.jobResult.stdoutExcerpt || state.jobResult.stderrExcerpt;
      runResultOutput.textContent =
        outputText || "No stdout/stderr excerpt is available yet.";
      runResultOutput.classList.toggle("muted", outputText.length === 0);
    } else {
      runResultStatus.textContent =
        "Queued. Waiting for worker to start and submit output.";
      runResultOutput.textContent =
        "Waiting for job execution output.";
      runResultOutput.classList.add("muted");
    }

    renderLink(
      artifactPublicLink,
      state.runResult.artifactPublicUrl,
      "The public artifact link will appear here after upload.",
    );
    renderBuildNotes(state.runResult.notes);
    return;
  }

  gzipOutputPath.textContent =
    "The tar.gz artifact path will appear here after a run request.";
  artifactHash.textContent =
    "The SHA-256 hash will appear here after upload.";
  runRequestDetails.textContent =
    "The Redis job id and object storage key will appear here after you request a run.";
  runResultStatus.textContent =
    "Worker output will appear here after the job is completed.";
  runResultOutput.textContent =
    "Waiting for job execution output.";
  runResultOutput.classList.add("muted");
  renderLink(
    artifactPublicLink,
    "",
    "The public artifact link will appear here after upload.",
  );

  if (imageDetails) {
    renderBuildNotes(imageDetails.notes);
    return;
  }

  renderBuildNotes([
    "The app creates Docker setup files automatically when they are missing.",
    "Request for run first builds the Docker image inside the selected project root.",
    "The app uploads only the tar.gz image artifact after compression.",
    "No container is created by this action in the current build.",
  ]);
}

function openSelectedFolder(): void {
  if (!state.selectedPath) {
    return;
  }

  addProcessLog("working", "Opening the selected project folder in the system file browser.");
  openPath(state.selectedPath)
    .then(() => {
      addProcessLog("success", "Selected project folder opened.");
    })
    .catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      addProcessLog("error", `Could not open the selected folder: ${message}`);
    });
}

function pickProjectFolder(): void {
  state.error = "";
  resetProcessLog("Picker");
  addProcessLog("working", "Opening the native folder picker.");
  renderUserState();

  open({
    directory: true,
    multiple: false,
    title: "Select the project folder to Dockerize and queue",
  })
    .then((folder) => {
      if (typeof folder !== "string") {
        state.logStage = "Idle";
        addProcessLog("info", "Folder selection was canceled.");
        renderUserState();
        return;
      }

      state.selectedPath = folder;
      state.buildStatus = "ready";
      state.buildResult = null;
      state.runResult = null;
      state.error = "";
      resetProcessLog("Ready");
      addProcessLog("info", `Project folder selected: ${folder}`);
      addProcessLog(
        "info",
        "Request for run will handle Docker setup, image export, compression, upload, and coordinator submission in one pass.",
      );
      renderUserState();
    })
    .catch((error) => {
      state.buildStatus = state.selectedPath ? "ready" : "idle";
      state.error = error instanceof Error ? error.message : String(error);
      state.logStage = "Picker Error";
      addProcessLog("error", `Folder picker failed: ${state.error}`);
      renderUserState();
    });
}

async function requestProjectRun(): Promise<void> {
  if (!state.selectedPath) {
    return;
  }

  const command = state.cmdlist
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  if (command.length === 0) {
    state.error = "Add at least one command line before requesting a run.";
    state.buildStatus = "error";
    state.logStage = "Validation";
    addProcessLog("error", state.error);
    renderUserState();
    return;
  }

  state.buildStatus = "working";
  state.buildResult = null;
  state.runResult = null;
  state.jobResult = null;
  state.resultTerminalLogged = false;
  state.error = "";
  stopJobResultPolling();
  stageRequestLog();
  renderUserState();

  try {
    const result = await invoke<RunRequestResult>("request_project_run", {
      projectPath: state.selectedPath,
      command,
    });

    state.buildResult = result.image;
    state.runResult = result;
    state.buildStatus = "done";
    appendResultLogs(result);
    startJobResultPolling(result.redisJobId);
  } catch (error) {
    clearProcessLogTimers();
    state.error = error instanceof Error ? error.message : String(error);
    state.buildStatus = "error";
    state.logStage = "Failed";
    addProcessLog("error", state.error);
  }

  renderUserState();
}

async function loadContributorProfile(): Promise<void> {
  try {
    const worker = await invoke<ContributorWorkerRecord | null>(
      "get_registered_contributor_worker",
    );
    state.contributorWorker = worker;
    state.contributorWorkerLoaded = true;
    if (worker && !state.contributorSetupMessage) {
      state.contributorSetupMessage =
        "A worker record already exists for this machine.";
    }
  } catch (error) {
    state.contributorWorker = null;
    state.contributorSetupError =
      error instanceof Error ? error.message : String(error);
  }

  renderContributorSetup();
}

async function registerContributorWorker(): Promise<void> {
  const workerName = contributorWorkerNameInput.value.trim();
  const password = contributorWorkerPasswordInput.value.trim();

  if (!workerName || !password) {
    state.contributorSetupError =
      "Worker name and password are required.";
    state.contributorSetupMessage = "";
    renderContributorSetup();
    return;
  }

  state.contributorSetupLoading = true;
  state.contributorSetupError = "";
  state.contributorSetupMessage = "";
  renderContributorSetup();

  try {
    const worker = await invoke<ContributorWorkerRecord>(
      "register_contributor_worker",
      {
        workerName,
        password,
      },
    );

    state.contributorWorker = worker;
    state.contributorWorkerLoaded = true;
    state.contributorSetupMessage =
      "Worker registered on this device. The local worker hash was saved.";
  } catch (error) {
    state.contributorSetupError =
      error instanceof Error ? error.message : String(error);
  } finally {
    state.contributorSetupLoading = false;
    renderContributorSetup();
  }
}

async function loginContributorWorker(): Promise<void> {
  const workerName = contributorWorkerNameInput.value.trim();
  const password = contributorWorkerPasswordInput.value.trim();

  if (!workerName || !password) {
    state.contributorSetupError =
      "Worker name and password are required.";
    state.contributorSetupMessage = "";
    renderContributorSetup();
    return;
  }

  state.contributorSetupLoading = true;
  state.contributorSetupError = "";
  state.contributorSetupMessage = "";
  renderContributorSetup();

  try {
    const worker = await invoke<ContributorWorkerRecord>(
      "login_contributor_worker",
      {
        workerName,
        password,
      },
    );
    state.contributorWorker = worker;
    state.contributorWorkerLoaded = true;
    state.contributorSetupMessage =
      "Contributor worker unlocked on this device.";
  } catch (error) {
    state.contributorSetupError =
      error instanceof Error ? error.message : String(error);
  } finally {
    state.contributorSetupLoading = false;
    renderContributorSetup();
  }
}

async function activateContributorWorker(): Promise<void> {
  state.contributorSetupLoading = true;
  state.contributorSetupError = "";
  state.contributorSetupMessage = "";
  renderContributorSetup();

  try {
    const worker = await invoke<ContributorWorkerRecord>(
      "activate_contributor_worker",
    );
    state.contributorWorker = worker;
    state.contributorWorkerLoaded = true;
    state.contributorSetupMessage =
      "Worker status is active. This device is now available for compute.";
  } catch (error) {
    state.contributorSetupError =
      error instanceof Error ? error.message : String(error);
  } finally {
    state.contributorSetupLoading = false;
    renderContributorSetup();
  }
}

function loadContributorView(): void {
  if (!state.contributorWorkerLoaded) {
    void loadContributorProfile();
  }
}

function clearSelection(): void {
  stopJobResultPolling();
  clearProcessLogTimers();
  state.selectedPath = "";
  state.buildStatus = "idle";
  state.buildResult = null;
  state.runResult = null;
  state.jobResult = null;
  state.resultTerminalLogged = false;
  state.error = "";
  state.logStage = "Idle";
  state.logEntries = [];
  renderUserState();
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    triggerModeChipBurst(button);
    const nextMode = button.dataset.mode;
    if (nextMode === "user" || nextMode === "contributor") {
      state.mode = nextMode;
      renderMode();
      if (nextMode === "contributor") {
        loadContributorView();
      }
    }
  });
});

pickFolderButton.addEventListener("click", () => {
  pickProjectFolder();
});

requestRunButton.addEventListener("click", () => {
  void requestProjectRun();
});

revealFolderButton.addEventListener("click", () => {
  openSelectedFolder();
});

contributorRegisterSubmit.addEventListener("click", () => {
  void registerContributorWorker();
});

contributorLoginSubmit.addEventListener("click", () => {
  void loginContributorWorker();
});

contributeButton.addEventListener("click", () => {
  void activateContributorWorker();
});

clearSelectionButton.addEventListener("click", clearSelection);

renderMode();
renderUserState();
renderContributorSetup();
renderCommandList();

addCommandButton.addEventListener("click", () => {
  state.cmdlist.push("");
  renderCommandList();
});
