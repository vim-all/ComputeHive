import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { openPath, openUrl } from "@tauri-apps/plugin-opener";

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
  outputApiUrl: string;
  outputPublicUrl: string;
  outputSizeBytes: number;
  outputArtifacts: RunRequestOutputArtifact[];
  finishedAtUnix: number;
  completed: boolean;
}

interface RunRequestOutputArtifact {
  relativePath: string;
  objectKey: string;
  uri: string;
  apiUrl: string;
  publicUrl: string;
  sha256: string;
  sizeBytes: number;
  contentType: string;
}

interface ContributorSharingStatus {
  running: boolean;
  pid: number | null;
  details: string;
  jobRunning: boolean;
  currentJobId: string | null;
  lastJobId: string | null;
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
      "Start sharing to run the local worker on this machine. Stop anytime.",
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

const runResultStatus =
  getElement<HTMLElement>("#run-result-status");
const runResultOutput =
  getElement<HTMLElement>("#run-result-output");
const runResultArtifacts =
  getElement<HTMLElement>("#run-result-artifacts");

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
const contributorSetupStatus =
  getElement<HTMLElement>("#contributor-setup-status");
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
  contributorSharing: ContributorSharingStatus | null;
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
  contributorSharing: null,
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

function renderResultArtifacts(
  container: HTMLElement,
  artifacts: RunRequestOutputArtifact[],
  placeholder: string,
): void {
  container.innerHTML = "";

  if (artifacts.length === 0) {
    container.textContent = placeholder;
    container.classList.add("muted");
    return;
  }

  container.classList.remove("muted");
  const fragment = document.createDocumentFragment();

  artifacts.forEach((artifact) => {
    const item = document.createElement("div");
    item.className = "detail-artifact-item";
    const href = artifact.publicUrl || artifact.apiUrl || artifact.uri;
    if (href) {
      const link = document.createElement("a");
      link.className = "detail-artifact-link";
      link.href = artifact.objectKey ? "#" : href;
      link.textContent =
        artifact.relativePath || artifact.objectKey || artifact.sha256;
      link.addEventListener("click", (event) => {
        event.preventDefault();
        void openResultArtifact(artifact);
      });
      item.append(link);
    } else {
      const label = document.createElement("strong");
      label.textContent =
        artifact.relativePath || artifact.objectKey || artifact.sha256;
      item.append(label);
    }

    const meta = document.createElement("p");
    meta.className = "detail-artifact-meta";
    const metaParts = [
      artifact.sizeBytes > 0 ? formatBytes(artifact.sizeBytes) : "",
      artifact.contentType || "",
      artifact.sha256 ? `sha256 ${artifact.sha256}` : "",
    ].filter((part) => part.length > 0);
    meta.textContent = metaParts.join(" · ");
    item.append(meta);

    fragment.append(item);
  });

  container.append(fragment);
}

async function openResultArtifact(
  artifact: RunRequestOutputArtifact,
): Promise<void> {
  try {
    const targetUrl = artifact.objectKey
      ? await invoke<string>("get_signed_result_artifact_url", {
        objectKey: artifact.objectKey,
      })
      : artifact.publicUrl || artifact.apiUrl || artifact.uri;

    if (!targetUrl) {
      throw new Error("No downloadable URL is available for this artifact.");
    }

    await openUrl(targetUrl);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    addProcessLog("error", `Could not open returned file: ${message}`);
  }
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
    "Submitting the job.",
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
      if ((result.outputArtifacts?.length ?? 0) > 0) {
        addProcessLog(
          "success",
          `Worker uploaded ${result.outputArtifacts.length} returned file(s) for download.`,
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
  contributeButton.disabled = state.contributorSetupLoading;
  contributeButton.textContent =
    state.contributorSharing?.running ? "Stop sharing" : "Start sharing";

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
      "Worker is ready to start.";
    contributorSetupStatus.classList.remove(
      "contributor-status-success",
      "contributor-status-error",
    );
  }

  if (state.contributorSharing?.details) {
    contributorSetupStatus.textContent = state.contributorSharing.details;
    contributorSetupStatus.classList.add("contributor-status-success");
    contributorSetupStatus.classList.remove("contributor-status-error");
  }

  const sharing = state.contributorSharing;
  profileWorkerStatus.textContent = sharing?.running ? "Sharing" : "Stopped";
  profileWorkerResources.textContent = sharing?.pid
    ? `Worker PID: ${sharing.pid}`
    : "Worker PID: not running";
  profileWorkerLoad.textContent = sharing?.jobRunning
    ? "Job state: running"
    : "Job state: idle";
  profileWorkerCapabilities.textContent = sharing?.currentJobId
    ? `Current job: ${sharing.currentJobId}`
    : "Current job: none";
  profileWorkerHeartbeat.textContent = sharing?.lastJobId
    ? `Last job: ${sharing.lastJobId}`
    : "Last job: none";
  profileWorkerStats.textContent = sharing?.details ?? "No sharing activity yet.";
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
    detectedStack.textContent = imageDetails.detectedStack;
    imageTag.textContent = imageDetails.imageTag;
    imageSize.textContent = formatBytes(imageDetails.imageSizeBytes);
    dockerSetupSource.textContent = imageDetails.dockerSetupSource;
  } else {
    detectedStack.textContent = "Not detected yet";
    imageTag.textContent = "Not built yet";
    imageSize.textContent = "0 B";
    dockerSetupSource.textContent = "Auto-generate if needed";
  }

  if (state.runResult) {
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
      renderResultArtifacts(
        runResultArtifacts,
        state.jobResult.outputArtifacts ?? [],
        "No returned files were uploaded for this job.",
      );
    } else {
      runResultStatus.textContent =
        "Queued. Waiting for worker to start and submit output.";
      runResultOutput.textContent =
        "Waiting for job execution output.";
      runResultOutput.classList.add("muted");
      renderResultArtifacts(
        runResultArtifacts,
        [],
        "Returned files will appear here after the job completes.",
      );
    }

    renderBuildNotes(state.runResult.notes);
    return;
  }

  runResultStatus.textContent =
    "Worker output will appear here after the job is completed.";
  runResultOutput.textContent =
    "Waiting for job execution output.";
  runResultOutput.classList.add("muted");
  renderResultArtifacts(
    runResultArtifacts,
    [],
    "Returned files will appear here after the job completes.",
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

async function refreshContributorSharingStatus(): Promise<void> {
  try {
    const sharing = await invoke<ContributorSharingStatus>(
      "get_contributor_sharing_status",
    );
    state.contributorSharing = sharing;
  } catch (error) {
    state.contributorSharing = {
      running: false,
      pid: null,
      details: error instanceof Error ? error.message : String(error),
      jobRunning: false,
      currentJobId: null,
      lastJobId: null,
    };
  }

  renderContributorSetup();
}

function loadContributorView(): void {
  void refreshContributorSharingStatus();
}

async function toggleContributorSharing(): Promise<void> {
  state.contributorSetupLoading = true;
  state.contributorSetupError = "";
  renderContributorSetup();

  try {
    const command = state.contributorSharing?.running
      ? "stop_contributor_sharing"
      : "start_contributor_sharing";
    const sharing = await invoke<ContributorSharingStatus>(command);
    state.contributorSharing = sharing;
    state.contributorSetupMessage = sharing.details;
  } catch (error) {
    state.contributorSetupError =
      error instanceof Error ? error.message : String(error);
  } finally {
    state.contributorSetupLoading = false;
    renderContributorSetup();
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

contributeButton.addEventListener("click", () => {
  void toggleContributorSharing();
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
