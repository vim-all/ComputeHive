import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";

type Mode = "user" | "contributor";
type BuildStatus = "idle" | "ready" | "working" | "done" | "error";
type ActionKind = "build" | "request";

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

interface IncomingRunRequest {
  jobId: string;
  projectName: string;
  detectedStack: string;
  status: string;
  containerImage: string;
  artifactSha256: string;
  artifactObjectKey: string;
  artifactUri: string;
  artifactApiUrl: string;
  artifactPublicUrl: string;
  gzipArchivePath: string;
  gzipSizeBytes: number;
  createdAtUnix: number;
  projectRoot: string;
}

interface InterfaceCopy {
  eyebrow: string;
  heading: string;
  copy: string;
}

const interfaceCopy: Record<Mode, InterfaceCopy> = {
  user: {
    eyebrow: "Project Image Flow",
    heading:
      "Build locally, then request a queued run with a hashed tar.gz artifact.",
    copy:
      "Choose a project directory and let ComputeHive generate Docker setup when it is missing. You can build the Docker image locally, then request a run so the app compresses the image to a tar.gz artifact, hashes it, uploads it to object storage, and writes the queue records into Redis.",
  },
  contributor: {
    eyebrow: "Contributor Resource Flow",
    heading:
      "Inspect the incoming run queue with image tags, artifact hashes, and contributor-facing metadata.",
    copy:
      "Use the contributor dashboard to inspect queued run requests pulled from Redis. Each queued request includes the uploaded tar.gz artifact details and the matching SHA-256 hash that contributors can verify before future contributor actions load and run the image.",
  },
};

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
const createImageButton = getElement<HTMLButtonElement>("#create-image");
const requestRunButton = getElement<HTMLButtonElement>("#request-run");
const clearSelectionButton =
  getElement<HTMLButtonElement>("#clear-selection");
const bundleStatus = getElement<HTMLElement>("#bundle-status");
const bundleSummaryTitle =
  getElement<HTMLElement>("#bundle-summary-title");
const bundleSummary = getElement<HTMLElement>("#bundle-summary");
const imageOutputPath = getElement<HTMLElement>("#image-output-path");
const gzipOutputPath = getElement<HTMLElement>("#gzip-output-path");
const artifactHash = getElement<HTMLElement>("#artifact-hash");
const runRequestDetails =
  getElement<HTMLElement>("#run-request-details");
const artifactPublicLink =
  getElement<HTMLAnchorElement>("#artifact-public-link");
const detectedStack = getElement<HTMLElement>("#detected-stack");
const imageTag = getElement<HTMLElement>("#image-tag");
const imageSize = getElement<HTMLElement>("#image-size");
const dockerSetupSource =
  getElement<HTMLElement>("#docker-setup-source");
const buildNotes = getElement<HTMLUListElement>("#build-notes");
const workerQueue = getElement<HTMLElement>("#worker-queue");
const refreshWorkersButton =
  getElement<HTMLButtonElement>("#refresh-workers");

const state: {
  mode: Mode;
  selectedPath: string;
  buildStatus: BuildStatus;
  activeAction: ActionKind;
  buildResult: DockerImageResult | null;
  runResult: RunRequestResult | null;
  error: string;
  contributorRequests: IncomingRunRequest[];
  contributorLoading: boolean;
  contributorError: string;
} = {
  mode: "user",
  selectedPath: "",
  buildStatus: "idle",
  activeAction: "build",
  buildResult: null,
  runResult: null,
  error: "",
  contributorRequests: [],
  contributorLoading: false,
  contributorError: "",
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

function shortHash(hash: string): string {
  if (hash.length <= 16) {
    return hash;
  }

  return `${hash.slice(0, 12)}…${hash.slice(-8)}`;
}

function createQueueInfoCard(
  titleText: string,
  bodyText: string,
): HTMLElement {
  const card = document.createElement("article");
  card.className = "queue-card queue-card-empty";

  const title = document.createElement("h3");
  title.textContent = titleText;

  const body = document.createElement("p");
  body.className = "support-copy";
  body.textContent = bodyText;

  card.append(title, body);
  return card;
}

function appendQueueMetric(
  parent: HTMLElement,
  labelText: string,
  valueText: string,
): void {
  const block = document.createElement("div");
  const label = document.createElement("span");
  label.textContent = labelText;
  const value = document.createElement("strong");
  value.textContent = valueText;
  block.append(label, value);
  parent.append(block);
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

function statusLabel(status: BuildStatus): string {
  switch (status) {
    case "idle":
      return "Idle";
    case "ready":
      return "Ready";
    case "working":
      return state.activeAction === "request" ? "Queueing" : "Building";
    case "done":
      return state.runResult ? "Queued" : "Built";
    case "error":
      return "Attention";
  }
}

function statusHeadline(status: BuildStatus): string {
  switch (status) {
    case "idle":
      return "Waiting for a folder";
    case "ready":
      return "Ready to build or request a run";
    case "working":
      return state.activeAction === "request"
        ? "Preparing the tar.gz artifact and queueing the run"
        : "Building the Docker image";
    case "done":
      return state.runResult
        ? "Run request queued successfully"
        : "Docker image created successfully";
    case "error":
      return state.activeAction === "request"
        ? "Run request needs attention"
        : "Docker image creation needs attention";
  }
}

function statusSummary(): string {
  if (state.error) {
    return state.error;
  }

  if (state.runResult) {
    return state.runResult.summary;
  }

  if (state.buildResult) {
    return state.buildResult.summary;
  }

  if (state.buildStatus === "ready") {
    return "The folder is selected. ComputeHive can either build the Docker image locally or build it, compress it to tar.gz, upload it, and queue the related Redis records for a contributor run.";
  }

  if (state.buildStatus === "working") {
    return state.activeAction === "request"
      ? "ComputeHive is generating Docker setup if needed, building the image, compressing it to tar.gz, hashing the artifact, uploading it to object storage, and writing the Redis queue records."
      : "ComputeHive is checking the project for Docker setup, generating missing Docker files, then building and exporting the image into the selected project root.";
  }

  return "Pick a project directory and let the app build locally or request a queued run with an uploaded tar.gz artifact.";
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

function renderContributorQueue(): void {
  workerQueue.replaceChildren();
  refreshWorkersButton.disabled = state.contributorLoading;
  refreshWorkersButton.textContent = state.contributorLoading
    ? "Refreshing queue..."
    : "Refresh queue";

  if (state.contributorLoading) {
    workerQueue.append(
      createQueueInfoCard(
        "Loading queued run requests",
        "ComputeHive is reading the current Redis queue and artifact metadata.",
      ),
    );
    return;
  }

  if (state.contributorError) {
    workerQueue.append(
      createQueueInfoCard("Queue load failed", state.contributorError),
    );
    return;
  }

  if (state.contributorRequests.length === 0) {
    workerQueue.append(
      createQueueInfoCard(
        "No queued run requests yet",
        "Use Request for run in the project user flow to create the Redis job record, artifact metadata record, and queue entry.",
      ),
    );
    return;
  }

  state.contributorRequests.forEach((worker) => {
    const card = document.createElement("article");
    card.className = "queue-card";

    const topRow = document.createElement("div");
    topRow.className = "queue-top";

    const titleWrap = document.createElement("div");
    const title = document.createElement("h3");
    title.textContent = worker.projectName;
    const subhead = document.createElement("p");
    subhead.className = "queue-owner";
    subhead.textContent = `${worker.jobId} · queued ${formatUnixTime(worker.createdAtUnix)}`;
    titleWrap.append(title, subhead);

    const badge = document.createElement("span");
    badge.className = "queue-badge";
    badge.textContent = worker.status;

    topRow.append(titleWrap, badge);

    const meta = document.createElement("div");
    meta.className = "queue-meta";
    appendQueueMetric(meta, "Stack", worker.detectedStack);
    appendQueueMetric(meta, "Artifact", formatBytes(worker.gzipSizeBytes));
    appendQueueMetric(meta, "Image tag", worker.containerImage);
    appendQueueMetric(meta, "Hash", shortHash(worker.artifactSha256));
    appendQueueMetric(meta, "Object key", worker.artifactObjectKey || "Pending");
    appendQueueMetric(meta, "Project root", worker.projectRoot || "Unknown");

    if (worker.artifactPublicUrl) {
      const linkRow = document.createElement("p");
      linkRow.className = "queue-link";
      const label = document.createElement("span");
      label.className = "queue-link-label";
      label.textContent = "Public link";
      const link = document.createElement("a");
      link.href = worker.artifactPublicUrl;
      link.target = "_blank";
      link.rel = "noreferrer";
      link.textContent = worker.artifactPublicUrl;
      linkRow.append(label, link);
      card.append(topRow, meta, linkRow);
    } else {
      card.append(topRow, meta);
    }
    workerQueue.append(card);
  });
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
}

function renderUserState(): void {
  selectedPath.textContent = state.selectedPath || "No folder selected yet";
  selectedPath.classList.toggle("is-empty", state.selectedPath.length === 0);

  const canAct =
    state.selectedPath.length > 0 && state.buildStatus !== "working";
  createImageButton.disabled = !canAct;
  requestRunButton.disabled = !canAct;
  pickFolderButton.disabled = state.buildStatus === "working";
  clearSelectionButton.disabled = state.buildStatus === "working";

  createImageButton.textContent =
    state.buildStatus === "working" && state.activeAction === "build"
      ? "Creating Docker image..."
      : "Create Docker image";
  requestRunButton.textContent =
    state.buildStatus === "working" && state.activeAction === "request"
      ? "Requesting run..."
      : "Request for run";

  bundleStatus.className = `status-pill status-${state.buildStatus}`;
  bundleStatus.textContent = statusLabel(state.buildStatus);
  bundleSummaryTitle.textContent = statusHeadline(state.buildStatus);
  bundleSummary.textContent = statusSummary();

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
    "The local build action does not upload anything.",
    "The run request action uploads only the tar.gz image artifact.",
    "No container is created by either action in this build.",
  ]);
}

async function pickProjectFolder(): Promise<void> {
  const folder = await open({
    directory: true,
    multiple: false,
    title: "Select the project folder to Dockerize and queue",
  });

  if (typeof folder !== "string") {
    return;
  }

  state.selectedPath = folder;
  state.buildStatus = "ready";
  state.buildResult = null;
  state.runResult = null;
  state.error = "";
  renderUserState();
}

async function createProjectDockerImage(): Promise<void> {
  if (!state.selectedPath) {
    return;
  }

  state.activeAction = "build";
  state.buildStatus = "working";
  state.buildResult = null;
  state.runResult = null;
  state.error = "";
  renderUserState();

  try {
    const result = await invoke<DockerImageResult>(
      "create_project_docker_image",
      {
        projectPath: state.selectedPath,
      },
    );

    state.buildResult = result;
    state.buildStatus = "done";
  } catch (error) {
    state.error = error instanceof Error ? error.message : String(error);
    state.buildStatus = "error";
  }

  renderUserState();
}

async function requestProjectRun(): Promise<void> {
  if (!state.selectedPath) {
    return;
  }

  state.activeAction = "request";
  state.buildStatus = "working";
  state.buildResult = null;
  state.runResult = null;
  state.error = "";
  renderUserState();

  try {
    const result = await invoke<RunRequestResult>("request_project_run", {
      projectPath: state.selectedPath,
    });

    state.buildResult = result.image;
    state.runResult = result;
    state.buildStatus = "done";
    void refreshContributorQueue();
  } catch (error) {
    state.error = error instanceof Error ? error.message : String(error);
    state.buildStatus = "error";
  }

  renderUserState();
}

async function refreshContributorQueue(): Promise<void> {
  state.contributorLoading = true;
  state.contributorError = "";
  renderContributorQueue();

  try {
    const requests = await invoke<IncomingRunRequest[]>(
      "list_incoming_run_requests",
    );
    state.contributorRequests = requests;
  } catch (error) {
    state.contributorRequests = [];
    state.contributorError =
      error instanceof Error ? error.message : String(error);
  } finally {
    state.contributorLoading = false;
    renderContributorQueue();
  }
}

function clearSelection(): void {
  state.selectedPath = "";
  state.buildStatus = "idle";
  state.activeAction = "build";
  state.buildResult = null;
  state.runResult = null;
  state.error = "";
  renderUserState();
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const nextMode = button.dataset.mode;
    if (nextMode === "user" || nextMode === "contributor") {
      state.mode = nextMode;
      renderMode();
      if (nextMode === "contributor") {
        void refreshContributorQueue();
      }
    }
  });
});

pickFolderButton.addEventListener("click", () => {
  void pickProjectFolder();
});

createImageButton.addEventListener("click", () => {
  void createProjectDockerImage();
});

requestRunButton.addEventListener("click", () => {
  void requestProjectRun();
});

refreshWorkersButton.addEventListener("click", () => {
  void refreshContributorQueue();
});

clearSelectionButton.addEventListener("click", clearSelection);

renderMode();
renderUserState();
void refreshContributorQueue();
