import { invoke } from "@tauri-apps/api/core";
import { mockInvoke } from "./mock";

/** True inside the desktop app; false in a plain browser, where a clearly-labelled sample backend is used instead. */
export const inTauri = typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

export interface DockerImageResult {
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

export interface RunOptions {
  cpuCores?: number;
  memoryMb?: number;
  gpuCount?: number;
  timeoutSeconds?: number;
  minDiskMb?: number;
  verify: boolean;
  os?: string;
  arch?: string;
  labels: string[];
}

export interface RunRequestResult {
  image: DockerImageResult;
  tarSizeBytes: number;
  artifactSha256: string;
  jobId: string;
  summary: string;
  notes: string[];
}

export interface WorkerView {
  id: string;
  state: "ready" | "running" | "overloaded" | "paused" | "dead";
  cpu: number;
  memoryMb: number;
  gpu: number;
  ramPercent: number;
  cpuPercent: number;
  diskPercent: number;
  runningJobs: number;
  os: string;
  arch: string;
  labels: string[];
  reliability: number;
  isolation: string;
  overloadReasons: string[];
  heartbeatAgeSecs: number;
}

export interface JobView {
  id: string;
  status: string;
  worker: string;
  attempt: number;
  maxAttempts: number;
  createdUnix: number;
  image: string;
  user: string;
  lastFailure: string;
  terminal: boolean;
}

export interface ClusterOverview {
  configured: boolean;
  coordinatorAddr: string;
  httpUrl: string;
  tls: boolean;
  hasToken: boolean;
  configSource: string;
  reachable: boolean;
  error: string | null;
  workers: WorkerView[];
  jobs: JobView[];
}

export interface JobProgress {
  jobId: string;
  status: string;
  terminal: boolean;
  worker: string;
  attempt: number;
  maxAttempts: number;
  lastFailure: string;
  verification: string;
  logTail: string;
}

export interface RunRequestOutputArtifact {
  relativePath: string;
  sha256: string;
  sizeBytes: number;
  contentType: string;
}

export interface RunRequestJobResult {
  jobId: string;
  status: string;
  resultStatus: string;
  failureReason: string;
  exitCode: number;
  stdoutExcerpt: string;
  stderrExcerpt: string;
  outputArtifacts: RunRequestOutputArtifact[];
  finishedAtUnix: number;
  completed: boolean;
}

export interface EvaluationView {
  worker: string;
  eligible: boolean;
  chosen: boolean;
  busy: boolean;
  score: number;
  reasons: string[];
  parts: Record<string, number>;
}

export interface ExplainView {
  summary: string;
  recorded: boolean;
  assignedWorker: string;
  weights: Record<string, number>;
  evaluations: EvaluationView[];
}

export interface DownloadedFile {
  relativePath: string;
  savedTo: string;
  sizeBytes: number;
  sha256Verified: boolean;
}

export interface ContributorSharingStatus {
  running: boolean;
  pid: number | null;
  details: string;
  jobRunning: boolean;
  currentJobId: string | null;
  lastJobId: string | null;
}

/** The worker's policy as its control API sends and accepts it (snake_case, whole object). */
export interface WorkerPolicy {
  paused: boolean;
  max_cpus: number;
  max_memory_mb: number;
  max_job_disk_mb: number;
  idle_only: boolean;
  idle_cpu_percent: number;
  schedule: string;
  allow_network: boolean;
  allowed_hosts: string[];
  [key: string]: unknown;
}

export interface WorkerMetrics {
  MemTotalMB?: number;
  MemUsedMB?: number;
  MemPercent?: number;
  CPUPercent?: number;
  WorkDiskUsedPercent?: number;
  DockerDiskUsedPercent?: number;
}

export interface ControlStatus {
  worker_id: string;
  version: string;
  isolation: string;
  state: string;
  not_taking: string;
  current_job?: string;
  uptime: string;
  policy: WorkerPolicy;
  metrics?: WorkerMetrics;
}

/** Calls a backend command. Errors always arrive as an Error with a human-readable message. */
export async function call<T>(cmd: string, args: Record<string, unknown> = {}): Promise<T> {
  if (!inTauri) {
    return mockInvoke<T>(cmd, args);
  }
  try {
    return await invoke<T>(cmd, args);
  } catch (e) {
    throw new Error(typeof e === "string" ? e : e instanceof Error ? e.message : String(e));
  }
}
