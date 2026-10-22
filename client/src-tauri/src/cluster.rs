//! Secure access to a ComputeHive cluster from the desktop app.
//!
//! Everything here goes through the coordinator's public, authenticated API: gRPC over TLS with the API
//! token, and the coordinator's own artifact/output store over HTTPS. The app never talks to Redis or to an
//! object store, so it works against exactly the cluster `make up` starts, with no extra credentials.
//!
//! The contributor controls (pause, resume, donation sliders) talk to the *local* worker over its private
//! Unix control socket, the same interface `computehive worker ...` uses.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

use crate::coordinatorpb::{
    admin_service_client::AdminServiceClient, client_service_client::ClientServiceClient,
    CancelJobRequest, CreateEnrollmentTokenRequest, ExplainJobRequest, GetJobStatusRequest,
    JobId, ListJobsRequest, ListWorkersRequest, Status as JobStatus,
};

const DEFAULT_COORDINATOR_ADDR: &str = "127.0.0.1:50051";
const HTTP_PORT: u16 = 8080;

// ---------------------------------------------------------------------------------------------------
// configuration

/// Where the app finds the cluster. Nothing here is a secret that the app invents: the token and CA are the
/// files `make up` writes to `.computehive/` (or the same values in the environment).
#[derive(Clone)]
pub struct ClusterConfig {
    pub coordinator_addr: String,
    pub http_url: String,
    pub api_token: Option<String>,
    pub ca_pem: Option<Vec<u8>>,
    pub tls_server_name: Option<String>,
    pub source: String,
}

impl ClusterConfig {
    pub fn tls(&self) -> bool {
        self.ca_pem.is_some()
    }

    fn host(&self) -> String {
        host_of(&self.coordinator_addr)
    }
}

/// Installs the process-wide rustls crypto provider (once). Every TLS user (gRPC and HTTPS) goes through
/// `load_config` first, so this always runs before the first connection.
pub fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Err means a provider was already installed elsewhere, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn host_of(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((host, _)) => host.trim_matches(|c| c == '[' || c == ']').to_string(),
        None => addr.to_string(),
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Directories that may hold `api.token` and `ca.crt`: a `.computehive` folder in the working directory or
/// any ancestor of it, of the executable, or of the source checkout; then `~/.config/computehive`.
fn config_dirs() -> Vec<PathBuf> {
    let mut starts: Vec<PathBuf> = Vec::new();
    if let Ok(cwd) = env::current_dir() {
        starts.push(cwd);
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(dir) = exe.parent() {
            starts.push(dir.to_path_buf());
        }
    }
    starts.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    let mut dirs: Vec<PathBuf> = Vec::new();
    for start in starts {
        for ancestor in start.ancestors() {
            let candidate = ancestor.join(".computehive");
            if candidate.is_dir() && !dirs.contains(&candidate) {
                dirs.push(candidate);
            }
        }
    }
    if let Some(home) = env_nonempty("HOME") {
        dirs.push(PathBuf::from(home).join(".config").join("computehive"));
    }
    dirs
}

fn read_trimmed(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Loads the configuration. It never fails: a missing token or CA is reported by `cluster_overview`, so the UI
/// can tell the user exactly what to do.
pub fn load_config() -> ClusterConfig {
    ensure_crypto_provider();
    super::ensure_env_file_loaded();
    let dirs = config_dirs();
    let mut sources: Vec<String> = Vec::new();

    let coordinator_addr = env_nonempty("COMPUTEHIVE_COORDINATOR")
        .or_else(|| env_nonempty("CLIENT_COORDINATOR_ADDR"))
        .or_else(|| env_nonempty("COORDINATOR_ADDR"))
        .unwrap_or_else(|| DEFAULT_COORDINATOR_ADDR.to_string());

    let api_token = env_nonempty("COMPUTEHIVE_TOKEN")
        .or_else(|| env_nonempty("API_TOKEN"))
        .or_else(|| {
            dirs.iter().find_map(|d| {
                let t = read_trimmed(&d.join("api.token"));
                if t.is_some() {
                    sources.push(d.display().to_string());
                }
                t
            })
        });

    let ca_path = env_nonempty("COMPUTEHIVE_CA")
        .map(PathBuf::from)
        .filter(|p| p.is_file())
        .or_else(|| {
            dirs.iter()
                .map(|d| d.join("ca.crt"))
                .find(|p| p.is_file())
        });
    let ca_pem = ca_path.as_ref().and_then(|p| fs::read(p).ok());

    let http_url = env_nonempty("COMPUTEHIVE_HTTP")
        .or_else(|| env_nonempty("COORDINATOR_HTTP_URL"))
        .unwrap_or_else(|| {
            let scheme = if ca_pem.is_some() { "https" } else { "http" };
            format!("{scheme}://{}:{HTTP_PORT}", host_of(&coordinator_addr))
        });

    let source = if sources.is_empty() {
        "environment".to_string()
    } else {
        sources.join(", ")
    };
    ClusterConfig {
        coordinator_addr,
        http_url: http_url.trim_end_matches('/').to_string(),
        api_token,
        ca_pem,
        tls_server_name: env_nonempty("COMPUTEHIVE_TLS_SERVER_NAME"),
        source,
    }
}

// ---------------------------------------------------------------------------------------------------
// gRPC channel with TLS and the API token

#[derive(Clone)]
pub struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.0 {
            request.metadata_mut().insert("authorization", value.clone());
        }
        Ok(request)
    }
}

type Intercepted = InterceptedService<Channel, Auth>;

async fn channel(cfg: &ClusterConfig) -> Result<(Channel, Auth), String> {
    let scheme = if cfg.tls() { "https" } else { "http" };
    let url = format!("{scheme}://{}", cfg.coordinator_addr);
    let mut endpoint = Endpoint::from_shared(url.clone())
        .map_err(|e| format!("invalid coordinator address {url}: {e}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10));
    if let Some(pem) = &cfg.ca_pem {
        let name = cfg.tls_server_name.clone().unwrap_or_else(|| cfg.host());
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(pem))
                    .domain_name(name),
            )
            .map_err(|e| format!("TLS configuration: {e}"))?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("cannot reach the coordinator at {}: {e}", cfg.coordinator_addr))?;
    let auth = match &cfg.api_token {
        Some(t) => Some(
            format!("Bearer {t}")
                .parse::<MetadataValue<Ascii>>()
                .map_err(|_| "the API token contains characters that cannot be sent".to_string())?,
        ),
        None => None,
    };
    Ok((channel, Auth(auth)))
}

pub async fn client(cfg: &ClusterConfig) -> Result<ClientServiceClient<Intercepted>, String> {
    let (ch, auth) = channel(cfg).await?;
    Ok(ClientServiceClient::with_interceptor(ch, auth))
}

pub async fn admin(cfg: &ClusterConfig) -> Result<AdminServiceClient<Intercepted>, String> {
    let (ch, auth) = channel(cfg).await?;
    Ok(AdminServiceClient::with_interceptor(ch, auth))
}

/// Turns a gRPC failure into a message that says what to do.
pub fn friendly(err: Status) -> String {
    match err.code() {
        tonic::Code::Unauthenticated => format!(
            "the coordinator rejected the API token ({}). Check .computehive/api.token or COMPUTEHIVE_TOKEN.",
            err.message()
        ),
        tonic::Code::Unavailable => format!(
            "the coordinator is unavailable ({}). Is the cluster running (`make up`)?",
            err.message()
        ),
        _ => format!("{}: {}", err.code(), err.message()),
    }
}

/// A single-use enrollment token for the local contributor worker, minted with the API token (which is
/// the operator's credential: someone who has it may already add workers).
pub async fn mint_enrollment_token(cfg: &ClusterConfig) -> Result<String, String> {
    let mut c = admin(cfg).await?;
    let resp = c
        .create_enrollment_token(CreateEnrollmentTokenRequest {
            ttl_seconds: 600,
            max_uses: 1,
            note: "desktop app".to_string(),
        })
        .await
        .map_err(friendly)?;
    Ok(resp.into_inner().token)
}

// ---------------------------------------------------------------------------------------------------
// views returned to the UI

fn status_label(raw: i32) -> String {
    JobStatus::try_from(raw)
        .map(|s| s.as_str_name().trim_start_matches("STATUS_").to_lowercase())
        .unwrap_or_else(|_| "unknown".to_string())
}

fn is_terminal(raw: i32) -> bool {
    matches!(
        JobStatus::try_from(raw),
        Ok(JobStatus::Succeeded) | Ok(JobStatus::Failed) | Ok(JobStatus::Canceled) | Ok(JobStatus::Dead)
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerView {
    pub(crate) id: String,
    pub(crate) state: String,
    pub(crate) cpu: i32,
    pub(crate) memory_mb: i32,
    pub(crate) gpu: i32,
    pub(crate) ram_percent: f64,
    pub(crate) cpu_percent: f64,
    pub(crate) disk_percent: f64,
    pub(crate) running_jobs: usize,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) labels: Vec<String>,
    pub(crate) reliability: f64,
    pub(crate) isolation: String,
    pub(crate) overload_reasons: Vec<String>,
    pub(crate) heartbeat_age_secs: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    id: String,
    status: String,
    worker: String,
    attempt: i32,
    max_attempts: i32,
    created_unix: i64,
    image: String,
    user: String,
    last_failure: String,
    terminal: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub(crate) configured: bool,
    pub(crate) coordinator_addr: String,
    pub(crate) http_url: String,
    pub(crate) tls: bool,
    pub(crate) has_token: bool,
    pub(crate) config_source: String,
    pub(crate) reachable: bool,
    pub(crate) error: Option<String>,
    pub(crate) workers: Vec<WorkerView>,
    pub(crate) jobs: Vec<JobView>,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One call that answers "what is the cluster doing": reachability, workers with their resources, recent jobs.
/// It never returns an error: problems are reported in `error` so the UI can show them next to the data.
#[tauri::command]
pub async fn cluster_overview() -> ClusterOverview {
    let cfg = load_config();
    let mut out = ClusterOverview {
        configured: cfg.api_token.is_some(),
        coordinator_addr: cfg.coordinator_addr.clone(),
        http_url: cfg.http_url.clone(),
        tls: cfg.tls(),
        has_token: cfg.api_token.is_some(),
        config_source: cfg.source.clone(),
        reachable: false,
        error: None,
        workers: Vec::new(),
        jobs: Vec::new(),
    };
    if cfg.api_token.is_none() {
        out.error = Some(
            "No API token found. Run `make up` in the repository (it writes .computehive/api.token), or set COMPUTEHIVE_TOKEN."
                .to_string(),
        );
    }
    let mut c = match client(&cfg).await {
        Ok(c) => c,
        Err(e) => {
            out.error = Some(e);
            return out;
        }
    };
    match c.list_workers(ListWorkersRequest {}).await {
        Ok(resp) => {
            out.reachable = true;
            let now = now_unix();
            for w in resp.into_inner().workers {
                let res = w.resources.unwrap_or_default();
                let usage = w.usage.unwrap_or_default();
                let state = if !w.alive {
                    "dead"
                } else if w.overloaded {
                    "overloaded"
                } else if w.draining {
                    "paused"
                } else if !w.running_jobs.is_empty() {
                    "running"
                } else {
                    "ready"
                };
                out.workers.push(WorkerView {
                    id: w.worker_id.map(|i| i.value).unwrap_or_default(),
                    state: state.to_string(),
                    cpu: res.cpu_cores,
                    memory_mb: res.memory_mb,
                    gpu: res.gpu_count,
                    ram_percent: usage.mem_used_percent,
                    cpu_percent: usage.cpu_percent,
                    disk_percent: usage
                        .work_disk_used_percent
                        .max(usage.docker_disk_used_percent),
                    running_jobs: w.running_jobs.len(),
                    os: w.os,
                    arch: w.arch,
                    labels: w.labels,
                    reliability: w.reliability,
                    isolation: w.isolation,
                    overload_reasons: usage.overload_reasons,
                    heartbeat_age_secs: (now - w.last_heartbeat_unix).max(0),
                });
            }
        }
        Err(e) => {
            out.error = Some(friendly(e));
            return out;
        }
    }
    match c.list_jobs(ListJobsRequest { limit: 25, status: 0 }).await {
        Ok(resp) => {
            for j in resp.into_inner().jobs {
                out.jobs.push(JobView {
                    id: j.job_id.map(|i| i.value).unwrap_or_default(),
                    status: status_label(j.status),
                    terminal: is_terminal(j.status),
                    worker: j.assigned_worker_id,
                    attempt: j.attempt,
                    max_attempts: j.max_attempts,
                    created_unix: j.created_at_unix,
                    image: j.container_image,
                    user: j.user,
                    last_failure: j.last_failure,
                });
            }
        }
        Err(e) => out.error = Some(friendly(e)),
    }
    out
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobProgress {
    pub(crate) job_id: String,
    pub(crate) status: String,
    pub(crate) terminal: bool,
    pub(crate) worker: String,
    pub(crate) attempt: i32,
    pub(crate) max_attempts: i32,
    pub(crate) last_failure: String,
    pub(crate) verification: String,
    /// Recent output of the running job. It is untrusted text: the UI must show it as text, never as markup.
    pub(crate) log_tail: String,
}

/// Live status of one job, including its recent output while it runs.
#[tauri::command]
pub async fn get_job_progress(job_id: String) -> Result<JobProgress, String> {
    let id = job_id.trim().to_string();
    if id.is_empty() {
        return Err("job_id is required".to_string());
    }
    let cfg = load_config();
    let mut c = client(&cfg).await?;
    let r = c
        .get_job_status(GetJobStatusRequest {
            job_id: Some(JobId { value: id.clone() }),
        })
        .await
        .map_err(friendly)?
        .into_inner();
    Ok(JobProgress {
        job_id: id,
        status: status_label(r.status),
        terminal: is_terminal(r.status),
        worker: r.assigned_worker_id,
        attempt: r.attempt,
        max_attempts: r.max_attempts,
        last_failure: r.last_failure,
        verification: r.verification,
        log_tail: r.log_tail,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationView {
    pub(crate) worker: String,
    pub(crate) eligible: bool,
    pub(crate) chosen: bool,
    pub(crate) busy: bool,
    pub(crate) score: f64,
    pub(crate) reasons: Vec<String>,
    pub(crate) parts: HashMap<String, f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExplainView {
    pub(crate) summary: String,
    pub(crate) recorded: bool,
    pub(crate) assigned_worker: String,
    pub(crate) weights: HashMap<String, f64>,
    pub(crate) evaluations: Vec<EvaluationView>,
}

/// Why a job went to the worker it did (or why it is still waiting).
#[tauri::command]
pub async fn explain_job(job_id: String) -> Result<ExplainView, String> {
    let cfg = load_config();
    let mut c = client(&cfg).await?;
    let r = c
        .explain_job(ExplainJobRequest {
            job_id: Some(JobId {
                value: job_id.trim().to_string(),
            }),
        })
        .await
        .map_err(friendly)?
        .into_inner();
    Ok(ExplainView {
        summary: r.summary,
        recorded: r.recorded,
        assigned_worker: r.assigned_worker_id,
        weights: r.weights,
        evaluations: r
            .evaluations
            .into_iter()
            .map(|e| EvaluationView {
                worker: e.worker_id.map(|i| i.value).unwrap_or_default(),
                eligible: e.eligible,
                chosen: e.chosen,
                busy: e.busy,
                score: e.score,
                reasons: e.reasons,
                parts: e.parts,
            })
            .collect(),
    })
}

#[tauri::command]
pub async fn cancel_job(job_id: String) -> Result<String, String> {
    let cfg = load_config();
    let mut c = client(&cfg).await?;
    let r = c
        .cancel_job(CancelJobRequest {
            job_id: Some(JobId {
                value: job_id.trim().to_string(),
            }),
        })
        .await
        .map_err(friendly)?
        .into_inner();
    Ok(r.disposition)
}

// ---------------------------------------------------------------------------------------------------
// artifacts and outputs over HTTPS

fn http_client(cfg: &ClusterConfig, timeout: Duration) -> Result<reqwest::blocking::Client, String> {
    let mut builder = reqwest::blocking::Client::builder().timeout(timeout);
    if let Some(pem) = &cfg.ca_pem {
        let cert = reqwest::Certificate::from_pem(pem)
            .map_err(|e| format!("the cluster CA certificate is not valid: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }
    builder
        .build()
        .map_err(|e| format!("could not build the HTTP client: {e}"))
}

/// Uploads the saved image (a plain `docker save` tar, exactly what `computehive submit` uploads) to the
/// coordinator's content-addressed store. The store rejects a body whose SHA-256 is not the one in the URL.
pub fn upload_artifact_blocking(path: &Path, sha256: &str) -> Result<(), String> {
    let cfg = load_config();
    let token = cfg
        .api_token
        .clone()
        .ok_or("no API token: run `make up` (it writes .computehive/api.token) or set COMPUTEHIVE_TOKEN")?;
    let size = fs::metadata(path)
        .map_err(|e| format!("cannot read the image archive: {e}"))?
        .len();
    let file = fs::File::open(path).map_err(|e| format!("cannot open the image archive: {e}"))?;
    let client = http_client(&cfg, Duration::from_secs(30 * 60))?;
    let url = format!("{}/artifacts/{sha256}", cfg.http_url);
    let resp = client
        .put(&url)
        .bearer_auth(token)
        .header("Content-Type", "application/x-tar")
        .body(reqwest::blocking::Body::sized(file, size))
        .send()
        .map_err(|e| format!("upload to {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "the coordinator refused the artifact upload: {} {}",
            resp.status(),
            resp.text().unwrap_or_default().trim()
        ));
    }
    Ok(())
}

/// A relative path that is safe to join under a directory we chose: no absolute paths, no `..`, no
/// backslashes, no empty or NUL-containing components. Output names come from the (untrusted) worker.
pub fn safe_relative_path(rel: &str) -> Option<PathBuf> {
    if rel.is_empty() || rel.contains('\\') || rel.contains('\0') || rel.starts_with('/') {
        return None;
    }
    let mut out = PathBuf::new();
    for part in rel.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return None;
        }
        out.push(part);
    }
    Some(out)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadedFile {
    relative_path: String,
    saved_to: String,
    size_bytes: u64,
    sha256_verified: bool,
}

/// Downloads a finished job's output files into `dest_dir`, verifying each against its SHA-256. A file that
/// fails verification is deleted and reported as an error; a hostile file name is refused before any request.
#[tauri::command]
pub async fn download_job_outputs(
    job_id: String,
    dest_dir: String,
) -> Result<Vec<DownloadedFile>, String> {
    let cfg = load_config();
    let mut c = client(&cfg).await?;
    let resp = c
        .get_job_result(crate::coordinatorpb::GetJobResultRequest {
            job_id: Some(JobId {
                value: job_id.trim().to_string(),
            }),
        })
        .await
        .map_err(friendly)?
        .into_inner();
    let artifacts = resp.result.map(|r| r.output_artifacts).unwrap_or_default();
    if artifacts.is_empty() {
        return Err("this job produced no output files".to_string());
    }
    let dest = PathBuf::from(dest_dir.trim());
    if dest.as_os_str().is_empty() {
        return Err("choose a folder to save the results into".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || -> Result<Vec<DownloadedFile>, String> {
        let token = cfg.api_token.clone().ok_or("no API token")?;
        let http = http_client(&cfg, Duration::from_secs(10 * 60))?;
        let mut saved = Vec::new();
        for a in artifacts {
            let rel = safe_relative_path(&a.relative_path)
                .ok_or_else(|| format!("refusing output path {:?} (it could escape the target folder)", a.relative_path))?;
            // uri is computehive-output://<job>/<attempt>/<path>
            let tail = a
                .uri
                .strip_prefix("computehive-output://")
                .ok_or_else(|| format!("unsupported output location {:?}", a.uri))?;
            let url = format!("{}/outputs/{}", cfg.http_url, tail);
            let target = dest.join(&rel);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
            }
            let mut resp = http
                .get(&url)
                .bearer_auth(&token)
                .send()
                .map_err(|e| format!("download {url}: {e}"))?;
            if !resp.status().is_success() {
                return Err(format!("download {}: {}", a.relative_path, resp.status()));
            }
            let mut file = fs::File::create(&target).map_err(|e| format!("cannot write {}: {e}", target.display()))?;
            let mut hasher = Sha256::new();
            let mut size = 0u64;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = resp.read(&mut buf).map_err(|e| format!("download {}: {e}", a.relative_path))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                file.write_all(&buf[..n]).map_err(|e| format!("write {}: {e}", target.display()))?;
                size += n as u64;
            }
            let got = hex_lower(&hasher.finalize());
            let verified = !a.sha256.is_empty();
            if verified && !got.eq_ignore_ascii_case(&a.sha256) {
                let _ = fs::remove_file(&target);
                return Err(format!(
                    "{} failed verification (expected sha256 {}, got {got}); the file was deleted",
                    a.relative_path, a.sha256
                ));
            }
            saved.push(DownloadedFile {
                relative_path: a.relative_path,
                saved_to: target.display().to_string(),
                size_bytes: size,
                sha256_verified: verified,
            });
        }
        Ok(saved)
    })
    .await
    .map_err(|e| format!("download task failed: {e}"))?
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------------------------------
// the local contributor worker's control socket

/// Where the app keeps the contributor worker's state (credential, policy, history, control socket).
pub fn contributor_state_dir() -> PathBuf {
    if let Some(dir) = env_nonempty("COMPUTEHIVE_STATE_DIR") {
        return PathBuf::from(dir);
    }
    let base = env_nonempty("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env_nonempty("HOME").map(|h| PathBuf::from(h).join(".local").join("state")))
        .unwrap_or_else(env::temp_dir);
    base.join("computehive-desktop")
}

fn control_socket() -> PathBuf {
    contributor_state_dir().join("control.sock")
}

/// A minimal HTTP/1.1 client over the worker's Unix socket (the control API is plain HTTP). Returns the status
/// code and body.
#[cfg(unix)]
fn control_call(method: &str, path: &str, body: Option<&str>) -> Result<(u16, String), String> {
    use std::os::unix::net::UnixStream;
    let sock = control_socket();
    let mut stream = UnixStream::connect(&sock).map_err(|_| {
        "the local worker is not running (start sharing first), or its control socket is not available".to_string()
    })?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(5))))
        .map_err(|e| e.to_string())?;
    let payload = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: worker\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(request.as_bytes()).map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    parse_http_response(&raw)
}

#[cfg(not(unix))]
fn control_call(_method: &str, _path: &str, _body: Option<&str>) -> Result<(u16, String), String> {
    Err("controlling the local worker is not supported on this platform yet".to_string())
}

/// Parses a complete HTTP/1.1 response (headers, then a Content-Length, chunked or read-to-close body).
pub fn parse_http_response(raw: &[u8]) -> Result<(u16, String), String> {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or("the worker sent an incomplete response")?;
    let mut lines = head.lines();
    let status_line = lines.next().ok_or("empty response")?;
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("bad status line {status_line:?}"))?;
    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = if chunked { decode_chunked(body)? } else { body.to_string() };
    Ok((code, body))
}

fn decode_chunked(mut rest: &str) -> Result<String, String> {
    let mut out = String::new();
    loop {
        let (size_line, after) = rest.split_once("\r\n").ok_or("bad chunked body")?;
        let size = usize::from_str_radix(size_line.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| "bad chunk size".to_string())?;
        if size == 0 {
            return Ok(out);
        }
        if after.len() < size {
            return Err("truncated chunked body".to_string());
        }
        out.push_str(&after[..size]);
        rest = after[size..].strip_prefix("\r\n").unwrap_or(&after[size..]);
    }
}

fn control_json(method: &str, path: &str, body: Option<&str>) -> Result<Value, String> {
    let (code, text) = control_call(method, path, body)?;
    let value: Value = serde_json::from_str(&text).unwrap_or(Value::String(text.clone()));
    if (200..300).contains(&code) {
        Ok(value)
    } else {
        let msg = value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or(text);
        Err(format!("the worker refused the request ({code}): {msg}"))
    }
}

/// The local worker's status and policy (state, why it is not taking jobs, current job, live metrics).
#[tauri::command]
pub fn contributor_control_status() -> Result<Value, String> {
    control_json("GET", "/v1/status", None)
}

/// Replaces the local worker's policy. The caller sends the whole policy (fetch it from the status first and
/// change fields): the worker rejects unknown fields so a typo cannot silently leave a protection off.
#[tauri::command]
pub fn contributor_set_policy(policy: Value) -> Result<Value, String> {
    let body = serde_json::to_string(&policy).map_err(|e| e.to_string())?;
    control_json("PUT", "/v1/policy", Some(&body))
}

/// Pause sharing. With `now`, the running job is handed back to the cluster immediately (requeued, not blamed).
#[tauri::command]
pub fn contributor_pause(now: bool) -> Result<Value, String> {
    control_json("POST", if now { "/v1/pause?now=true" } else { "/v1/pause" }, None)
}

#[tauri::command]
pub fn contributor_resume() -> Result<Value, String> {
    control_json("POST", "/v1/resume", None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_paths_that_could_escape_the_target_folder_are_refused() {
        for bad in [
            "", "/etc/passwd", "../x", "a/../../x", "a/./b", "a//b", "a\\b", "..", ".", "a/", "nul\0byte",
        ] {
            assert!(safe_relative_path(bad).is_none(), "{bad:?} must be refused");
        }
        assert_eq!(
            safe_relative_path("results/out.csv").unwrap(),
            PathBuf::from("results").join("out.csv")
        );
        assert_eq!(safe_relative_path("a.txt").unwrap(), PathBuf::from("a.txt"));
    }

    #[test]
    fn host_is_taken_from_an_address_with_or_without_brackets() {
        assert_eq!(host_of("10.0.0.5:50051"), "10.0.0.5");
        assert_eq!(host_of("coordinator.example.com:50051"), "coordinator.example.com");
        assert_eq!(host_of("[::1]:50051"), "::1");
        assert_eq!(host_of("localhost"), "localhost");
    }

    #[test]
    fn parses_content_length_and_chunked_responses() {
        let plain = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}";
        assert_eq!(parse_http_response(plain).unwrap(), (200, "{\"ok\":true}".to_string()));

        let chunked = b"HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        assert_eq!(parse_http_response(chunked).unwrap(), (400, "hello world".to_string()));

        assert!(parse_http_response(b"garbage").is_err());
        assert!(parse_http_response(b"HTTP/1.1 abc OK\r\n\r\n").is_err());
        assert!(parse_http_response(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n").is_err());
    }

    #[test]
    fn status_labels_are_friendly() {
        assert_eq!(status_label(JobStatus::Succeeded as i32), "succeeded");
        assert_eq!(status_label(JobStatus::Queued as i32), "queued");
        assert_eq!(status_label(9999), "unknown");
        assert!(is_terminal(JobStatus::Failed as i32));
        assert!(!is_terminal(JobStatus::Running as i32));
    }

    // ---- live tests: they need a running cluster (`make up`). Run with: cargo test -- --ignored ----

    #[test]
    #[ignore = "needs a running cluster: make up"]
    fn live_overview_over_tls_with_the_api_token() {
        let o = tauri::async_runtime::block_on(cluster_overview());
        assert!(o.has_token, "no API token found: {:?}", o.error);
        assert!(o.tls, "expected the cluster CA to be found (.computehive/ca.crt)");
        assert!(o.reachable, "the coordinator was not reachable: {:?}", o.error);
        assert!(!o.workers.is_empty(), "no workers are registered");
        assert!(o.workers.iter().all(|w| !w.id.is_empty()));
    }

    #[test]
    #[ignore = "needs a running cluster: make up"]
    fn live_a_wrong_token_is_rejected_with_a_helpful_message() {
        std::env::set_var("COMPUTEHIVE_TOKEN", "definitely-not-the-token");
        let o = tauri::async_runtime::block_on(cluster_overview());
        std::env::remove_var("COMPUTEHIVE_TOKEN");
        assert!(!o.reachable || o.error.is_some(), "a wrong token must not be accepted");
        let msg = o.error.unwrap_or_default();
        assert!(msg.contains("API token"), "the message should say what to fix: {msg}");
    }

    #[test]
    #[ignore = "needs a running cluster: make up"]
    fn live_unknown_job_is_a_clear_error_not_a_crash() {
        let r = tauri::async_runtime::block_on(get_job_progress("00000000-0000-0000-0000-000000000000".to_string()));
        assert!(r.is_err());
        let e = tauri::async_runtime::block_on(explain_job("00000000-0000-0000-0000-000000000000".to_string()));
        assert!(e.is_err());
    }
}
