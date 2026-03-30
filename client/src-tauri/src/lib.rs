use flate2::{write::GzEncoder, Compression};
use hmac::{Hmac, Mac};
use redis::Commands;
use reqwest::blocking::Client as BlockingHttpClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tonic::transport::Channel;
use std::{
    collections::HashMap,
    error::Error as StdError,
    fs::{self, File},
    io::{self, BufReader, BufWriter, ErrorKind, Read},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::OnceLock,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use time::{macros::format_description, OffsetDateTime};
use url::Url;
use uuid::Uuid;

pub mod coordinatorpb {
    tonic::include_proto!("compute.v1");
}

use coordinatorpb::client_service_client::ClientServiceClient;
use coordinatorpb::{ResourceSpec as CoordinatorResourceSpec, SubmitJobRequest};

const JOB_QUEUE_KEY: &str = "job_queue";
const WORKERS_KEY: &str = "workers";
const JOB_STATUS_QUEUED: &str = "queued";
const ARTIFACT_RECORD_PREFIX: &str = "computehive_job_artifact:";
const CONTRIBUTOR_ACTIVE_WORKER_KEY: &str = "computehive_active_contributor_worker";
const WORKER_AUTH_PREFIX: &str = "computehive_worker_auth:";
const LOCAL_WORKER_IDENTITY_FILE: &str = ".computehive-worker-identity.json";
const DEFAULT_MAX_RUNTIME_SECONDS: i32 = 3600;
const DEFAULT_REQUIRED_CPU_CORES: i32 = 1;
const DEFAULT_REQUIRED_GPU_COUNT: i32 = 0;
const DEFAULT_REQUIRED_MEMORY_MB: i32 = 1024;
const DEFAULT_R2_REGION: &str = "auto";
const DEFAULT_COORDINATOR_ADDR: &str = "127.0.0.1:50051";
const DEFAULT_CONTRIBUTOR_CPU_CORES: i32 = 8;
const DEFAULT_CONTRIBUTOR_GPU_COUNT: i32 = 1;
const DEFAULT_CONTRIBUTOR_MEMORY_MB: i32 = 16384;
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

static APP_CONFIG: OnceLock<Result<AppConfig, String>> = OnceLock::new();
static ENV_FILE_LOADED: OnceLock<()> = OnceLock::new();

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DockerImageResult {
    project_name: String,
    project_root: String,
    dockerfile_path: String,
    image_archive_path: String,
    image_tag: String,
    image_size_bytes: u64,
    detected_stack: String,
    docker_setup_source: String,
    generated_files: Vec<String>,
    summary: String,
    notes: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequestResult {
    image: DockerImageResult,
    gzip_archive_path: String,
    gzip_size_bytes: u64,
    artifact_sha256: String,
    artifact_object_key: String,
    artifact_uri: String,
    artifact_api_url: String,
    artifact_public_url: String,
    artifact_record_key: String,
    artifact_etag: Option<String>,
    redis_job_id: String,
    redis_job_key: String,
    redis_status_key: String,
    redis_queue_key: String,
    summary: String,
    notes: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IncomingRunRequest {
    job_id: String,
    project_name: String,
    detected_stack: String,
    status: String,
    container_image: String,
    artifact_sha256: String,
    artifact_object_key: String,
    artifact_uri: String,
    artifact_api_url: String,
    artifact_public_url: String,
    gzip_archive_path: String,
    gzip_size_bytes: u64,
    created_at_unix: i64,
    project_root: String,
}

struct DockerSetup {
    dockerfile_path: PathBuf,
    detected_stack: String,
    docker_setup_source: String,
    generated_files: Vec<PathBuf>,
    notes: Vec<String>,
    dockerfile_generated_by_app: bool,
}

enum DetectedProject {
    Node(NodeProject),
    Python(PythonProject),
    Rust(RustProject),
    Generic,
}

struct NodeProject {
    package_manager: PackageManager,
    framework_label: String,
    install_command: String,
    build_command: Option<String>,
    run_command: String,
    port: u16,
}

enum PackageManager {
    Npm,
    Pnpm,
    Yarn,
    Bun,
}

struct PythonProject {
    install_step: String,
    run_command: String,
}

struct RustProject {
    binary_name: String,
}

#[derive(Clone)]
struct AppConfig {
    object_storage: ObjectStorageConfig,
    redis_url: String,
    coordinator_addr: String,
}

#[derive(Clone)]
struct ObjectStorageConfig {
    endpoint_url: String,
    bucket_name: String,
    bucket_api_url: String,
    bucket_public_url: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct RedisResourceSpec {
    cpu_cores: i32,
    gpu_count: i32,
    memory_mb: i32,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerRecord {
    id: String,
    status: String,
    resources: RedisWorkerResources,
    current_load: RedisWorkerCurrentLoad,
    capabilities: RedisWorkerCapabilities,
    last_heartbeat: i64,
    stats: RedisWorkerStats,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerResources {
    cpu_cores: i32,
    memory_mb: i32,
    gpu: bool,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerCurrentLoad {
    cpu_used: i32,
    memory_used: i32,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerCapabilities {
    docker: bool,
    gpu_supported: bool,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerStats {
    jobs_completed: i32,
    jobs_failed: i32,
}

#[derive(Serialize, Deserialize)]
struct RedisJobRecord {
    id: String,
    container_image: String,
    command: Vec<String>,
    required_resources: Option<RedisResourceSpec>,
    environment: HashMap<String, String>,
    max_runtime_seconds: i32,
    status: String,
    created_at_unix: i64,
    assigned_worker_id: String,
}

#[derive(Serialize)]
struct ContributorWorkerRecord {
    id: String,
    status: String,
    resources: ContributorWorkerResources,
    current_load: ContributorWorkerCurrentLoad,
    capabilities: ContributorWorkerCapabilities,
    last_heartbeat: i64,
    stats: ContributorWorkerStats,
}

#[derive(Serialize)]
struct ContributorWorkerResources {
    cpu_cores: i32,
    memory_mb: i32,
    gpu: bool,
}

#[derive(Serialize)]
struct ContributorWorkerCurrentLoad {
    cpu_used: i32,
    memory_used: i32,
}

#[derive(Serialize)]
struct ContributorWorkerCapabilities {
    docker: bool,
    gpu_supported: bool,
}

#[derive(Serialize)]
struct ContributorWorkerStats {
    jobs_completed: i32,
    jobs_failed: i32,
}

#[derive(Serialize, Deserialize)]
struct RedisWorkerAuthRecord {
    worker_id: String,
    worker_name: String,
    password_hash: String,
}

#[derive(Serialize, Deserialize)]
struct LocalWorkerIdentity {
    worker_id: String,
    worker_name: String,
    worker_hash: String,
}

#[derive(Serialize, Deserialize)]
struct ArtifactRecord {
    job_id: String,
    project_name: String,
    project_root: String,
    detected_stack: String,
    dockerfile_path: String,
    image_archive_path: String,
    gzip_archive_path: String,
    container_image: String,
    artifact_sha256: String,
    artifact_object_key: String,
    artifact_uri: String,
    artifact_api_url: String,
    artifact_public_url: String,
    gzip_size_bytes: u64,
    image_size_bytes: u64,
    docker_setup_source: String,
    created_at_unix: i64,
}

struct UploadedArtifact {
    artifact_uri: String,
    artifact_api_url: String,
    artifact_public_url: String,
    artifact_object_key: String,
    artifact_etag: Option<String>,
}

struct SignedObjectStorageRequest {
    url: String,
    headers: Vec<(String, String)>,
}

enum DockerInfoStatus {
    Ready,
    DaemonUnavailable(String),
    OtherFailure(String),
}

#[tauri::command]
fn create_project_docker_image(project_path: String) -> Result<DockerImageResult, String> {
    build_project_docker_image(&project_path)
}

#[tauri::command]
async fn request_project_run(project_path: String) -> Result<RunRequestResult, String> {
    let config = app_config()?;
    let image = build_project_docker_image(&project_path)?;
    let project_root = PathBuf::from(&image.project_root);
    let gzip_archive_path = tar_gz_path_for_project(&project_root, &image.project_name);

    if gzip_archive_path.exists() {
        fs::remove_file(&gzip_archive_path)
            .map_err(|error| format!("Failed to replace the existing compressed image: {error}"))?;
    }

    gzip_file(Path::new(&image.image_archive_path), &gzip_archive_path)?;

    let gzip_size_bytes = fs::metadata(&gzip_archive_path)
        .map_err(|error| format!("Failed to inspect the compressed image: {error}"))?
        .len();
    let artifact_sha256 = sha256_for_file(&gzip_archive_path)?;
    let job_id = Uuid::new_v4().to_string();
    let object_key = object_key_for_artifact(&image.project_name, &job_id, &artifact_sha256);

    let uploaded_artifact = upload_artifact_to_object_storage(
        config,
        &gzip_archive_path,
        &object_key,
        &artifact_sha256,
    )
    .await?;

    let artifact_record = ArtifactRecord {
        job_id: job_id.clone(),
        project_name: image.project_name.clone(),
        project_root: image.project_root.clone(),
        detected_stack: image.detected_stack.clone(),
        dockerfile_path: image.dockerfile_path.clone(),
        image_archive_path: image.image_archive_path.clone(),
        gzip_archive_path: gzip_archive_path.display().to_string(),
        container_image: image.image_tag.clone(),
        artifact_sha256: artifact_sha256.clone(),
        artifact_object_key: uploaded_artifact.artifact_object_key.clone(),
        artifact_uri: uploaded_artifact.artifact_uri.clone(),
        artifact_api_url: uploaded_artifact.artifact_api_url.clone(),
        artifact_public_url: uploaded_artifact.artifact_public_url.clone(),
        gzip_size_bytes,
        image_size_bytes: image.image_size_bytes,
        docker_setup_source: image.docker_setup_source.clone(),
        created_at_unix: current_unix_timestamp(),
    };

    let mut environment = HashMap::new();
    environment.insert(
        "COMPUTEHIVE_ARTIFACT_SHA256".to_string(),
        artifact_record.artifact_sha256.clone(),
    );
    environment.insert(
        "COMPUTEHIVE_ARTIFACT_URI".to_string(),
        artifact_record.artifact_uri.clone(),
    );
    environment.insert(
        "COMPUTEHIVE_ARTIFACT_API_URL".to_string(),
        artifact_record.artifact_api_url.clone(),
    );
    environment.insert(
        "COMPUTEHIVE_ARTIFACT_OBJECT_KEY".to_string(),
        artifact_record.artifact_object_key.clone(),
    );
    environment.insert(
        "COMPUTEHIVE_PROJECT_NAME".to_string(),
        artifact_record.project_name.clone(),
    );
    environment.insert(
        "COMPUTEHIVE_DETECTED_STACK".to_string(),
        artifact_record.detected_stack.clone(),
    );
    environment.insert("COMPUTEHIVE_IMAGE_TAG".to_string(), image.image_tag.clone());
    environment.insert(
        "COMPUTEHIVE_IMAGE_ARCHIVE_PATH".to_string(),
        artifact_record.gzip_archive_path.clone(),
    );

    let coordinator_job_id = match submit_job_to_coordinator(config, &image, environment).await {
        Ok(job_id) => job_id,
        Err(error) => {
            let cleanup_result = delete_artifact_from_object_storage(config, &object_key).await;
            let cleanup_message = match cleanup_result {
                Ok(()) => "The uploaded artifact was cleaned up automatically.".to_string(),
                Err(cleanup_error) => format!(
                    "The uploaded artifact could not be cleaned up automatically: {cleanup_error}"
                ),
            };

            return Err(format!(
                "Failed to submit the run request to the coordinator after uploading the artifact. {error} {cleanup_message}"
            ));
        }
    };

    let mut notes = image.notes.clone();
    notes.push(format!(
        "Compressed the Docker image archive to {}.",
        gzip_archive_path.display()
    ));
    notes.push(format!(
        "Calculated SHA-256 for the compressed artifact: {}.",
        artifact_sha256
    ));
    notes.push(format!(
        "Uploaded the compressed artifact to object storage as {}.",
        uploaded_artifact.artifact_object_key
    ));
    notes.push(format!(
        "Public artifact link: {}.",
        uploaded_artifact.artifact_public_url
    ));
    if config.object_storage.bucket_public_url == config.object_storage.bucket_api_url {
        notes.push(
            "S3_PUBLIC_BUCKET_URL is not configured, so the public artifact link currently uses the same bucket base URL as S3_BUCKET."
                .to_string(),
        );
    }
    notes.push(format!(
        "Submitted the run request to the coordinator SubmitJob endpoint as job {}.",
        coordinator_job_id
    ));
    notes.push(
        "The submitted job carries the artifact hash and object storage location in its environment for future contributor actions."
            .to_string(),
    );
    notes.push(
        "ComputeHive intentionally did not create or run a container during this request."
            .to_string(),
    );

    let summary = format!(
        "Built the Docker image, compressed it to a tar.gz artifact, uploaded it to object storage, and submitted run request {} to the coordinator.",
        coordinator_job_id
    );

    Ok(RunRequestResult {
        image,
        gzip_archive_path: gzip_archive_path.display().to_string(),
        gzip_size_bytes,
        artifact_sha256,
        artifact_object_key: uploaded_artifact.artifact_object_key,
        artifact_uri: uploaded_artifact.artifact_uri,
        artifact_api_url: uploaded_artifact.artifact_api_url,
        artifact_public_url: uploaded_artifact.artifact_public_url,
        artifact_record_key: artifact_record_key(&coordinator_job_id),
        artifact_etag: uploaded_artifact.artifact_etag,
        redis_job_id: coordinator_job_id.clone(),
        redis_job_key: job_key(&coordinator_job_id),
        redis_status_key: job_status_key(&coordinator_job_id),
        redis_queue_key: JOB_QUEUE_KEY.to_string(),
        summary,
        notes,
    })
}

#[tauri::command]
fn list_incoming_run_requests() -> Result<Vec<IncomingRunRequest>, String> {
    let config = app_config()?;
    let client = redis::Client::open(config.redis_url.as_str())
        .map_err(|error| format!("Failed to parse the Redis URL: {error}"))?;
    let mut connection = client
        .get_connection()
        .map_err(|error| format!("Failed to connect to Redis: {error}"))?;

    let queued_job_ids = connection
        .lrange::<_, Vec<String>>(JOB_QUEUE_KEY, 0, 49)
        .map_err(|error| format!("Failed to read the Redis job queue: {error}"))?;

    if queued_job_ids.is_empty() {
        return Ok(Vec::new());
    }

    let job_keys = queued_job_ids
        .iter()
        .map(|job_id| job_key(job_id))
        .collect::<Vec<_>>();
    let artifact_keys = queued_job_ids
        .iter()
        .map(|job_id| artifact_record_key(job_id))
        .collect::<Vec<_>>();
    let status_keys = queued_job_ids
        .iter()
        .map(|job_id| job_status_key(job_id))
        .collect::<Vec<_>>();
    let job_payloads = redis::cmd("MGET")
        .arg(&job_keys)
        .query::<Vec<Option<String>>>(&mut connection)
        .map_err(|error| format!("Failed to load queued job records: {error}"))?;
    let artifact_payloads = redis::cmd("MGET")
        .arg(&artifact_keys)
        .query::<Vec<Option<String>>>(&mut connection)
        .map_err(|error| format!("Failed to load queued artifact records: {error}"))?;
    let statuses = redis::cmd("MGET")
        .arg(&status_keys)
        .query::<Vec<Option<String>>>(&mut connection)
        .map_err(|error| format!("Failed to load queued job statuses: {error}"))?;

    let mut requests = Vec::with_capacity(queued_job_ids.len());

    for (index, job_id) in queued_job_ids.into_iter().enumerate() {
        let Some(job_payload) = job_payloads.get(index).and_then(|payload| payload.as_ref()) else {
            continue;
        };
        let artifact_payload = artifact_payloads
            .get(index)
            .and_then(|payload| payload.as_deref());
        let status = statuses
            .get(index)
            .and_then(|value| value.clone())
            .unwrap_or_else(|| JOB_STATUS_QUEUED.to_string());

        let job: RedisJobRecord = serde_json::from_str(job_payload).map_err(|error| {
            format!(
                "Failed to parse the queued job record {}: {error}",
                job_key(&job_id)
            )
        })?;

        let artifact = artifact_payload
            .map(serde_json::from_str::<ArtifactRecord>)
            .transpose()
            .map_err(|error| {
                format!(
                    "Failed to parse the queued artifact record {}: {error}",
                    artifact_record_key(&job_id)
                )
            })?;

        requests.push(IncomingRunRequest {
            job_id: job.id,
            project_name: artifact
                .as_ref()
                .map(|record| record.project_name.clone())
                .unwrap_or_else(|| "Unknown project".to_string()),
            detected_stack: artifact
                .as_ref()
                .map(|record| record.detected_stack.clone())
                .unwrap_or_else(|| "Unknown stack".to_string()),
            status,
            container_image: job.container_image,
            artifact_sha256: artifact
                .as_ref()
                .map(|record| record.artifact_sha256.clone())
                .unwrap_or_default(),
            artifact_object_key: artifact
                .as_ref()
                .map(|record| record.artifact_object_key.clone())
                .unwrap_or_default(),
            artifact_uri: artifact
                .as_ref()
                .map(|record| record.artifact_uri.clone())
                .unwrap_or_default(),
            artifact_api_url: artifact
                .as_ref()
                .map(|record| record.artifact_api_url.clone())
                .unwrap_or_default(),
            artifact_public_url: artifact
                .as_ref()
                .map(|record| record.artifact_public_url.clone())
                .unwrap_or_default(),
            gzip_archive_path: artifact
                .as_ref()
                .map(|record| record.gzip_archive_path.clone())
                .unwrap_or_default(),
            gzip_size_bytes: artifact
                .as_ref()
                .map(|record| record.gzip_size_bytes)
                .unwrap_or_default(),
            created_at_unix: artifact
                .as_ref()
                .map(|record| record.created_at_unix)
                .unwrap_or(job.created_at_unix),
            project_root: artifact
                .as_ref()
                .map(|record| record.project_root.clone())
                .unwrap_or_default(),
        });
    }

    Ok(requests)
}

#[tauri::command]
fn register_contributor_worker(
    worker_name: String,
    password: String,
) -> Result<ContributorWorkerRecord, String> {
    let worker_name = normalize_worker_name(&worker_name)?;
    let password_hash = hash_string(password.trim());
    let config = app_config()?;
    let client = redis::Client::open(config.redis_url.as_str())
        .map_err(|error| format!("Failed to parse the Redis URL: {error}"))?;
    let mut connection = client
        .get_connection()
        .map_err(|error| format!("Failed to connect to Redis: {error}"))?;

    let auth_key = worker_auth_key(&worker_name);
    let existing_auth = connection
        .get::<_, Option<String>>(&auth_key)
        .map_err(|error| format!("Failed to check existing worker auth {}: {error}", auth_key))?;
    if existing_auth.is_some() {
        return Err("That worker name is already registered.".to_string());
    }

    let registered_at_unix = current_unix_timestamp();
    let worker_id = Uuid::new_v4().to_string();
    let worker_record = RedisWorkerRecord {
        id: worker_id.clone(),
        status: "available".to_string(),
        resources: RedisWorkerResources {
            cpu_cores: DEFAULT_CONTRIBUTOR_CPU_CORES,
            memory_mb: DEFAULT_CONTRIBUTOR_MEMORY_MB,
            gpu: DEFAULT_CONTRIBUTOR_GPU_COUNT > 0,
        },
        current_load: RedisWorkerCurrentLoad {
            cpu_used: 0,
            memory_used: 0,
        },
        capabilities: RedisWorkerCapabilities {
            docker: true,
            gpu_supported: DEFAULT_CONTRIBUTOR_GPU_COUNT > 0,
        },
        last_heartbeat: registered_at_unix,
        stats: RedisWorkerStats {
            jobs_completed: 0,
            jobs_failed: 0,
        },
    };
    let worker_payload = serde_json::to_string(&worker_record)
        .map_err(|error| format!("Failed to serialize worker record: {error}"))?;
    let auth_record = RedisWorkerAuthRecord {
        worker_id: worker_id.clone(),
        worker_name: worker_name.clone(),
        password_hash: password_hash.clone(),
    };
    let auth_payload = serde_json::to_string(&auth_record)
        .map_err(|error| format!("Failed to serialize worker auth record: {error}"))?;
    let local_identity = LocalWorkerIdentity {
        worker_id: worker_id.clone(),
        worker_name: worker_name.clone(),
        worker_hash: build_worker_hash(&worker_id, &worker_name, &password_hash)?,
    };
    save_local_worker_identity(&local_identity)?;

    redis::pipe()
        .atomic()
        .sadd(WORKERS_KEY, &worker_id)
        .ignore()
        .set(redis_worker_key(&worker_id), worker_payload)
        .ignore()
        .set(auth_key, auth_payload)
        .ignore()
        .set(redis_worker_alive_key(&worker_id), "1")
        .ignore()
        .set(CONTRIBUTOR_ACTIVE_WORKER_KEY, &worker_id)
        .ignore()
        .query::<()>(&mut connection)
        .map_err(|error| format!("Failed to store contributor worker in Redis: {error}"))?;

    Ok(to_contributor_worker_record(&worker_record))
}

#[tauri::command]
fn get_registered_contributor_worker() -> Result<Option<ContributorWorkerRecord>, String> {
    let config = app_config()?;
    let client = redis::Client::open(config.redis_url.as_str())
        .map_err(|error| format!("Failed to parse the Redis URL: {error}"))?;
    let mut connection = client
        .get_connection()
        .map_err(|error| format!("Failed to connect to Redis: {error}"))?;

    let Some(local_identity) = load_local_worker_identity()? else {
        return Ok(None);
    };
    let worker_id = local_identity.worker_id;

    let worker_payload = connection
        .get::<_, Option<String>>(redis_worker_key(&worker_id))
        .map_err(|error| {
            format!(
                "Failed to load worker record {}: {error}",
                redis_worker_key(&worker_id)
            )
        })?;

    let Some(worker_payload) = worker_payload else {
        return Ok(None);
    };

    let worker_record: RedisWorkerRecord = match serde_json::from_str(&worker_payload) {
        Ok(record) => record,
        Err(_) => return Ok(None),
    };

    let auth_payload = connection
        .get::<_, Option<String>>(worker_auth_key(&local_identity.worker_name))
        .map_err(|error| format!("Failed to load worker auth for {}: {error}", local_identity.worker_name))?;
    let Some(auth_payload) = auth_payload else {
        return Ok(None);
    };
    let auth_record: RedisWorkerAuthRecord = serde_json::from_str(&auth_payload)
        .map_err(|error| format!("Failed to parse worker auth: {error}"))?;
    let expected_hash =
        build_worker_hash(&auth_record.worker_id, &auth_record.worker_name, &auth_record.password_hash)?;
    if expected_hash != local_identity.worker_hash {
        return Err("This is not your device.".to_string());
    }

    Ok(Some(to_contributor_worker_record(&worker_record)))
}

#[tauri::command]
fn login_contributor_worker(
    worker_name: String,
    password: String,
) -> Result<ContributorWorkerRecord, String> {
    let worker_name = normalize_worker_name(&worker_name)?;
    let password_hash = hash_string(password.trim());
    let local_identity = load_local_worker_identity()?
        .ok_or_else(|| "No local worker identity was found on this device.".to_string())?;
    let config = app_config()?;
    let client = redis::Client::open(config.redis_url.as_str())
        .map_err(|error| format!("Failed to parse the Redis URL: {error}"))?;
    let mut connection = client
        .get_connection()
        .map_err(|error| format!("Failed to connect to Redis: {error}"))?;

    let auth_payload = connection
        .get::<_, Option<String>>(worker_auth_key(&worker_name))
        .map_err(|error| format!("Failed to load worker auth: {error}"))?
        .ok_or_else(|| "Worker name or password is incorrect.".to_string())?;
    let auth_record: RedisWorkerAuthRecord = serde_json::from_str(&auth_payload)
        .map_err(|error| format!("Failed to parse worker auth: {error}"))?;
    if auth_record.password_hash != password_hash {
        return Err("Worker name or password is incorrect.".to_string());
    }

    let expected_hash =
        build_worker_hash(&auth_record.worker_id, &auth_record.worker_name, &auth_record.password_hash)?;
    if local_identity.worker_name != auth_record.worker_name
        || local_identity.worker_id != auth_record.worker_id
        || local_identity.worker_hash != expected_hash
    {
        return Err("This is not your device.".to_string());
    }

    connection
        .set::<_, _, ()>(CONTRIBUTOR_ACTIVE_WORKER_KEY, &auth_record.worker_id)
        .map_err(|error| format!("Failed to mark the active worker: {error}"))?;

    let worker_payload = connection
        .get::<_, Option<String>>(redis_worker_key(&auth_record.worker_id))
        .map_err(|error| format!("Failed to load worker record: {error}"))?
        .ok_or_else(|| "The worker record could not be found.".to_string())?;
    let worker_record: RedisWorkerRecord = serde_json::from_str(&worker_payload)
        .map_err(|error| format!("Failed to parse worker record: {error}"))?;
    Ok(to_contributor_worker_record(&worker_record))
}

#[tauri::command]
fn activate_contributor_worker() -> Result<ContributorWorkerRecord, String> {
    let local_identity = load_local_worker_identity()?
        .ok_or_else(|| "No local worker identity was found on this device.".to_string())?;
    let config = app_config()?;
    let client = redis::Client::open(config.redis_url.as_str())
        .map_err(|error| format!("Failed to parse the Redis URL: {error}"))?;
    let mut connection = client
        .get_connection()
        .map_err(|error| format!("Failed to connect to Redis: {error}"))?;

    let worker_payload = connection
        .get::<_, Option<String>>(redis_worker_key(&local_identity.worker_id))
        .map_err(|error| format!("Failed to load worker record: {error}"))?
        .ok_or_else(|| "The worker record could not be found.".to_string())?;
    let mut worker_record: RedisWorkerRecord = serde_json::from_str(&worker_payload)
        .map_err(|error| format!("Failed to parse worker record: {error}"))?;
    worker_record.status = "active".to_string();
    worker_record.last_heartbeat = current_unix_timestamp();

    let payload = serde_json::to_string(&worker_record)
        .map_err(|error| format!("Failed to serialize worker record: {error}"))?;
    redis::pipe()
        .atomic()
        .set(redis_worker_key(&worker_record.id), payload)
        .ignore()
        .set(redis_worker_alive_key(&worker_record.id), "1")
        .ignore()
        .query::<()>(&mut connection)
        .map_err(|error| format!("Failed to activate the worker record: {error}"))?;

    Ok(to_contributor_worker_record(&worker_record))
}

fn build_project_docker_image(project_path: &str) -> Result<DockerImageResult, String> {
    let project_root = PathBuf::from(project_path);

    if !project_root.exists() {
        return Err("The selected folder does not exist.".to_string());
    }

    if !project_root.is_dir() {
        return Err("The selected path is not a folder.".to_string());
    }

    let project_root = fs::canonicalize(project_root)
        .map_err(|error| format!("Failed to resolve the selected folder: {error}"))?;
    let project_name = project_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project")
        .to_string();
    let sanitized_name = sanitize_name(&project_name);
    let image_tag = format!("computehive-{}:latest", sanitized_name);
    let image_file_name = format!("{sanitized_name}-computehive-image.tar");
    let image_archive_path = project_root.join(&image_file_name);

    let mut docker_setup = ensure_docker_setup(&project_root, &project_name)?;
    docker_setup
        .notes
        .extend(ensure_docker_runtime_available()?);

    if image_archive_path.exists() {
        fs::remove_file(&image_archive_path)
            .map_err(|error| format!("Failed to replace the existing image archive: {error}"))?;
    }

    if let Err(primary_error) =
        build_docker_image(&project_root, &docker_setup.dockerfile_path, &image_tag)
    {
        if docker_setup.dockerfile_generated_by_app {
            let fallback_contents = generic_dockerfile(&project_name);
            fs::write(&docker_setup.dockerfile_path, fallback_contents)
                .map_err(|error| format!("Failed to write the fallback Dockerfile: {error}"))?;
            docker_setup.docker_setup_source = "App generated fallback".to_string();
            docker_setup.notes.push(
                "The initial auto-generated Dockerfile did not build cleanly, so ComputeHive replaced it with a generic fallback Dockerfile and retried the image build."
                    .to_string(),
            );

            build_docker_image(&project_root, &docker_setup.dockerfile_path, &image_tag).map_err(
                |fallback_error| {
                    format!("{primary_error} The fallback Dockerfile also failed. {fallback_error}")
                },
            )?;
        } else {
            return Err(primary_error);
        }
    }

    save_docker_image(&image_archive_path, &image_tag)?;

    let image_size_bytes = fs::metadata(&image_archive_path)
        .map_err(|error| format!("Failed to inspect the exported Docker image: {error}"))?
        .len();

    let generated_files = docker_setup
        .generated_files
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    let generated_summary = if generated_files.is_empty() {
        "Used the existing Docker setup in the selected project.".to_string()
    } else {
        format!(
            "Generated {} Docker setup file(s) before building the image.",
            generated_files.len()
        )
    };
    let summary = format!(
        "{generated_summary} Built Docker image {} and saved it as {} in the selected project root.",
        image_tag, image_file_name
    );

    docker_setup
        .notes
        .push("No container was created by this action.".to_string());
    docker_setup.notes.push(format!(
        "Saved the Docker image archive to {}.",
        image_archive_path.display()
    ));

    Ok(DockerImageResult {
        project_name,
        project_root: project_root.display().to_string(),
        dockerfile_path: docker_setup.dockerfile_path.display().to_string(),
        image_archive_path: image_archive_path.display().to_string(),
        image_tag,
        image_size_bytes,
        detected_stack: docker_setup.detected_stack,
        docker_setup_source: docker_setup.docker_setup_source,
        generated_files,
        summary,
        notes: docker_setup.notes,
    })
}

fn ensure_docker_setup(project_root: &Path, project_name: &str) -> Result<DockerSetup, String> {
    let detected_project = detect_project(project_root)?;
    let dockerfile_path = project_root.join("Dockerfile");
    let dockerignore_path = project_root.join(".dockerignore");
    let dockerfile_generated_by_app = !dockerfile_path.exists();

    let mut generated_files = Vec::new();
    let mut notes = vec![format!(
        "Detected project stack: {}.",
        detected_project.label()
    )];

    if dockerfile_generated_by_app {
        let dockerfile_contents = detected_project.primary_dockerfile(project_name);
        fs::write(&dockerfile_path, dockerfile_contents)
            .map_err(|error| format!("Failed to create the Dockerfile: {error}"))?;
        generated_files.push(dockerfile_path.clone());
        notes.push(format!(
            "Generated a Dockerfile for the detected {} project.",
            detected_project.label()
        ));
    } else {
        notes.push("Used the existing Dockerfile from the selected project root.".to_string());
    }

    if !dockerignore_path.exists() {
        fs::write(&dockerignore_path, default_dockerignore())
            .map_err(|error| format!("Failed to create the .dockerignore file: {error}"))?;
        generated_files.push(dockerignore_path.clone());
        notes.push(
            "Generated a .dockerignore file to keep the Docker build context lean.".to_string(),
        );
    } else {
        notes.push("Used the existing .dockerignore from the selected project root.".to_string());
    }

    notes.extend(detected_project.notes());

    let docker_setup_source = if dockerfile_generated_by_app {
        "App generated".to_string()
    } else if generated_files.is_empty() {
        "Existing setup".to_string()
    } else {
        "Existing Dockerfile + generated ignore".to_string()
    };

    Ok(DockerSetup {
        dockerfile_path,
        detected_stack: detected_project.label().to_string(),
        docker_setup_source,
        generated_files,
        notes,
        dockerfile_generated_by_app,
    })
}

fn detect_project(project_root: &Path) -> Result<DetectedProject, String> {
    if project_root.join("package.json").exists() {
        return detect_node_project(project_root).map(DetectedProject::Node);
    }

    if project_root.join("requirements.txt").exists()
        || project_root.join("pyproject.toml").exists()
        || project_root.join("manage.py").exists()
        || project_root.join("app.py").exists()
        || project_root.join("main.py").exists()
    {
        return Ok(DetectedProject::Python(detect_python_project(project_root)));
    }

    if project_root.join("Cargo.toml").exists() {
        return Ok(DetectedProject::Rust(detect_rust_project(project_root)?));
    }

    Ok(DetectedProject::Generic)
}

fn detect_node_project(project_root: &Path) -> Result<NodeProject, String> {
    let package_json_path = project_root.join("package.json");
    let package_json = fs::read_to_string(&package_json_path)
        .map_err(|error| format!("Failed to read package.json: {error}"))?;
    let package_json: Value = serde_json::from_str(&package_json)
        .map_err(|error| format!("Failed to parse package.json: {error}"))?;
    let package_manager = detect_package_manager(project_root, &package_json);
    let has_build = script_exists(&package_json, "build");
    let has_start = script_exists(&package_json, "start");
    let has_preview = script_exists(&package_json, "preview");
    let has_dev = script_exists(&package_json, "dev");
    let uses_next =
        dependency_exists(&package_json, "next") || script_contains(&package_json, "next");
    let uses_vite =
        dependency_exists(&package_json, "vite") || script_contains(&package_json, "vite");
    let uses_nest = dependency_exists(&package_json, "@nestjs/core");
    let framework_label = if uses_next {
        "Node.js (Next.js)"
    } else if uses_nest {
        "Node.js (NestJS)"
    } else if uses_vite {
        "Node.js (Vite)"
    } else {
        "Node.js"
    }
    .to_string();
    let port = if uses_next || has_start {
        3000
    } else if uses_vite {
        4173
    } else {
        3000
    };

    let run_command = if has_start {
        package_manager.run_script_command("start")
    } else if has_preview {
        format!(
            "{} -- --host 0.0.0.0 --port {}",
            package_manager.run_script_command("preview"),
            port
        )
    } else if has_dev {
        format!(
            "{} -- --host 0.0.0.0 --port {}",
            package_manager.run_script_command("dev"),
            port
        )
    } else {
        "node -e 'console.log(\"ComputeHive built this image. Add a start script before running the container.\")'".to_string()
    };

    Ok(NodeProject {
        install_command: package_manager.install_command(project_root),
        build_command: has_build.then(|| package_manager.run_script_command("build")),
        package_manager,
        framework_label,
        run_command,
        port,
    })
}

fn detect_python_project(project_root: &Path) -> PythonProject {
    let install_step = if project_root.join("requirements.txt").exists() {
        "RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt"
            .to_string()
    } else if project_root.join("pyproject.toml").exists() {
        "RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .".to_string()
    } else {
        "RUN pip install --no-cache-dir --upgrade pip".to_string()
    };

    let run_command = if project_root.join("manage.py").exists() {
        "python manage.py runserver 0.0.0.0:8000".to_string()
    } else if project_root.join("app.py").exists() {
        "python app.py".to_string()
    } else if project_root.join("main.py").exists() {
        "python main.py".to_string()
    } else {
        "python -m http.server 8000".to_string()
    };

    PythonProject {
        install_step,
        run_command,
    }
}

fn detect_rust_project(project_root: &Path) -> Result<RustProject, String> {
    let cargo_toml_path = project_root.join("Cargo.toml");
    let cargo_toml = fs::read_to_string(&cargo_toml_path)
        .map_err(|error| format!("Failed to read Cargo.toml: {error}"))?;
    let binary_name = extract_cargo_package_name(&cargo_toml).unwrap_or_else(|| "app".to_string());
    Ok(RustProject { binary_name })
}

impl DetectedProject {
    fn label(&self) -> &str {
        match self {
            Self::Node(project) => &project.framework_label,
            Self::Python(_) => "Python",
            Self::Rust(_) => "Rust",
            Self::Generic => "Generic project",
        }
    }

    fn primary_dockerfile(&self, project_name: &str) -> String {
        match self {
            Self::Node(project) => node_dockerfile(project),
            Self::Python(project) => python_dockerfile(project),
            Self::Rust(project) => rust_dockerfile(project),
            Self::Generic => generic_dockerfile(project_name),
        }
    }

    fn notes(&self) -> Vec<String> {
        match self {
            Self::Node(project) => vec![
                format!(
                    "Generated Node.js Docker setup using {} commands.",
                    project.package_manager.label()
                ),
                format!(
                    "The generated image exposes port {} and runs {} by default.",
                    project.port, project.run_command
                ),
            ],
            Self::Python(_) => vec![
                "Generated Python Docker setup with a Python 3.11 slim base image.".to_string(),
                "The generated Python image defaults to port 8000.".to_string(),
            ],
            Self::Rust(project) => vec![
                "Generated Rust Docker setup with a release build step.".to_string(),
                format!(
                    "The generated Rust image targets the binary `{}` by default.",
                    project.binary_name
                ),
            ],
            Self::Generic => vec![
                "Generated a generic fallback Dockerfile because no specific project stack was detected."
                    .to_string(),
            ],
        }
    }
}

impl PackageManager {
    fn label(&self) -> &str {
        match self {
            Self::Npm => "npm",
            Self::Pnpm => "pnpm",
            Self::Yarn => "yarn",
            Self::Bun => "bun",
        }
    }

    fn base_image(&self) -> &str {
        match self {
            Self::Bun => "oven/bun:1.1-alpine",
            _ => "node:20-alpine",
        }
    }

    fn requires_corepack(&self) -> bool {
        matches!(self, Self::Pnpm | Self::Yarn)
    }

    fn run_script_command(&self, script: &str) -> String {
        match self {
            Self::Npm => format!("npm run {script}"),
            Self::Pnpm => format!("pnpm run {script}"),
            Self::Yarn => format!("yarn {script}"),
            Self::Bun => format!("bun run {script}"),
        }
    }

    fn install_command(&self, project_root: &Path) -> String {
        match self {
            Self::Npm => {
                if project_root.join("package-lock.json").exists() {
                    "npm ci".to_string()
                } else {
                    "npm install".to_string()
                }
            }
            Self::Pnpm => {
                if project_root.join("pnpm-lock.yaml").exists() {
                    "pnpm install --frozen-lockfile".to_string()
                } else {
                    "pnpm install".to_string()
                }
            }
            Self::Yarn => {
                if project_root.join("yarn.lock").exists() {
                    "yarn install --frozen-lockfile".to_string()
                } else {
                    "yarn install".to_string()
                }
            }
            Self::Bun => {
                if project_root.join("bun.lockb").exists() || project_root.join("bun.lock").exists()
                {
                    "bun install --frozen-lockfile".to_string()
                } else {
                    "bun install".to_string()
                }
            }
        }
    }
}

fn detect_package_manager(project_root: &Path, package_json: &Value) -> PackageManager {
    let package_manager = package_json
        .get("packageManager")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if package_manager.starts_with("bun")
        || project_root.join("bun.lockb").exists()
        || project_root.join("bun.lock").exists()
    {
        PackageManager::Bun
    } else if package_manager.starts_with("pnpm") || project_root.join("pnpm-lock.yaml").exists() {
        PackageManager::Pnpm
    } else if package_manager.starts_with("yarn") || project_root.join("yarn.lock").exists() {
        PackageManager::Yarn
    } else {
        PackageManager::Npm
    }
}

fn dependency_exists(package_json: &Value, dependency_name: &str) -> bool {
    package_json
        .get("dependencies")
        .and_then(Value::as_object)
        .and_then(|dependencies| dependencies.get(dependency_name))
        .is_some()
        || package_json
            .get("devDependencies")
            .and_then(Value::as_object)
            .and_then(|dependencies| dependencies.get(dependency_name))
            .is_some()
}

fn script_exists(package_json: &Value, script_name: &str) -> bool {
    package_json
        .get("scripts")
        .and_then(Value::as_object)
        .and_then(|scripts| scripts.get(script_name))
        .is_some()
}

fn script_contains(package_json: &Value, needle: &str) -> bool {
    package_json
        .get("scripts")
        .and_then(Value::as_object)
        .map(|scripts| {
            scripts
                .values()
                .filter_map(Value::as_str)
                .any(|script| script.contains(needle))
        })
        .unwrap_or(false)
}

fn node_dockerfile(project: &NodeProject) -> String {
    let corepack_step = if project.package_manager.requires_corepack() {
        "RUN corepack enable\n"
    } else {
        ""
    };
    let build_step = project
        .build_command
        .as_ref()
        .map(|command| format!("RUN {command}\n"))
        .unwrap_or_default();

    format!(
        "FROM {}\nWORKDIR /app\n{}COPY . .\nRUN {}\n{}EXPOSE {}\nCMD sh -c \"{}\"\n",
        project.package_manager.base_image(),
        corepack_step,
        project.install_command,
        build_step,
        project.port,
        escape_for_docker_shell(&project.run_command),
    )
}

fn python_dockerfile(project: &PythonProject) -> String {
    format!(
        "FROM python:3.11-slim\nWORKDIR /app\nENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1\nCOPY . .\n{}\nEXPOSE 8000\nCMD sh -c \"{}\"\n",
        project.install_step,
        escape_for_docker_shell(&project.run_command),
    )
}

fn rust_dockerfile(project: &RustProject) -> String {
    format!(
        "FROM rust:1.81-slim\nWORKDIR /app\nCOPY . .\nRUN cargo build --release\nCMD sh -c \"if [ -x target/release/{0} ]; then ./target/release/{0}; else echo 'ComputeHive built the Rust image but could not find the expected binary.'; fi\"\n",
        escape_for_docker_shell(&project.binary_name),
    )
}

fn generic_dockerfile(project_name: &str) -> String {
    format!(
        "FROM alpine:3.20\nWORKDIR /workspace\nCOPY . .\nCMD sh -c \"echo 'ComputeHive created a generic Docker image for {}.'\"\n",
        escape_for_docker_shell(project_name),
    )
}

fn default_dockerignore() -> &'static str {
    ".git\n.gitignore\n.DS_Store\nnode_modules\ndist\nbuild\ncoverage\ntarget\n.venv\nvenv\n__pycache__\n.next\n.nuxt\n.turbo\n*.log\n*.pid\n*.tar\n*.tar.gz\n*.zip\n*-computehive-image.tar\n*-computehive-image.tar.gz\n"
}

fn ensure_docker_runtime_available() -> Result<Vec<String>, String> {
    match docker_info_status()? {
        DockerInfoStatus::Ready => Ok(vec![
            "Docker daemon is available and ready for image build.".to_string(),
        ]),
        DockerInfoStatus::DaemonUnavailable(details) => {
            let runtime = launch_docker_runtime()?;
            wait_for_docker_daemon(&runtime)?;
            Ok(vec![
                format!("Docker daemon was not running, so ComputeHive launched {runtime}."),
                format!("Docker reported before startup: {details}"),
                "Docker daemon became ready and the app continued automatically.".to_string(),
            ])
        }
        DockerInfoStatus::OtherFailure(details) => Err(format!(
            "Docker is installed but not usable in the current environment. {}",
            details
        )),
    }
}

fn docker_info_status() -> Result<DockerInfoStatus, String> {
    let output = Command::new("docker")
        .arg("info")
        .output()
        .map_err(|error| command_invocation_error("docker", error))?;

    if output.status.success() {
        return Ok(DockerInfoStatus::Ready);
    }

    let details = summarize_command_output(&output);
    let lower = details.to_ascii_lowercase();

    if lower.contains("cannot connect to the docker daemon")
        || lower.contains("is the docker daemon running")
        || lower.contains("error during connect")
        || lower.contains("docker.sock")
    {
        Ok(DockerInfoStatus::DaemonUnavailable(details))
    } else {
        Ok(DockerInfoStatus::OtherFailure(details))
    }
}

fn launch_docker_runtime() -> Result<String, String> {
    for candidate in docker_runtime_candidates() {
        let status = Command::new("open")
            .arg("-a")
            .arg(&candidate)
            .status()
            .map_err(|error| format!("Failed to launch {candidate}: {error}"))?;

        if status.success() {
            return Ok(candidate);
        }
    }

    Err(
        "Docker daemon is not running and ComputeHive could not find a supported local runtime to launch automatically. Install or enable OrbStack or Docker Desktop."
            .to_string(),
    )
}

fn docker_runtime_candidates() -> Vec<String> {
    let mut candidates = Vec::new();

    if app_exists_in_common_locations("OrbStack.app") {
        candidates.push("OrbStack".to_string());
    }

    if app_exists_in_common_locations("Docker.app") {
        candidates.push("Docker".to_string());
    }

    candidates
}

fn app_exists_in_common_locations(app_name: &str) -> bool {
    let applications_path = Path::new("/Applications").join(app_name);
    if applications_path.exists() {
        return true;
    }

    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join("Applications").join(app_name).exists())
        .unwrap_or(false)
}

fn wait_for_docker_daemon(runtime_name: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_details = String::new();

    while Instant::now() < deadline {
        match docker_info_status()? {
            DockerInfoStatus::Ready => return Ok(()),
            DockerInfoStatus::DaemonUnavailable(details)
            | DockerInfoStatus::OtherFailure(details) => {
                last_details = details;
            }
        }

        thread::sleep(Duration::from_secs(1));
    }

    if last_details.is_empty() {
        Err(format!(
            "{runtime_name} was launched, but Docker did not become ready in time."
        ))
    } else {
        Err(format!(
            "{runtime_name} was launched, but Docker did not become ready in time. Last Docker response: {last_details}"
        ))
    }
}

fn build_docker_image(
    project_root: &Path,
    dockerfile_path: &Path,
    image_tag: &str,
) -> Result<(), String> {
    let output = Command::new("docker")
        .arg("build")
        .arg("-t")
        .arg(image_tag)
        .arg("-f")
        .arg(dockerfile_path)
        .arg(project_root)
        .output()
        .map_err(|error| command_invocation_error("docker build", error))?;

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Docker build failed. {}",
            summarize_command_output(&output)
        ))
    }
}

fn save_docker_image(image_archive_path: &Path, image_tag: &str) -> Result<(), String> {
    let output = Command::new("docker")
        .arg("save")
        .arg("-o")
        .arg(image_archive_path)
        .arg(image_tag)
        .output()
        .map_err(|error| command_invocation_error("docker save", error))?;

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Docker image export failed. {}",
            summarize_command_output(&output)
        ))
    }
}

fn gzip_file(source_path: &Path, target_path: &Path) -> Result<(), String> {
    let input = File::open(source_path)
        .map_err(|error| format!("Failed to open the Docker image tar for compression: {error}"))?;
    let output = File::create(target_path).map_err(|error| {
        format!("Failed to create the compressed Docker image archive: {error}")
    })?;

    let mut encoder = GzEncoder::new(BufWriter::new(output), Compression::best());
    io::copy(&mut BufReader::new(input), &mut encoder)
        .map_err(|error| format!("Failed to compress the Docker image archive: {error}"))?;
    encoder
        .finish()
        .map_err(|error| format!("Failed to finalize the compressed image archive: {error}"))?;

    Ok(())
}

fn sha256_for_file(path: &Path) -> Result<String, String> {
    let file = File::open(path)
        .map_err(|error| format!("Failed to open the compressed image for hashing: {error}"))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|error| format!("Failed to read the compressed image for hashing: {error}"))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

async fn upload_artifact_to_object_storage(
    config: &AppConfig,
    artifact_path: &Path,
    object_key: &str,
    artifact_sha256: &str,
) -> Result<UploadedArtifact, String> {
    let config = config.clone();
    let artifact_path = artifact_path.to_path_buf();
    let object_key = object_key.to_string();
    let artifact_sha256 = artifact_sha256.to_string();

    tauri::async_runtime::spawn_blocking(move || {
        upload_artifact_to_object_storage_blocking(
            &config,
            &artifact_path,
            &object_key,
            &artifact_sha256,
        )
    })
    .await
    .map_err(|error| format!("The object storage upload task failed: {error}"))?
}

async fn delete_artifact_from_object_storage(
    config: &AppConfig,
    object_key: &str,
) -> Result<(), String> {
    let config = config.clone();
    let object_key = object_key.to_string();

    tauri::async_runtime::spawn_blocking(move || {
        let signed_request = sign_object_storage_request(
            &config.object_storage,
            "DELETE",
            &object_key,
            EMPTY_PAYLOAD_SHA256,
            &[],
        )?;
        let response = build_object_storage_http_client()?
            .delete(&signed_request.url)
            .headers(reqwest_headers(&signed_request.headers)?)
            .send()
            .map_err(|error| {
                format!(
                    "Failed to delete the uploaded artifact from object storage: {}",
                    format_reqwest_error(&error)
                )
            })?;

        if response.status().is_success() || response.status().as_u16() == 404 {
            Ok(())
        } else {
            let status = response.status();
            let body = response
                .text()
                .unwrap_or_else(|_| "Object storage did not return an error body.".to_string());
            Err(format!(
                "Object storage cleanup failed with status {}. {}",
                status, body
            ))
        }
    })
    .await
    .map_err(|error| format!("The object storage cleanup task failed: {error}"))?
}

fn upload_artifact_to_object_storage_blocking(
    config: &AppConfig,
    artifact_path: &Path,
    object_key: &str,
    artifact_sha256: &str,
) -> Result<UploadedArtifact, String> {
    let artifact_size_bytes = fs::metadata(artifact_path)
        .map_err(|error| format!("Failed to inspect the compressed artifact for upload: {error}"))?
        .len();

    let file = File::open(artifact_path)
        .map_err(|error| format!("Failed to open the compressed artifact for upload: {error}"))?;

    let signed_request = sign_object_storage_request(
        &config.object_storage,
        "PUT",
        object_key,
        artifact_sha256,
        &[
            ("content-type", "application/gzip"),
            ("x-amz-meta-artifact-sha256", artifact_sha256),
        ],
    )?;

    let response = build_object_storage_http_client()?
        .put(&signed_request.url)
        .headers(reqwest_headers(&signed_request.headers)?)
        .header("content-length", artifact_size_bytes.to_string())
        .body(file)
        .send()
        .map_err(|error| {
            format!(
                "Failed to upload the artifact to object storage: {}",
                format_reqwest_error(&error)
            )
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .unwrap_or_else(|_| "Object storage did not return an error body.".to_string());
        return Err(format!(
            "Object storage upload failed with status {}. {}",
            status, body
        ));
    }

    Ok(UploadedArtifact {
        artifact_uri: format!("s3://{}/{}", config.object_storage.bucket_name, object_key),
        artifact_api_url: join_bucket_api_url(&config.object_storage.bucket_api_url, object_key),
        artifact_public_url: join_bucket_api_url(
            &config.object_storage.bucket_public_url,
            object_key,
        ),
        artifact_object_key: object_key.to_string(),
        artifact_etag: response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
    })
}

fn sign_object_storage_request(
    config: &ObjectStorageConfig,
    method: &str,
    object_key: &str,
    payload_sha256: &str,
    extra_headers: &[(&str, &str)],
) -> Result<SignedObjectStorageRequest, String> {
    let endpoint = Url::parse(&config.endpoint_url)
        .map_err(|error| format!("Failed to parse the object storage endpoint: {error}"))?;
    let host = endpoint_host_header(&endpoint)?;
    let object_key = object_key.trim_start_matches('/');
    let canonical_uri = format!(
        "/{}/{}",
        aws_uri_encode_path_segment(config.bucket_name.trim_matches('/')),
        aws_uri_encode_path_segment(object_key)
    );
    let request_url = format!(
        "{}/{}/{}",
        config.endpoint_url.trim_end_matches('/'),
        config.bucket_name.trim_matches('/'),
        object_key
    );
    let timestamp = aws_timestamp().map_err(|error| {
        format!("Failed to create the object storage request timestamp: {error}")
    })?;

    let mut canonical_headers = vec![
        ("host".to_string(), host),
        (
            "x-amz-content-sha256".to_string(),
            payload_sha256.to_string(),
        ),
        ("x-amz-date".to_string(), timestamp.amz_date.clone()),
    ];

    for (name, value) in extra_headers {
        canonical_headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }

    canonical_headers.sort_by(|left, right| left.0.cmp(&right.0));

    let canonical_headers_text = canonical_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let signed_headers = canonical_headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let credential_scope = format!(
        "{}/{}/s3/aws4_request",
        timestamp.date_stamp, config.region
    );
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n\n{canonical_headers_text}\n{signed_headers}\n{payload_sha256}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp.amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = aws_v4_signature(
        &config.secret_access_key,
        &timestamp.date_stamp,
        &config.region,
        "s3",
        &string_to_sign,
    )?;
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        config.access_key_id, credential_scope, signed_headers, signature
    );

    let mut headers = canonical_headers;
    headers.push(("authorization".to_string(), authorization));

    Ok(SignedObjectStorageRequest {
        url: request_url,
        headers,
    })
}

fn build_object_storage_http_client() -> Result<BlockingHttpClient, String> {
    BlockingHttpClient::builder()
        .http1_only()
        .connect_timeout(Duration::from_secs(20))
        .timeout(None::<Duration>)
        .build()
        .map_err(|error| format!("Failed to initialize the object storage HTTP client: {error}"))
}

fn reqwest_headers(headers: &[(String, String)]) -> Result<reqwest::header::HeaderMap, String> {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| format!("Invalid object storage header name `{name}`: {error}"))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|error| format!("Invalid object storage header value for `{name}`: {error}"))?;
        map.insert(header_name, header_value);
    }

    Ok(map)
}

fn endpoint_host_header(endpoint: &Url) -> Result<String, String> {
    let host = endpoint
        .host_str()
        .ok_or_else(|| "The object storage endpoint is missing a host.".to_string())?;

    Ok(match endpoint.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn aws_uri_encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~'
            | b'/' => encoded.push(*byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

fn aws_v4_signature(
    secret_access_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> Result<String, String> {
    let key_date = hmac_sha256(format!("AWS4{secret_access_key}").as_bytes(), date_stamp)?;
    let key_region = hmac_sha256(&key_date, region)?;
    let key_service = hmac_sha256(&key_region, service)?;
    let signing_key = hmac_sha256(&key_service, "aws4_request")?;
    let signature = hmac_sha256(&signing_key, string_to_sign)?;
    Ok(hex_lower(&signature))
}

fn hmac_sha256(key: &[u8], message: &str) -> Result<Vec<u8>, String> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).map_err(|error| format!("Invalid HMAC key: {error}"))?;
    mac.update(message.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }

    output
}

struct AwsTimestamp {
    amz_date: String,
    date_stamp: String,
}

fn aws_timestamp() -> Result<AwsTimestamp, time::error::Format> {
    let now = OffsetDateTime::now_utc();
    let amz_date = now.format(format_description!("[year][month][day]T[hour][minute][second]Z"))?;
    let date_stamp = now.format(format_description!("[year][month][day]"))?;

    Ok(AwsTimestamp {
        amz_date,
        date_stamp,
    })
}

fn format_reqwest_error(error: &reqwest::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();

    while let Some(next) = source {
        message.push_str(" Caused by: ");
        message.push_str(&next.to_string());
        source = next.source();
    }

    message
}

fn placeholder_contributor_command() -> Vec<String> {
    vec![
        "/bin/sh".to_string(),
        "-lc".to_string(),
        "echo 'ComputeHive artifact verified and queued. Contributor actions will load and run this image in a later pass.'"
            .to_string(),
    ]
}

fn tar_gz_path_for_project(project_root: &Path, project_name: &str) -> PathBuf {
    let sanitized_name = sanitize_name(project_name);
    project_root.join(format!("{sanitized_name}-computehive-image.tar.gz"))
}

fn object_key_for_artifact(project_name: &str, job_id: &str, artifact_sha256: &str) -> String {
    let sanitized_name = sanitize_name(project_name);
    let hash_prefix = artifact_sha256.chars().take(16).collect::<String>();
    format!(
        "computehive/run-requests/{sanitized_name}/{job_id}/{sanitized_name}-{hash_prefix}.tar.gz"
    )
}

fn app_config() -> Result<&'static AppConfig, String> {
    match APP_CONFIG.get_or_init(load_app_config) {
        Ok(config) => Ok(config),
        Err(error) => Err(error.clone()),
    }
}

fn load_app_config() -> Result<AppConfig, String> {
    ensure_env_file_loaded();

    let bucket_url = required_env("S3_BUCKET")?;
    let access_key_id = required_env("S3_ACCESS_KEY_ID")?;
    let secret_access_key = required_env("S3_SECRET_ACCESS_KEY")?;
    let redis_url = required_env("REDIS_URL")?;
    let coordinator_addr = optional_env("COORDINATOR_ADDR")
        .unwrap_or_else(|| DEFAULT_COORDINATOR_ADDR.to_string());
    let (endpoint_url, bucket_name, bucket_api_url) = parse_bucket_url(&bucket_url)?;
    let bucket_public_url =
        optional_env("S3_PUBLIC_BUCKET_URL").unwrap_or_else(|| bucket_api_url.clone());

    Ok(AppConfig {
        object_storage: ObjectStorageConfig {
            endpoint_url,
            bucket_name,
            bucket_api_url,
            bucket_public_url,
            access_key_id,
            secret_access_key,
            region: DEFAULT_R2_REGION.to_string(),
        },
        redis_url,
        coordinator_addr,
    })
}

async fn submit_job_to_coordinator(
    config: &AppConfig,
    image: &DockerImageResult,
    environment: HashMap<String, String>,
) -> Result<String, String> {
    let endpoint = coordinator_endpoint(&config.coordinator_addr);
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|error| format!("Failed to parse coordinator address {endpoint}: {error}"))?
        .connect()
        .await
        .map_err(|error| format!("Failed to connect to coordinator at {endpoint}: {error}"))?;

    let mut client = ClientServiceClient::new(channel);
    let response = client
        .submit_job(SubmitJobRequest {
            container_image: image.image_tag.clone(),
            command: placeholder_contributor_command(),
            required_resources: Some(CoordinatorResourceSpec {
                cpu_cores: DEFAULT_REQUIRED_CPU_CORES,
                gpu_count: DEFAULT_REQUIRED_GPU_COUNT,
                memory_mb: DEFAULT_REQUIRED_MEMORY_MB,
            }),
            environment,
            max_runtime_seconds: DEFAULT_MAX_RUNTIME_SECONDS,
            job: None,
        })
        .await
        .map_err(|error| format!("Coordinator SubmitJob request failed: {error}"))?;

    let job_id = response.into_inner().job_id.map(|id| id.value).unwrap_or_default();
    if job_id.trim().is_empty() {
        return Err("Coordinator SubmitJob response did not include a job id.".to_string());
    }

    Ok(job_id)
}

fn coordinator_endpoint(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

fn ensure_env_file_loaded() {
    ENV_FILE_LOADED.get_or_init(|| {
        for path in env_file_candidates() {
            if path.exists() {
                let _ = dotenvy::from_path_override(path);
                break;
            }
        }
    });
}

fn env_file_candidates() -> Vec<PathBuf> {
    let mut paths = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("../.env")];

    if let Ok(current_dir) = std::env::current_dir() {
        paths.push(current_dir.join(".env"));
    }

    paths
}

fn required_env(key: &str) -> Result<String, String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("Missing required configuration value: {key}"))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_bucket_url(bucket_url: &str) -> Result<(String, String, String), String> {
    let url =
        Url::parse(bucket_url).map_err(|error| format!("Failed to parse S3_BUCKET: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "S3_BUCKET is missing a host.".to_string())?;
    let endpoint_url = match url.port() {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    };
    let bucket_name = url.path().trim_matches('/').to_string();

    if bucket_name.is_empty() {
        return Err("S3_BUCKET must include the bucket name in the path.".to_string());
    }

    let bucket_api_url = format!("{}/{}", endpoint_url, bucket_name);
    Ok((endpoint_url, bucket_name, bucket_api_url))
}

fn join_bucket_api_url(bucket_api_url: &str, object_key: &str) -> String {
    format!(
        "{}/{}",
        bucket_api_url.trim_end_matches('/'),
        object_key.trim_start_matches('/')
    )
}

fn current_unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn job_key(job_id: &str) -> String {
    format!("job:{job_id}")
}

fn job_status_key(job_id: &str) -> String {
    format!("job_status:{job_id}")
}

fn artifact_record_key(job_id: &str) -> String {
    format!("{ARTIFACT_RECORD_PREFIX}{job_id}")
}

fn redis_worker_key(worker_id: &str) -> String {
    format!("worker:{worker_id}")
}

fn redis_worker_alive_key(worker_id: &str) -> String {
    format!("worker_alive:{worker_id}")
}

fn worker_auth_key(worker_name: &str) -> String {
    format!("{WORKER_AUTH_PREFIX}{worker_name}")
}

fn to_contributor_worker_record(worker_record: &RedisWorkerRecord) -> ContributorWorkerRecord {
    ContributorWorkerRecord {
        id: worker_record.id.clone(),
        status: worker_record.status.clone(),
        resources: ContributorWorkerResources {
            cpu_cores: worker_record.resources.cpu_cores,
            memory_mb: worker_record.resources.memory_mb,
            gpu: worker_record.resources.gpu,
        },
        current_load: ContributorWorkerCurrentLoad {
            cpu_used: worker_record.current_load.cpu_used,
            memory_used: worker_record.current_load.memory_used,
        },
        capabilities: ContributorWorkerCapabilities {
            docker: worker_record.capabilities.docker,
            gpu_supported: worker_record.capabilities.gpu_supported,
        },
        last_heartbeat: worker_record.last_heartbeat,
        stats: ContributorWorkerStats {
            jobs_completed: worker_record.stats.jobs_completed,
            jobs_failed: worker_record.stats.jobs_failed,
        },
    }
}

fn normalize_worker_name(worker_name: &str) -> Result<String, String> {
    let normalized = worker_name.trim().to_lowercase();
    if normalized.is_empty() {
        return Err("Worker name is required.".to_string());
    }
    Ok(normalized)
}

fn hash_string(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn build_worker_hash(worker_id: &str, worker_name: &str, password_hash: &str) -> Result<String, String> {
    let fingerprint = local_device_fingerprint()?;
    Ok(hash_string(&format!(
        "{worker_id}:{worker_name}:{password_hash}:{fingerprint}"
    )))
}

fn local_device_fingerprint() -> Result<String, String> {
    let hostname = Command::new("hostname")
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_default();
    let username = std::env::var("USER").unwrap_or_default();
    let home = std::env::var("HOME").unwrap_or_default();
    let raw = format!(
        "{}:{}:{}:{}:{}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        username,
        home,
        hostname
    );
    if raw.trim_matches(':').is_empty() {
        return Err("Unable to derive a stable device fingerprint.".to_string());
    }
    Ok(hash_string(&raw))
}

fn local_worker_identity_path() -> Result<PathBuf, String> {
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| "HOME is not set, so the local worker identity cannot be stored.".to_string())?;
    Ok(home.join(LOCAL_WORKER_IDENTITY_FILE))
}

fn save_local_worker_identity(identity: &LocalWorkerIdentity) -> Result<(), String> {
    let path = local_worker_identity_path()?;
    let payload = serde_json::to_vec(identity)
        .map_err(|error| format!("Failed to serialize the local worker identity: {error}"))?;
    fs::write(&path, payload)
        .map_err(|error| format!("Failed to store the local worker identity at {}: {error}", path.display()))
}

fn load_local_worker_identity() -> Result<Option<LocalWorkerIdentity>, String> {
    let path = local_worker_identity_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read(&path)
        .map_err(|error| format!("Failed to read the local worker identity at {}: {error}", path.display()))?;
    let identity = serde_json::from_slice(&payload)
        .map_err(|error| format!("Failed to parse the local worker identity: {error}"))?;
    Ok(Some(identity))
}

fn sanitize_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    if sanitized.is_empty() {
        "project".to_string()
    } else {
        sanitized
    }
}

fn extract_cargo_package_name(cargo_toml: &str) -> Option<String> {
    let mut in_package_section = false;

    for line in cargo_toml.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with('[') {
            in_package_section = trimmed == "[package]";
            continue;
        }

        if in_package_section && trimmed.starts_with("name") {
            let name = trimmed.split('=').nth(1)?.trim().trim_matches('"');
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }

    None
}

fn escape_for_docker_shell(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn command_invocation_error(command_name: &str, error: std::io::Error) -> String {
    if error.kind() == ErrorKind::NotFound {
        format!(
            "{command_name} is not available. Install Docker and make sure the docker CLI is in PATH."
        )
    } else {
        format!("Failed to run {command_name}: {error}")
    }
}

fn summarize_command_output(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };

    let summarized = combined
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");

    if summarized.is_empty() {
        "Docker did not return additional output.".to_string()
    } else {
        summarized
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            create_project_docker_image,
            request_project_run,
            list_incoming_run_requests,
            register_contributor_worker,
            login_contributor_worker,
            activate_contributor_worker,
            get_registered_contributor_worker
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
