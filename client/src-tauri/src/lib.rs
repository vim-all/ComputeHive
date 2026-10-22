use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, ErrorKind, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

pub mod coordinatorpb {
    tonic::include_proto!("compute.v1");
}

mod cluster;

use coordinatorpb::{GetJobResultRequest, ResourceSpec as CoordinatorResourceSpec, SubmitJobRequest};

const DEFAULT_MAX_RUNTIME_SECONDS: i32 = 3600;
const DEFAULT_REQUIRED_CPU_CORES: i32 = 1;
const DEFAULT_REQUIRED_GPU_COUNT: i32 = 0;
const DEFAULT_REQUIRED_MEMORY_MB: i32 = 1024;
static ENV_FILE_LOADED: OnceLock<()> = OnceLock::new();
static CONTRIBUTOR_WORKER_CHILD: OnceLock<Mutex<Option<Child>>> = OnceLock::new();
static CONTRIBUTOR_RUNTIME_STATE: OnceLock<Mutex<ContributorRuntimeState>> = OnceLock::new();

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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequestJobResult {
    job_id: String,
    worker_id: String,
    failure_reason: String,
    status: String,
    result_status: String,
    partial: bool,
    exit_code: i32,
    stdout_excerpt: String,
    stderr_excerpt: String,
    output_uri: String,
    output_api_url: String,
    output_public_url: String,
    output_size_bytes: i64,
    output_artifacts: Vec<RunRequestOutputArtifact>,
    finished_at_unix: i64,
    completed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequestOutputArtifact {
    relative_path: String,
    object_key: String,
    uri: String,
    api_url: String,
    public_url: String,
    sha256: String,
    size_bytes: i64,
    content_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContributorSharingStatus {
    running: bool,
    pid: Option<u32>,
    details: String,
    job_running: bool,
    current_job_id: Option<String>,
    last_job_id: Option<String>,
}

#[derive(Default)]
struct ContributorRuntimeState {
    job_running: bool,
    current_job_id: Option<String>,
    last_job_id: Option<String>,
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
fn stop_contributor_sharing() -> Result<ContributorSharingStatus, String> {
    let slot = contributor_worker_slot();
    let mut guard = slot
        .lock()
        .map_err(|_| "Contributor worker process lock is poisoned.".to_string())?;

    if let Some(mut child) = guard.take() {
        let pid = child.id();
        stop_worker_process(&mut child);
        let runtime = contributor_runtime_snapshot();

        return Ok(ContributorSharingStatus {
            running: false,
            pid: Some(pid),
            details: "Sharing stopped. Worker process was terminated.".to_string(),
            job_running: false,
            current_job_id: None,
            last_job_id: runtime.last_job_id,
        });
    }

    let runtime = contributor_runtime_snapshot();

    Ok(ContributorSharingStatus {
        running: false,
        pid: None,
        details: "Sharing is already stopped.".to_string(),
        job_running: false,
        current_job_id: None,
        last_job_id: runtime.last_job_id,
    })
}

#[tauri::command]
fn get_contributor_sharing_status() -> Result<ContributorSharingStatus, String> {
    let slot = contributor_worker_slot();
    let mut guard = slot
        .lock()
        .map_err(|_| "Contributor worker process lock is poisoned.".to_string())?;

    if let Some(child) = guard.as_mut() {
        match child.try_wait() {
            Ok(None) => {
                let runtime = contributor_runtime_snapshot();
                return Ok(ContributorSharingStatus {
                    running: true,
                    pid: Some(child.id()),
                    details: "Worker process is running.".to_string(),
                    job_running: runtime.job_running,
                    current_job_id: runtime.current_job_id,
                    last_job_id: runtime.last_job_id,
                });
            }
            Ok(Some(status)) => {
                let code = status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                *guard = None;
                let runtime = contributor_runtime_snapshot();
                return Ok(ContributorSharingStatus {
                    running: false,
                    pid: None,
                    details: format!("Worker process exited with status {code}."),
                    job_running: false,
                    current_job_id: None,
                    last_job_id: runtime.last_job_id,
                });
            }
            Err(error) => {
                *guard = None;
                let runtime = contributor_runtime_snapshot();
                return Ok(ContributorSharingStatus {
                    running: false,
                    pid: None,
                    details: format!("Failed to inspect worker process state: {error}"),
                    job_running: false,
                    current_job_id: None,
                    last_job_id: runtime.last_job_id,
                });
            }
        }
    }

    let runtime = contributor_runtime_snapshot();

    Ok(ContributorSharingStatus {
        running: false,
        pid: None,
        details: "Worker process is not running.".to_string(),
        job_running: false,
        current_job_id: None,
        last_job_id: runtime.last_job_id,
    })
}

fn contributor_worker_slot() -> &'static Mutex<Option<Child>> {
    CONTRIBUTOR_WORKER_CHILD.get_or_init(|| Mutex::new(None))
}

fn contributor_runtime_slot() -> &'static Mutex<ContributorRuntimeState> {
    CONTRIBUTOR_RUNTIME_STATE.get_or_init(|| Mutex::new(ContributorRuntimeState::default()))
}

fn reset_contributor_runtime_state() {
    if let Ok(mut runtime) = contributor_runtime_slot().lock() {
        *runtime = ContributorRuntimeState::default();
    }
}

fn contributor_runtime_snapshot() -> ContributorRuntimeState {
    contributor_runtime_slot()
        .lock()
        .map(|runtime| ContributorRuntimeState {
            job_running: runtime.job_running,
            current_job_id: runtime.current_job_id.clone(),
            last_job_id: runtime.last_job_id.clone(),
        })
        .unwrap_or_default()
}

fn stream_worker_logs<R>(reader: R)
where
    R: Read + Send + 'static,
{
    let lines = BufReader::new(reader).lines();
    for line in lines.map_while(Result::ok) {
        track_worker_runtime_from_log_line(&line);
    }
}

fn track_worker_runtime_from_log_line(line: &str) {
    let job_id = extract_log_field(line, "job_id");

    if line.contains("job received")
        || line.contains("starting job")
        || line.contains("starting container execution")
    {
        if let Ok(mut runtime) = contributor_runtime_slot().lock() {
            runtime.job_running = true;
            if let Some(id) = job_id.clone() {
                runtime.current_job_id = Some(id.clone());
                runtime.last_job_id = Some(id);
            }
        }
        return;
    }

    if line.contains("finished job") || line.contains("job failed before execution completion") {
        if let Ok(mut runtime) = contributor_runtime_slot().lock() {
            runtime.job_running = false;
            if let Some(id) = job_id {
                runtime.last_job_id = Some(id.clone());
                if runtime.current_job_id.as_deref() == Some(id.as_str()) {
                    runtime.current_job_id = None;
                }
            } else {
                runtime.current_job_id = None;
            }
        }
    }
}

fn extract_log_field(line: &str, key: &str) -> Option<String> {
    let pattern = format!("{key}=");
    let index = line.find(&pattern)? + pattern.len();
    let value = &line[index..];
    if value.is_empty() {
        return None;
    }

    if let Some(quoted) = value.strip_prefix('"') {
        let end = quoted.find('"')?;
        let parsed = quoted[..end].trim();
        return if parsed.is_empty() {
            None
        } else {
            Some(parsed.to_string())
        };
    }

    let raw = value.split_whitespace().next().unwrap_or_default().trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn resolve_worker_command() -> Result<Command, String> {
    if let Some(path) = optional_env("COMPUTEHIVE_WORKER_BIN") {
        let worker_bin = PathBuf::from(path);
        if worker_bin.exists() {
            return Ok(Command::new(worker_bin));
        }
    }

    let worker_dir = workspace_worker_dir()?;
    let built_by_make = worker_dir.join("../bin/computehive-worker");
    if built_by_make.exists() {
        return Ok(Command::new(built_by_make));
    }
    let compiled_worker = worker_dir.join("worker");
    if compiled_worker.exists() {
        return Ok(Command::new(compiled_worker));
    }

    let mut command = Command::new("go");
    command
        .current_dir(worker_dir)
        .arg("run")
        .arg("./cmd/worker");
    Ok(command)
}

fn configure_worker_command(command: &mut Command) -> Result<(), String> {
    #[cfg(unix)]
    {
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
    }

    Ok(())
}

fn stop_worker_process(child: &mut Child) {
    #[cfg(unix)]
    {
        let group_id = child.id() as i32;

        unsafe {
            let _ = libc::killpg(group_id, libc::SIGTERM);
        }

        thread::sleep(Duration::from_millis(350));

        if matches!(child.try_wait(), Ok(None)) {
            unsafe {
                let _ = libc::killpg(group_id, libc::SIGKILL);
            }
        }

        let _ = child.wait();
        return;
    }

    #[cfg(not(unix))]
    {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn workspace_worker_dir() -> Result<PathBuf, String> {
    if let Some(path) = optional_env("COMPUTEHIVE_WORKER_DIR") {
        let dir = PathBuf::from(path);
        if dir.exists() {
            return Ok(dir);
        }
    }

    let repo_guess = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../worker");
    if repo_guess.exists() {
        return Ok(repo_guess);
    }

    if let Ok(current_dir) = std::env::current_dir() {
        let sibling = current_dir.join("../worker");
        if sibling.exists() {
            return Ok(sibling);
        }
    }

    Err(
        "Could not locate worker directory. Set COMPUTEHIVE_WORKER_DIR or COMPUTEHIVE_WORKER_BIN."
            .to_string(),
    )
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
    // The worker only loads image tars whose tags are in the computehive-job namespace (so a crafted image cannot
    // overwrite the contributor's own images), so that is the repository this tag must use.
    let image_tag = format!("computehive-job:{}", sanitized_name);
    let image_file_name = format!("{sanitized_name}-computehive-image.tar");
    // The saved image is a build artifact, not part of the user's project: keep it out of their folder (and their
    // version control) in the OS temp directory, one subfolder per project path so two projects never collide.
    let archive_dir = std::env::temp_dir()
        .join("computehive-desktop")
        .join(format!("{:x}", {
            let digest = Sha256::digest(project_root.to_string_lossy().as_bytes());
            u64::from_be_bytes(digest[..8].try_into().unwrap_or([0; 8]))
        }));
    fs::create_dir_all(&archive_dir)
        .map_err(|e| format!("Failed to create the build folder {}: {e}", archive_dir.display()))?;
    let image_archive_path = archive_dir.join(&image_file_name);

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

fn coordinator_job_status_label(status: i32) -> &'static str {
    match status {
        1 => "queued",
        2 => "assigned",
        3 => "running",
        4 => "completed",
        5 => "failed",
        _ => "queued",
    }
}

fn coordinator_job_status_terminal(status: i32) -> bool {
    matches!(status, 4 | 5)
}

fn coordinator_result_status_name(status: i32) -> &'static str {
    match status {
        1 => "STATUS_QUEUED",
        2 => "STATUS_ASSIGNED",
        3 => "STATUS_RUNNING",
        4 => "STATUS_SUCCEEDED",
        5 => "STATUS_FAILED",
        6 => "STATUS_CANCELED",
        _ => "",
    }
}

fn coordinator_result_status_terminal(status: i32) -> bool {
    matches!(status, 4 | 5 | 6)
}

fn normalize_client_command(command: Vec<String>) -> Result<Vec<String>, String> {
    let lines = command
        .into_iter()
        .map(|segment| segment.trim().to_string())
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    // No command: the image's own CMD runs (the artifact is attached, so the coordinator allows this).
    if lines.is_empty() {
        return Ok(Vec::new());
    }

    let script = lines.join("\n");
    Ok(vec!["/bin/sh".to_string(), "-lc".to_string(), script])
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

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
// ---------------------------------------------------------------------------------------------------
// running a project on the cluster

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequestResult {
    image: DockerImageResult,
    tar_size_bytes: u64,
    artifact_sha256: String,
    job_id: String,
    summary: String,
    notes: Vec<String>,
}

/// What the user asked for beyond the defaults. Every field is optional; the coordinator validates them again.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct RunOptions {
    cpu_cores: Option<i32>,
    memory_mb: Option<i32>,
    gpu_count: Option<i32>,
    timeout_seconds: Option<i32>,
    min_disk_mb: Option<i32>,
    verify: bool,
    os: Option<String>,
    arch: Option<String>,
    labels: Vec<String>,
}

fn validate_options(o: &RunOptions) -> Result<(), String> {
    let check = |name: &str, v: Option<i32>, lo: i32, hi: i32| -> Result<(), String> {
        match v {
            Some(x) if x < lo || x > hi => Err(format!("{name} must be between {lo} and {hi}, got {x}")),
            _ => Ok(()),
        }
    };
    check("CPU cores", o.cpu_cores, 1, 1024)?;
    check("memory (MB)", o.memory_mb, 16, 4 * 1024 * 1024)?;
    check("GPUs", o.gpu_count, 0, 64)?;
    check("time limit (seconds)", o.timeout_seconds, 1, 7 * 24 * 3600)?;
    check("minimum free disk (MB)", o.min_disk_mb, 0, 16 * 1024 * 1024)?;
    if o.labels.len() > 16 {
        return Err("at most 16 labels are allowed".to_string());
    }
    Ok(())
}

/// Builds the project into an image, uploads it to the cluster, and submits the job. Returns as soon as the job
/// is queued; the UI then follows it with `get_job_progress`.
#[tauri::command]
async fn request_project_run(
    project_path: String,
    command: Vec<String>,
    options: Option<RunOptions>,
) -> Result<RunRequestResult, String> {
    let started = Instant::now();
    let cfg = cluster::load_config();
    if cfg.api_token.is_none() {
        return Err(
            "No API token found. Run `make up` in the repository (it writes .computehive/api.token), or set COMPUTEHIVE_TOKEN."
                .to_string(),
        );
    }
    let opts = options.unwrap_or_default();
    validate_options(&opts)?;
    let command_lines = normalize_client_command(command)?;

    let build_started = Instant::now();
    let build_path = project_path.clone();
    let image = tauri::async_runtime::spawn_blocking(move || build_project_docker_image(&build_path))
        .await
        .map_err(|e| format!("The image build task failed: {e}"))??;
    let build_ms = build_started.elapsed().as_millis();

    // The artifact is the plain `docker save` tar (the same bytes `computehive submit` uploads): the worker
    // inspects its tags before loading it. It is addressed by its SHA-256, which the store verifies.
    let archive = PathBuf::from(&image.image_archive_path);
    let upload_started = Instant::now();
    let (sha, tar_size) = tauri::async_runtime::spawn_blocking(move || -> Result<(String, u64), String> {
        let sha = sha256_for_file(&archive)?;
        let size = fs::metadata(&archive)
            .map_err(|e| format!("Failed to inspect the image archive: {e}"))?
            .len();
        cluster::upload_artifact_blocking(&archive, &sha)?;
        Ok((sha, size))
    })
    .await
    .map_err(|e| format!("The upload task failed: {e}"))??;
    let upload_ms = upload_started.elapsed().as_millis();

    let mut client = cluster::client(&cfg).await?;
    let response = client
        .submit_job(SubmitJobRequest {
            container_image: image.image_tag.clone(),
            command: command_lines,
            required_resources: Some(CoordinatorResourceSpec {
                cpu_cores: opts.cpu_cores.unwrap_or(DEFAULT_REQUIRED_CPU_CORES),
                gpu_count: opts.gpu_count.unwrap_or(DEFAULT_REQUIRED_GPU_COUNT),
                memory_mb: opts.memory_mb.unwrap_or(DEFAULT_REQUIRED_MEMORY_MB),
            }),
            max_runtime_seconds: opts.timeout_seconds.unwrap_or(DEFAULT_MAX_RUNTIME_SECONDS),
            artifact_url: format!("computehive-artifact://{sha}"),
            artifact_sha256: sha.clone(),
            verify: opts.verify,
            min_disk_mb: opts.min_disk_mb.unwrap_or(0),
            os: opts.os.unwrap_or_default(),
            arch: opts.arch.unwrap_or_default(),
            labels: opts.labels,
            ..Default::default()
        })
        .await
        .map_err(cluster::friendly)?;
    let job_id = response
        .into_inner()
        .job_id
        .map(|id| id.value)
        .unwrap_or_default();
    if job_id.trim().is_empty() {
        return Err("The coordinator did not return a job id.".to_string());
    }

    let mut notes = image.notes.clone();
    notes.push(format!(
        "Saved the image ({} bytes, sha256 {}) and uploaded it to the cluster's artifact store.",
        tar_size,
        &sha[..12.min(sha.len())]
    ));
    notes.push(format!(
        "Timing: image build {build_ms} ms, hash and upload {upload_ms} ms, total {} ms.",
        started.elapsed().as_millis()
    ));
    let summary = format!("Built the image, uploaded it, and queued job {job_id}.");
    Ok(RunRequestResult {
        image,
        tar_size_bytes: tar_size,
        artifact_sha256: sha,
        job_id,
        summary,
        notes,
    })
}

#[tauri::command]
async fn get_run_request_result(job_id: String) -> Result<Option<RunRequestJobResult>, String> {
    let trimmed_job_id = job_id.trim();
    if trimmed_job_id.is_empty() {
        return Err("job_id is required".to_string());
    }
    let cfg = cluster::load_config();
    let mut client = cluster::client(&cfg).await?;
    let response = match client
        .get_job_result(GetJobResultRequest {
            job_id: Some(coordinatorpb::JobId {
                value: trimmed_job_id.to_string(),
            }),
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(error) if error.code() == tonic::Code::NotFound => return Ok(None),
        Err(error) => return Err(cluster::friendly(error)),
    };

    let mut job_result = RunRequestJobResult {
        job_id: trimmed_job_id.to_string(),
        worker_id: String::new(),
        failure_reason: String::new(),
        status: coordinator_job_status_label(response.status).to_string(),
        result_status: String::new(),
        partial: false,
        exit_code: 0,
        stdout_excerpt: String::new(),
        stderr_excerpt: String::new(),
        output_uri: String::new(),
        output_api_url: String::new(),
        output_public_url: String::new(),
        output_size_bytes: 0,
        output_artifacts: Vec::new(),
        finished_at_unix: 0,
        completed: coordinator_job_status_terminal(response.status),
    };
    if let Some(result) = response.result {
        job_result.failure_reason = result.failure_reason.clone();
        job_result.result_status = coordinator_result_status_name(result.status).to_string();
        job_result.exit_code = result.exit_code;
        job_result.stdout_excerpt = result.stdout_excerpt;
        job_result.stderr_excerpt = result.stderr_excerpt;
        job_result.output_uri = result.output_uri;
        job_result.output_api_url = result.output_api_url;
        job_result.output_public_url = result.output_public_url;
        job_result.output_size_bytes = result.output_size_bytes;
        job_result.finished_at_unix = result.finished_at_unix;
        job_result.output_artifacts = result
            .output_artifacts
            .into_iter()
            .map(|a| RunRequestOutputArtifact {
                relative_path: a.relative_path,
                object_key: a.object_key,
                uri: a.uri,
                api_url: a.api_url,
                public_url: a.public_url,
                sha256: a.sha256,
                size_bytes: a.size_bytes,
                content_type: a.content_type,
            })
            .collect();
        job_result.completed = job_result.completed || coordinator_result_status_terminal(result.status);
    }
    Ok(Some(job_result))
}

// ---------------------------------------------------------------------------------------------------
// contributor mode: run a worker on this machine

/// Environment for the local worker: where the cluster is, the CA to trust, how to enrol, and where to keep its
/// state (including the private control socket the controls below talk to).
fn apply_worker_env(
    command: &mut Command,
    cfg: &cluster::ClusterConfig,
    state_dir: &Path,
    enrollment_token: Option<&str>,
) -> Result<(), String> {
    command
        .env("COORDINATOR_ADDR", &cfg.coordinator_addr)
        .env("COORDINATOR_HTTP_URL", &cfg.http_url)
        .env("STATE_DIR", state_dir)
        // The app follows the worker through its log lines, so keep them in the line-based text format.
        .env("LOG_FORMAT", "text");
    if let Some(pem) = &cfg.ca_pem {
        let ca_path = state_dir.join("cluster-ca.crt");
        fs::write(&ca_path, pem).map_err(|e| format!("Failed to save the cluster CA: {e}"))?;
        command.env("COORDINATOR_TLS_CA", ca_path);
        if let Some(name) = &cfg.tls_server_name {
            command.env("COORDINATOR_TLS_SERVER_NAME", name);
        }
    }
    if let Some(token) = enrollment_token {
        command.env("ENROLLMENT_TOKEN", token);
    }
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .unwrap_or_else(|| "this-machine".to_string());
    command.env("WORKER_ID", format!("desktop-{}", sanitize_name(&host)));
    Ok(())
}

/// Starts (or reports) the worker on this machine. If the worker has never enrolled, a single-use enrollment
/// token is used: the one the user typed, or one minted with the API token.
#[tauri::command]
async fn start_contributor_sharing(
    enrollment_token: Option<String>,
) -> Result<ContributorSharingStatus, String> {
    {
        let slot = contributor_worker_slot();
        let mut guard = slot
            .lock()
            .map_err(|_| "Contributor worker process lock is poisoned.".to_string())?;
        if let Some(child) = guard.as_mut() {
            match child.try_wait() {
                Ok(None) => {
                    let runtime = contributor_runtime_snapshot();
                    return Ok(ContributorSharingStatus {
                        running: true,
                        pid: Some(child.id()),
                        details: "Sharing is already running.".to_string(),
                        job_running: runtime.job_running,
                        current_job_id: runtime.current_job_id,
                        last_job_id: runtime.last_job_id,
                    });
                }
                Ok(Some(_)) | Err(_) => *guard = None,
            }
        }
    }

    let cfg = cluster::load_config();
    let state_dir = cluster::contributor_state_dir();
    fs::create_dir_all(&state_dir).map_err(|e| format!("Failed to create {}: {e}", state_dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700));
    }

    // A worker that already enrolled keeps its own credential; only a first start needs a token.
    let token = if state_dir.join("credential").exists() {
        None
    } else {
        match enrollment_token.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()) {
            Some(t) => Some(t),
            None => Some(cluster::mint_enrollment_token(&cfg).await.map_err(|e| {
                format!("This machine has not joined the cluster yet and no enrollment token was given. Paste one (from `computehive admin enroll`), or provide the API token so the app can create one. ({e})")
            })?),
        }
    };

    reset_contributor_runtime_state();
    let mut command = resolve_worker_command()?;
    configure_worker_command(&mut command)?;
    apply_worker_env(&mut command, &cfg, &state_dir, token.as_deref())?;
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .stdin(Stdio::null())
        .spawn()
        .map_err(|error| format!("Failed to start local worker process: {error}"))?;
    if let Some(stdout) = child.stdout.take() {
        thread::spawn(move || stream_worker_logs(stdout));
    }
    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || stream_worker_logs(stderr));
    }
    let pid = child.id();
    *contributor_worker_slot()
        .lock()
        .map_err(|_| "Contributor worker process lock is poisoned.".to_string())? = Some(child);

    let runtime = contributor_runtime_snapshot();
    Ok(ContributorSharingStatus {
        running: true,
        pid: Some(pid),
        details: "Sharing started. The worker is joining the cluster.".to_string(),
        job_running: runtime.job_running,
        current_job_id: runtime.current_job_id,
        last_job_id: runtime.last_job_id,
    })
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            create_project_docker_image,
            request_project_run,
            get_run_request_result,
            start_contributor_sharing,
            stop_contributor_sharing,
            get_contributor_sharing_status,
            cluster::cluster_overview,
            cluster::get_job_progress,
            cluster::explain_job,
            cluster::cancel_job,
            cluster::download_job_outputs,
            cluster::contributor_control_status,
            cluster::contributor_set_policy,
            cluster::contributor_pause,
            cluster::contributor_resume
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// The whole desktop path against the real, secured cluster: build the example project, upload it, submit,
    /// follow it to the end, and read the result. Needs `make up` and Docker. Run: cargo test -- --ignored
    #[test]
    #[ignore = "needs a running cluster (make up) and Docker"]
    fn live_build_upload_submit_follow_and_read_the_result() {
        // Build from a throwaway copy: the app writes helper files (a .dockerignore) into the project it builds,
        // and a test must not leave anything in the repository.
        let project = std::env::temp_dir().join(format!("computehive-live-hello-{}", std::process::id()));
        let _ = fs::remove_dir_all(&project);
        fs::create_dir_all(&project).expect("temp project");
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/hello/Dockerfile"),
            project.join("Dockerfile"),
        )
        .expect("copy the example Dockerfile");
        let res = tauri::async_runtime::block_on(request_project_run(
            project.display().to_string(),
            Vec::new(), // no command: the image's own CMD runs
            Some(RunOptions::default()),
        ))
        .expect("request_project_run");
        assert!(res.image.image_tag.starts_with("computehive-job:"), "the worker only accepts this namespace: {}", res.image.image_tag);
        assert!(!res.job_id.is_empty());

        let deadline = Instant::now() + Duration::from_secs(120);
        let last = loop {
            let p = tauri::async_runtime::block_on(cluster::get_job_progress(res.job_id.clone())).expect("progress");
            if p.terminal {
                break p.status;
            }
            assert!(Instant::now() < deadline, "job did not finish in time (last status {})", p.status);
            thread::sleep(Duration::from_secs(1));
        };
        assert_eq!(last, "succeeded");

        let result = tauri::async_runtime::block_on(get_run_request_result(res.job_id.clone()))
            .expect("result")
            .expect("a finished job has a result");
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout_excerpt.contains("hello from"), "stdout was {:?}", result.stdout_excerpt);

        let why = tauri::async_runtime::block_on(cluster::explain_job(res.job_id)).expect("explain");
        assert!(why.evaluations.iter().any(|e| e.chosen), "the decision should name the chosen worker");
        let _ = fs::remove_dir_all(&project);
    }

    /// The contributor path from the app: start a worker (it enrols with a token the app mints), watch it appear in
    /// the cluster, change its limits over the control socket, pause and resume it, then stop it. Uses a throwaway
    /// state directory so it never touches a real worker's credential.
    #[test]
    #[ignore = "needs a running cluster (make up), Docker and the worker binary (make build)"]
    fn live_contributor_start_control_and_stop() {
        let dir = std::env::temp_dir().join(format!("computehive-desktop-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        std::env::set_var("COMPUTEHIVE_STATE_DIR", &dir);
        std::env::set_var("HOSTNAME", "livetest");

        let started = tauri::async_runtime::block_on(start_contributor_sharing(None)).expect("start sharing");
        assert!(started.running);

        // The worker needs a moment to enrol and open its control socket.
        let deadline = Instant::now() + Duration::from_secs(40);
        let status = loop {
            match cluster::contributor_control_status() {
                Ok(v) => break v,
                Err(e) => {
                    assert!(Instant::now() < deadline, "the worker never became controllable: {e}");
                    thread::sleep(Duration::from_millis(500));
                }
            }
        };
        assert_eq!(status["worker_id"], "desktop-livetest");

        // Change the limits the way the sliders do: send the whole policy back with edited fields.
        let mut policy = status["policy"].clone();
        policy["max_cpus"] = serde_json::json!(2);
        policy["max_job_disk_mb"] = serde_json::json!(128);
        let applied = cluster::contributor_set_policy(policy).expect("apply policy");
        assert_eq!(applied["max_cpus"], 2.0);
        assert_eq!(applied["max_job_disk_mb"], 128);
        // A policy with an unknown field must be refused, never silently half-applied.
        assert!(cluster::contributor_set_policy(serde_json::json!({"max_cpuz": 1})).is_err());

        cluster::contributor_pause(false).expect("pause");
        assert_eq!(cluster::contributor_control_status().unwrap()["policy"]["paused"], true);
        cluster::contributor_resume().expect("resume");
        assert_eq!(cluster::contributor_control_status().unwrap()["policy"]["paused"], false);

        // It is a real member of the cluster while it runs.
        let overview = tauri::async_runtime::block_on(cluster::cluster_overview());
        assert!(overview.workers.iter().any(|w| w.id == "desktop-livetest"), "the worker did not join the cluster");

        let stopped = tauri::async_runtime::block_on(async { stop_contributor_sharing() }).expect("stop sharing");
        assert!(!stopped.running);
        std::env::remove_var("COMPUTEHIVE_STATE_DIR");
        let _ = fs::remove_dir_all(&dir);
    }
}
