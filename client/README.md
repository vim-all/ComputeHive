# ComputeHive Desktop

The desktop interface to a ComputeHive cluster: run a project, watch the cluster, and share your own machine.

- frontend: Vite + TypeScript (no UI framework), in `src/`
- backend: Tauri 2 + Rust, in `src-tauri/`

It talks to the **same secured API as the `computehive` CLI**: gRPC over TLS with the API token, and the coordinator's own
artifact/output store over HTTPS. It never touches Redis or an object store, so it needs no credentials of its own.

## What it does

| Tab | What you can do |
|---|---|
| **Run** | Pick a project folder (a Dockerfile is generated if there is none), set a command, CPU/RAM/GPU/time limit, optional placement (OS, architecture, labels, free disk) and result verification. The app builds the image, uploads it, submits the job and follows it: status, the worker it ran on, **live output**, **why that worker was chosen**, the result, and **downloading output files** (each verified against its SHA-256). |
| **Cluster** | Every worker with its state, platform, RAM/CPU/disk against the 90% safety limit, reliability and labels; overload reasons; recent jobs (click one to follow it). |
| **Contributor** | Start or stop sharing this machine. While sharing: pause (or "pause now", which hands the running job back), CPU and memory to donate, disk per job, idle-only with a threshold, a schedule, and the network allow-list. Limits are enforced by the local worker, so the cluster cannot override them. |

## Run it

```bash
make up                         # from the repository root: starts the cluster and writes .computehive/
cd client
npm install
npm run tauri dev               # the desktop app
# or, to look at the UI alone in a browser with sample data:
npm run dev                     # http://localhost:1420  (a banner says it is preview mode)
```

Prerequisites: Node 20+, Rust (stable), and the Tauri system libraries for your OS
(<https://tauri.app/start/prerequisites/>); Docker for building images and, in contributor mode, running jobs.

## How it finds the cluster

In order: environment variables (`COMPUTEHIVE_COORDINATOR` / `COORDINATOR_ADDR`, `COMPUTEHIVE_TOKEN`, `COMPUTEHIVE_CA`,
`COMPUTEHIVE_HTTP`, `COMPUTEHIVE_TLS_SERVER_NAME`), then a `.computehive/` directory (holding `api.token` and `ca.crt`) in the working
directory, next to the executable, in the source checkout, or `~/.config/computehive/`. If a token or CA is missing the app says what to do
instead of failing silently; `make up` creates both.

For a remote cluster, copy `ca.crt` and `api.token` into `~/.config/computehive/` and set `COMPUTEHIVE_COORDINATOR=host:50051`.

## Contributor mode

"Start sharing" runs the worker binary on this machine (`bin/computehive-worker` from `make build`, else `worker/worker`, else `go run`).
If this machine has not joined the cluster yet, a single-use enrollment token is used: the one you paste under *First time on this
cluster?*, or one the app creates with the API token. The worker keeps its own revocable credential and state in
`~/.local/state/computehive-desktop/` (override with `COMPUTEHIVE_STATE_DIR`); its control socket there (mode 0600) is what the controls use.

## Layout and responsiveness

The page scrolls (nothing has a fixed height); cards reflow with `auto-fit` grids; tables and logs scroll inside their own wrappers; every
grid child can shrink; the header is sticky and wraps on narrow windows. The window can be resized down to 340 x 460. This was audited in a
real browser at 7 sizes (340 to 1920 px) on all three tabs, checking horizontal overflow, elements past the edge, clipped content,
reachability of the end of each page and tap-target size, with a finished job and the contributor panel populated.

## Tests

```bash
cd src-tauri
cargo test                                  # unit tests: path safety, address parsing, the control-socket HTTP parser, labels
cargo test -- --ignored --test-threads=1    # live tests against a running cluster (make up) and Docker
cd .. && npm run build                      # type-checks (tsc) and builds the frontend
```

The live tests build and run `examples/hello` through the app's own commands (TLS, token, upload, submit, follow, result, explain), check
that a wrong token is rejected with a useful message, and drive the contributor path (start a worker, enrol, change limits over the
socket, pause, resume, stop). They are what caught a TLS crypto-provider panic that would have crashed the packaged app on its first
connection.

Not automated: driving the native Tauri window itself.

## Troubleshooting

| Symptom | Fix |
|---|---|
| "No API token found" | Run `make up` from the repository, or set `COMPUTEHIVE_TOKEN`. |
| "Cluster unreachable" chip | Is the cluster running? `docker compose ps`. For a remote cluster check `COMPUTEHIVE_COORDINATOR` and that `ca.crt` matches it. |
| Run fails at "build" | Docker is not running or not reachable by this user. |
| Contributor: "has not joined the cluster yet" | Paste an enrollment token from `computehive admin enroll`, or provide the API token. |
| Contributor controls say the worker "is starting" | It is enrolling and opening its socket; this takes a few seconds. |
