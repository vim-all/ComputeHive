# gVisor (`runsc`) isolation

By default jobs run in **hardened runc**: non-root, all capabilities dropped, no-new-privileges, read-only root
filesystem, default seccomp profile, resource limits and no network. Containers still share the host kernel, so a
kernel vulnerability is the main residual risk (see `THREAT_MODEL.md`, residual risk 1).

[gVisor](https://gvisor.dev) interposes a user-space kernel, so a job's syscalls never reach the host kernel directly.
ComputeHive supports it as an option:

| `ISOLATION` / `--isolation` | Behaviour |
|---|---|
| `auto` (default) | use `runsc` if the Docker daemon has it registered, otherwise hardened runc |
| `runc` | never use gVisor |
| `gvisor` | require it: the worker refuses to start if the daemon has no `runsc` runtime |

The level in use (`hardened-runc` or `gvisor`) is reported in every heartbeat, shown by `computehive worker status`,
and recorded in the job history. A submitter can demand it: `computehive submit DIR --isolation gvisor` only runs on
gVisor workers; a contributor can demand it of themselves with `computehive worker policy set require_gvisor=true`.

## Install (Linux)

1. Install `runsc` and `containerd-shim-runsc-v1` from the gVisor releases and put them on `PATH`.
2. `sudo runsc install` (registers the runtime in `/etc/docker/daemon.json`), then `sudo systemctl restart docker`.
3. Check: `docker info --format '{{json .Runtimes}}'` lists `runsc`.

## What was and was not verified

The detection (`executor.DetectRuntime`), the `--runtime runsc` flag, the isolation level in heartbeats, the scheduler
filter (`TestRequiredIsolationIsHonouredByTheClaimScript`) and the `require_gvisor` admission rule are covered by tests.
**Running jobs under gVisor was not verified**: the development machine's Docker has only `runc`, so no `runsc` container
was ever started. Treat the gVisor path as implemented but unproven until you run `make hostile` on a host that has it.
Some workloads behave differently under gVisor (unusual syscalls, performance of syscall-heavy code).
