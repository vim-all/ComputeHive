# Threat model

Scope: ComputeHive in **contributor mode**, where strangers' jobs run on volunteers' machines, and the
coordinator that connects them. This document is written *before* the Phase 4 implementation and is the
checklist the implementation is held to. Each mitigation names the test that demonstrates it; anything not
demonstrated is listed under "Residual risks" rather than claimed.

## 1. Assets (what we protect, in priority order)

| # | Asset | Owner | Why it matters |
|---|---|---|---|
| A1 | The contributor's machine: files, credentials, browser sessions, SSH keys, cloud credentials | Contributor | Volunteers will not run the worker if it can hurt them |
| A2 | The contributor's network position: LAN devices, router, localhost services, cloud metadata (169.254.169.254) | Contributor | A job with network access can pivot from a laptop into the LAN |
| A3 | The contributor's resources: CPU, RAM, disk, battery, Docker daemon, their own Docker images and registry logins | Contributor | A job must not starve the machine or tamper with their images |
| A4 | The integrity of results | Submitter | A malicious worker can return wrong output |
| A5 | The coordinator, its Redis, and the queue | Operator | One compromised worker must not be able to take over the cluster |
| A6 | Submitters' code and data | Submitter | Jobs are visible to whichever worker runs them (see residual risks) |
| A7 | Availability of the cluster | Everyone | Flooding, poisoned jobs and crash loops waste capacity |

## 2. Attackers

| ID | Attacker | Capability |
|---|---|---|
| T1 | **Malicious submitter** | Can submit any image/command/env to the cluster; wants to escape the container, read the host, scan the LAN, mine crypto, or wreck the machine |
| T2 | **Malicious worker (volunteer)** | Runs the real worker binary or a modified one; wants to forge results, steal other users' jobs, get assigned work it should not, or attack the coordinator |
| T3 | **Network attacker** | On the path between worker and coordinator (coffee-shop Wi-Fi); can eavesdrop, replay, impersonate |
| T4 | **Unauthenticated internet client** | Can reach the coordinator's ports if exposed; can flood it or enrol rogue workers |
| T5 | **Compromised/malicious coordinator** | Can send arbitrary job specs to workers |
| T6 | **Curious worker owner** | Reads the submitters' code and data that run on their machine |

Out of scope: a malicious *operator* of the coordinator (they already control everything), physical access to a
contributor's machine, and kernel/Docker 0-days (reduced, not eliminated, by gVisor).

## 3. Trust boundaries

```
 submitter ──(1)──> coordinator ──(2)──> worker process ──(3)──> Docker daemon ──(4)──> job container
 (T1,T4)            (A5)         (T3)    (trusted by the        (root on the host)      (UNTRUSTED code)
                                          contributor)
```

1. Submitter to coordinator: authenticated API token; input validated and size-capped.
2. Coordinator to worker: TLS; enrolment token then a revocable per-worker credential; optional mTLS.
3. The worker is **trusted infrastructure** and is the component that must keep the boundary at (4).
4. The job container is the only place untrusted code runs.

## 4. Threats and mitigations (STRIDE-style, by boundary)

### 4.1 Untrusted code in the container (T1 against A1, A2, A3)

| Threat | Mitigation | Evidence |
|---|---|---|
| Container escape via privileges | Never `--privileged`; `--cap-drop=ALL`; `no-new-privileges`; non-root user; no `--cap-add`, `--device`, `--pid/--ipc/--net=host`. A final argument audit refuses to start a container whose arguments contain any of these | `TestSafeArgs*`, hostile suite: `CapEff=0`, `NoNewPrivs=1`, uid 65534 |
| Kernel attack surface | Docker's default seccomp profile (explicitly verified active); optional **gVisor (`runsc`)** user-space kernel, auto-detected and reported as the isolation level | `TestHostile/seccomp_filter_active`; gVisor test skipped if `runsc` is absent |
| Writing outside the sandbox | Read-only root filesystem; small `noexec` tmpfs scratch; the only writable bind mount is the job's own output directory | `TestHostile/write_outside_sandbox` |
| Reading host files | No host mounts other than the job's output directory; PID/mount/IPC namespaces isolated | `TestHostile/read_host_files` (sentinel file never visible) |
| Reaching the Docker socket | The socket is never mounted, and the argument audit rejects any mount containing `docker.sock` | `TestHostile/docker_socket` |
| Network access: LAN scan, exfiltration, cloud metadata (169.254.169.254) | `--network none` by default. Optional egress only through a **filtering proxy sidecar**: the job joins a per-job `--internal` Docker network (no route anywhere, and no route to the host either) whose only peer is a locked-down sidecar container; the sidecar alone has a way out. It is used only when the job asks for it **and** the contributor's policy allows it; the proxy resolves DNS itself, refuses private/loopback/link-local/CGNAT/NAT64/metadata ranges after resolution (no DNS-rebinding), allows only listed hosts and ports 80/443 | `TestHostile/(a)`, `(i)`, `TestNetworkAllowlistModeOnRealDocker`, `TestConnectDeniedCases`, `TestDNSRebindingCannotRedirectTheConnection` |
| Resource exhaustion: memory, CPU, processes, disk, descriptors | `--memory` = `--memory-swap`; `--cpus`; `--pids-limit`; `ulimit nofile/fsize/core`; wall-clock timeout with forced kill; output-directory size watcher that kills the container; stdout/stderr capped | `TestHostile/{memory,fork_bomb,cpu_spin,disk_fill}` |
| Leaving junk behind (containers, volumes, images) | Containers removed deterministically (not `--rm`, which can strand `Dead` containers); loaded images removed after the job; startup sweep of this worker's orphans | `TestCleanup*`, hostile suite asserts nothing is left |
| Poisoning the contributor's Docker (overwriting `ubuntu:latest` via a crafted image tar) | The image tar's manifest is inspected **before** `docker load`; every tag must be inside the `computehive-job:` namespace, or the load is refused | `TestInspectImageTar*` |
| Using the contributor's registry credentials / pulling arbitrary images | `--pull=never` always; worker runs Docker with an empty `DOCKER_CONFIG`, so stored logins are never used; image allow/deny policy | `TestExecutorEnvIsolatesDockerConfig`, `TestImagePolicy*` |
| Flag injection through job fields (image starting with `-`, env names) | Image reference and environment names validated against strict patterns on the worker (and the coordinator); no field is ever concatenated into a shell | `TestValidateImageRef`, `TestSafeArgsRejectsInjection` |

### 4.2 The worker and the coordinator (T2, T3, T4, T5 against A4, A5, A7)

| Threat | Mitigation | Evidence |
|---|---|---|
| Eavesdropping, tampering, impersonating the coordinator | TLS on the gRPC channel (server certificate verified against a pinned CA); **mTLS** supported (`--tls-client-ca`) | `TestMutualTLS*` |
| Rogue worker enrols itself | Enrolment requires an **admin-issued token that expires** (and has a use limit) | `TestEnrollment*` |
| Stolen worker credential | Per-worker credential, stored **hashed** server-side, **revocable**; revoking requeues the worker's jobs and denies later calls | `TestRevokeWorker*` |
| Unauthenticated job submission or admin actions | API token required on all client/admin RPCs (`AUTH_DISABLED=true` is explicit and logs a loud warning) | `TestAuth*` |
| Flooding the coordinator | Per-identity token-bucket rate limits (`ResourceExhausted`), request size limits, bounded input validation on every RPC | `TestRateLimit*`, `TestValidate*` |
| A worker returns forged results | Optional **redundant execution** (`verify`): the job runs on two different workers and output digests are compared; mismatch fails the job | `TestVerify*` |
| A worker claims jobs it should not (overloaded, wrong isolation) | Eligibility is decided inside the atomic claim script from coordinator-held data, not from the worker's say-so (see Phase 3) | Phase 2/3 tests |
| Compromised coordinator sends a hostile job spec | The worker re-validates every field and applies the contributor's local policy; the sandbox does not depend on the spec being honest | `TestPolicy*`, hostile suite |
| Replaying an old result | Lease tokens: a result is accepted once, only for the current attempt | Phase 2 tests |

### 4.3 Contributor control (A1, A3)

The contributor, not the cluster, has the last word: CPU/RAM/disk donated, schedule (idle-only or hours), pause/resume,
a **kill switch** that stops everything and exits, network on/off and allowed hosts, image allow/deny, and a history of
what ran. These are enforced by the worker locally; the coordinator cannot override them.
Evidence: `TestPolicy*`, `TestControl*`.

## 5. Residual risks (not eliminated; stated honestly)

1. **Container escape through a kernel or runc vulnerability.** Containers share the host kernel. Hardening reduces the
   attack surface (no caps, seccomp, no-new-privileges, read-only rootfs) but cannot rule out a kernel 0-day. gVisor
   substantially reduces this risk but is optional and was **not verified in the development environment** (no `runsc`
   installed there). Mitigation: keep the host patched; prefer gVisor for untrusted workloads.
2. **Network allowlist mode (resolved by design, residual remains small).** An earlier design served the proxy on the
   Docker bridge gateway; testing on real Docker showed internal networks cannot reach the gateway at all, and that
   design could in any case have exposed host services bound to wildcard addresses. The shipped design (sidecar on the
   job's internal network) was measured on real Docker: from inside the job the default route, direct internet, direct
   DNS, `169.254.169.254`, private and loopback addresses, port 22, unlisted hosts, a host service on `0.0.0.0` and the
   bridge gateway were all unreachable, while the allowed host worked over both HTTP and a CONNECT tunnel. What remains:
   the sidecar itself can reach the internet and (through the default bridge) the host, constrained only by the proxy's own
   address filter, so a bug in that filter is a bug in the boundary. Network mode needs the worker to run directly on the
   host (not in a container) and is off by default.
3. **Disk quota is enforced by a watcher, not by the filesystem.** Measured: a job writing zeros with `dd` against a 50 MB
   cap was killed after 119-125 MB had been written (about 2.4x overshoot at memory speed, 100 ms polling, 20 MB writes).
   Mitigation: `RLIMIT_FSIZE` caps any single file at the quota, the host-disk safety rule (Phase 3) still trips at 90%,
   and the work directory should live on its own partition.
4. **Confidentiality of submitted code and data (A6).** The worker owner can read whatever runs on their machine.
   ComputeHive provides no confidentiality against the worker. Do not submit secrets.
5. **Result integrity for non-deterministic jobs.** Redundant execution compares digests, so a legitimately
   non-deterministic job will mismatch, and two colluding workers can agree on a wrong answer. It is a detection
   mechanism, not a proof.
6. **Denial of service by a determined submitter.** Rate limits, quotas, retries and a dead-letter state bound the damage
   but a flood of valid jobs still consumes capacity.
7. **Dev-mode defaults.** `AUTH_DISABLED=true` and plaintext gRPC exist for local experiments and are called out in logs.
8. **Local control interface.** The pause/kill API listens on a Unix socket readable only by the worker's user; any process
   running as that user can use it.
9. **Metadata and timing side channels** between co-tenant jobs on the same worker are not addressed (one job per worker by default).
