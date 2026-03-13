# Security Model

Flute runs untrusted code. The security model is defense-in-depth: multiple
independent layers ensure that a compromise at one level does not escalate.

**Workers MUST always run inside Docker.** The container is the primary security
boundary. Without it, user code has full access to the host filesystem and
network. There is no safe "local bare-metal" mode — `python -m flute.server`
outside Docker is only for framework development with trusted test code.

## Layer 1: Container Isolation

Every worker runs inside a locked-down Docker container.

```yaml
read_only: true               # immutable root filesystem
tmpfs: /tmp:size=512M         # writable temp only via tmpfs
cap_drop: [ALL]               # no Linux capabilities
security_opt: no-new-privileges
mem_limit: 1g / 512m          # per-container memory cap
memswap_limit: 1g / 512m      # no swap
cpus: 2.0 / 1.0               # CPU time cap
pids_limit: 256 / 64          # process/thread limit
```

The container runs as a non-root user (`flute:flute`) with `HOME=/tmp`.

### Network Isolation

Workers are on an `internal: true` Docker network. They can reach Redis but
have **no internet access and no LAN access**. Only the `control` network
(Redis host port) is bridged to the host.

## Layer 2: Process-Level Limits

The Python subprocess has additional resource limits set via `preexec_fn`:

| Limit | Mechanism | Default |
|-------|-----------|---------|
| Disk write size | `RLIMIT_FSIZE` | `max_disk_mb` (50 MB) |
| Runtime | Wall-clock monitoring in handler loop | `max_runtime_seconds` (30s) |
| Memory (RSS) | psutil monitoring every 50ms | `max_memory_mb` (128 MB) |
| Disk usage | Directory size polling | `max_disk_mb` (50 MB) |

**Why not RLIMIT_AS?** Virtual address space limits break scientific libraries
like NumPy/OpenBLAS that map large virtual regions without committing physical
memory. RSS monitoring + container cgroup limits provide the real defense.

**Why not RLIMIT_NPROC?** OpenBLAS and scikit-learn need worker threads.
The container `pids_limit` provides the ceiling.

When any limit is exceeded, the entire process tree is killed via `psutil`.

## Layer 3: Code Sandboxing

The runner wrapper (`RUNNER_TEMPLATE`) clears all environment variables before
executing user code, keeping only a safe allowlist:

```python
_safe = {'HOME', 'PATH', 'LANG', 'TERM', 'TMPDIR', 'PYTHONPATH',
         'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'}
for _k in list(os.environ):
    if _k not in _safe:
        del os.environ[_k]
```

The workdir is `chmod 0o700` and ephemeral (cleaned up via `shutil.rmtree`
in a `finally` block).

## Layer 4: Quota Enforcement

Per-user, per-worker-type CPU time quotas prevent resource abuse:

- Checked **before** execution (reject if already exceeded)
- Charged **after** execution (wall-clock seconds)
- Charged **even on failure/kill** (so crashes can't be exploited for free compute)
- Window-based with automatic expiry (Redis key TTL = interval)

## Kubernetes Hardening

The K8s manifests add:

```yaml
securityContext:
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: [ALL]
```

Plus resource requests/limits and an `emptyDir` for `/tmp`.

## What Flute Does NOT Protect Against

- **Kernel exploits.** If the container runtime itself is compromised,
  isolation fails. Use gVisor or Kata Containers for stronger isolation.
- **Side channels.** Timing attacks, CPU cache attacks, etc. are out of scope.
- **Denial of service via valid code.** A user can consume their full quota
  with legitimate but expensive computation. Quotas are the mitigation.
- **Network egress from the host.** The `internal` network blocks container
  egress, but the host itself can still reach the internet.
