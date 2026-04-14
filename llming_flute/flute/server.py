"""
Async Python Session Server backed by Redis.

Sessions are submitted via Redis. Each session runs using a registered handler
for the configured worker type. The server monitors sessions and enforces
resource limits via the handler.

Worker type is set via WORKER_TYPE env var (default: "python").
Each type listens on its own queue: session:queue:{worker_type}.

Workers are configured via WORKER_DIR env var pointing to a directory with a
pyproject.toml containing [tool.flute.worker] config. Task runner workers
define a handler class that receives Pydantic-typed tasks and returns typed
responses.

Designed to run inside a locked-down Docker container (no network egress,
read-only rootfs, tmpfs workdirs, cgroup memory/cpu/pids caps).
"""

import asyncio
import base64
import contextlib
import json
import os
import shutil
import signal
import tempfile
import time
import traceback

from flute.handlers import (
    HANDLER_REGISTRY,
    HandlerResult,
    PythonHandler,
    ServiceHandler,
    TaskRunnerHandler,
    WorkerHandler,
    _dir_size_mb,
    _kill_tree,
    _make_preexec,
    _signal_name,
    _stream_output,
)
from flute.quota_rules import get_quota_for_type, parse_quota_rules
from flute.redis_conn import connect_redis
from flute.worker_config import load_task_handler, load_worker_config

# Re-export for backward compatibility and coverage
__all__ = [
    "HandlerResult",
    "PythonHandler",
    "ServiceHandler",
    "TaskRunnerHandler",
    "WorkerHandler",
    "_dir_size_mb",
    "_kill_tree",
    "_make_preexec",
    "_signal_name",
    "_stream_output",
    "create_handler",
    "run_session",
    "serve",
    "main",
    "_charge_quota",
    "_heartbeat",
    "_register_service",
    "_store_sample_sources",
]

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment)
# ---------------------------------------------------------------------------
DEFAULT_MAX_RUNTIME = 30  # seconds
DEFAULT_MAX_MEMORY_MB = 128  # megabytes
DEFAULT_MAX_DISK_MB = 50  # megabytes
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "4"))
WORKDIR_BASE = os.environ.get("WORKDIR_BASE", tempfile.gettempdir())
WORKER_TYPE = os.environ.get("WORKER_TYPE", "python")
WORKER_DIR = os.environ.get("WORKER_DIR", "")
QUOTA_SECONDS = int(os.environ.get("QUOTA_SECONDS", "600"))  # 10 min default
QUOTA_INTERVAL = int(os.environ.get("QUOTA_INTERVAL", "3600"))  # 1 hour window
QUOTA_RULES = os.environ.get("QUOTA_RULES", f"*:{QUOTA_SECONDS}/{QUOTA_INTERVAL}")
SESSION_TTL = int(os.environ.get("SESSION_TTL", "1800"))  # 30 min default
MAX_LOG_LINES = int(os.environ.get("MAX_LOG_LINES", "500"))
INFRA_TTL = 120  # Infrastructure keys (workers, services) TTL in seconds

_quota_rules = parse_quota_rules(QUOTA_RULES)


# ---------------------------------------------------------------------------
# Handler creation from worker directory config
# ---------------------------------------------------------------------------


def create_handler(worker_dir: str | None = None) -> WorkerHandler:
    """Create a handler from a worker directory's pyproject.toml config.

    If worker_dir is None or empty, falls back to HANDLER_REGISTRY lookup
    using WORKER_TYPE.
    """
    if not worker_dir:
        handler_cls = HANDLER_REGISTRY.get(WORKER_TYPE, PythonHandler)
        return handler_cls()

    config = load_worker_config(worker_dir)
    wtype = config["type"]
    handler_ref = config.get("handler")

    if wtype == "service-runner" and handler_ref:
        module_name, class_name = handler_ref.split(":")
        handler = ServiceHandler(
            worker_type=config["name"],
            worker_dir=worker_dir,
            module_name=module_name,
            class_name=class_name,
        )
    elif wtype == "task-runner" and handler_ref:
        task_handler = load_task_handler(worker_dir, handler_ref)
        handler = TaskRunnerHandler(
            worker_type=config["name"],
            task_handler=task_handler,
        )
    else:
        # Python runner — use PythonHandler with the worker's name as type
        handler = PythonHandler(worker_type=config["name"])

    handler.worker_config = config
    return handler


# ---------------------------------------------------------------------------
# Quota tracking
# ---------------------------------------------------------------------------


async def _charge_quota(rconn, user_id, worker_type, elapsed, quota_interval):
    """Atomically add elapsed seconds to the user's quota key, setting TTL on first use."""
    key = f"quota:{user_id}:{worker_type}"
    await rconn.incrbyfloat(key, round(elapsed, 2))
    if await rconn.ttl(key) < 0:
        await rconn.expire(key, quota_interval)


# ---------------------------------------------------------------------------
# Session execution (runs as an asyncio task)
# ---------------------------------------------------------------------------


async def run_session(spec: dict, rconn, handler: WorkerHandler):
    sid = spec["session_id"]
    user_id = spec.get("user_id")
    wt = handler.worker_type

    # Inject log buffering config so handlers can buffer lines
    spec = {**spec, "max_log_lines": MAX_LOG_LINES}

    async def _set(key, val, ex=SESSION_TTL):
        await rconn.set(f"session:{sid}:{key}", val, ex=ex)

    # --- quota pre-check ---
    quota_limit, quota_interval = get_quota_for_type(
        _quota_rules, wt, QUOTA_SECONDS, QUOTA_INTERVAL,
    )
    if user_id:
        quota_key = f"quota:{user_id}:{wt}"
        current = await rconn.get(quota_key)
        if current is not None and float(current) >= quota_limit:
            await _set("status", "quota_exceeded")
            await _set("error", (
                f"quota exceeded: {float(current):.0f}s / {quota_limit}s"
                f" used in current {quota_interval}s window"
            ))
            return

    workdir = tempfile.mkdtemp(prefix=f"session_{sid}_", dir=WORKDIR_BASE)
    os.chmod(workdir, 0o700)  # Restrict access to session owner only
    await _set("workdir", workdir)
    session_start = None

    try:
        # --- write input files ---
        input_files = spec.get("input_files", {})
        for fname, b64data in input_files.items():
            safe_name = os.path.basename(fname)
            fpath = os.path.join(workdir, safe_name)
            with open(fpath, "wb") as f:
                f.write(base64.b64decode(b64data))

        # --- execute via handler ---
        await _set("status", "running")
        channel = f"session:{sid}:logs:stream"
        session_start = time.monotonic()

        result = await handler.execute(spec, workdir, rconn, sid, channel)

        # --- store logs ---
        await _set("logs", result.logs)

        # --- collect output files ---
        internal = {"__session_code.py", "__session_runner.py", "__session.log"}
        input_names = {os.path.basename(f) for f in input_files}
        out_key = f"session:{sid}:output_files"
        for fname in os.listdir(workdir):
            if fname in internal or fname in input_names:
                continue
            fpath = os.path.join(workdir, fname)
            if os.path.isfile(fpath):
                with open(fpath, "rb") as f:
                    await rconn.hset(out_key, fname, base64.b64encode(f.read()).decode())
        await rconn.expire(out_key, SESSION_TTL)

        # Set TTL on buffered log lines
        log_lines_key = f"session:{sid}:logs:lines"
        await rconn.expire(log_lines_key, SESSION_TTL)

        # --- charge quota ---
        if user_id and session_start is not None:
            await _charge_quota(
                rconn, user_id, wt, time.monotonic() - session_start, quota_interval,
            )

        # --- store response (task runners) ---
        if result.response is not None:
            await _set("response", result.response)

        # --- set terminal status LAST so clients see complete data ---
        if result.exit_code is not None:
            await _set("exit_code", result.exit_code)
        await _set("error", result.error)
        await _set("status", result.status)

    except Exception:
        if user_id and session_start is not None:
            await _charge_quota(
                rconn, user_id, wt, time.monotonic() - session_start, quota_interval,
            )
        await _set("status", "error")
        await _set("error", traceback.format_exc())
        await _set("logs", traceback.format_exc())
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Worker heartbeat
# ---------------------------------------------------------------------------


async def _heartbeat(rconn, worker_type, worker_id, shutdown_event, handler=None):
    """Periodically update worker heartbeat in Redis."""
    key = f"{worker_type}:{worker_id}"
    try:
        while not shutdown_event.is_set():
            info = json.dumps({
                "worker_type": worker_type,
                "last_heartbeat": time.time(),
                "pid": os.getpid(),
            })
            await rconn.hset("workers", key, info)
            # Keep infrastructure keys alive while this worker runs
            await rconn.expire("workers", INFRA_TTL)
            # Re-register service info each heartbeat (survives key expiry)
            if handler is not None:
                await _register_service(rconn, handler)
            await rconn.expire("services", INFRA_TTL)
            await rconn.set("quota:__config:rules", QUOTA_RULES, ex=INFRA_TTL)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shutdown_event.wait(), timeout=10)
    finally:
        with contextlib.suppress(Exception):
            await rconn.hdel("workers", key)


# ---------------------------------------------------------------------------
# Service registration
# ---------------------------------------------------------------------------


async def _register_service(rconn, handler: WorkerHandler):
    """Register this worker's capabilities in the Redis 'services' hash."""
    config = handler.worker_config or {}
    info: dict = {
        "worker_type": handler.worker_type,
        "type": config.get("type", "python-runner"),
        "description": config.get("description", ""),
        "dependencies": config.get("dependencies", []),
    }
    if isinstance(handler, ServiceHandler):
        info["type"] = "service-runner"
        info["operations"] = config.get("operations", [])
        info["accept_files"] = config.get("accept_files", [])
    elif isinstance(handler, TaskRunnerHandler):
        info["type"] = "task-runner"
        th = handler.task_handler
        info["task_schema"] = th.task_model.model_json_schema()
        info["response_schema"] = th.response_model.model_json_schema()
        # Prefer handler description if config doesn't have one
        if not info["description"]:
            info["description"] = getattr(th, "description", "")
    # Include sample index if the worker provides one
    samples = config.get("samples")
    if samples:
        info["samples"] = samples
        # Store sample file contents in Redis for client retrieval
        worker_dir = config.get("worker_dir", "")
        await _store_sample_sources(rconn, handler.worker_type, worker_dir, samples)
    await rconn.hset("services", handler.worker_type, json.dumps(info))


async def _store_sample_sources(rconn, worker_type, worker_dir, samples_index):
    """Store sample script contents in Redis so clients can fetch them."""
    samples_dir = os.path.join(worker_dir, "samples")
    key = f"samples:{worker_type}"
    for cat in samples_index.get("categories", []):
        for item in cat.get("items", []):
            fpath = os.path.join(samples_dir, item["file"])
            if os.path.isfile(fpath):
                with open(fpath) as f:
                    await rconn.hset(key, item["file"], f.read())
    await rconn.expire(key, INFRA_TTL)


# ---------------------------------------------------------------------------
# Server loop
# ---------------------------------------------------------------------------


async def serve(rconn, shutdown_event, max_concurrent, handler=None):
    """Main async serve loop. Takes explicit parameters for testability."""
    if handler is None:
        handler = create_handler(WORKER_DIR)
    wt = handler.worker_type
    queue_key = f"session:queue:{wt}"

    # Publish quota config so clients can read limits via quota()
    await rconn.set("quota:__config:rules", QUOTA_RULES)

    # Register service capabilities
    await _register_service(rconn, handler)

    # Start heartbeat
    import uuid

    worker_id = uuid.uuid4().hex[:8]
    heartbeat_task = asyncio.create_task(
        _heartbeat(rconn, wt, worker_id, shutdown_event, handler)
    )

    slots = asyncio.Semaphore(max_concurrent)
    active_tasks: list[asyncio.Task] = []

    while not shutdown_event.is_set():
        await slots.acquire()

        if shutdown_event.is_set():
            slots.release()
            break

        result = await rconn.blpop(queue_key, timeout=1)
        if result is None:
            slots.release()
            continue

        _, payload = result
        try:
            spec = json.loads(payload)
        except json.JSONDecodeError as e:
            print(f"[server] invalid JSON in queue: {e}")
            slots.release()
            continue

        sid = spec.get("session_id", "unknown")
        print(f"[server] starting session {sid}")
        await rconn.set(f"session:{sid}:status", "queued", ex=SESSION_TTL)

        async def _worker(s=spec, sem=slots):
            try:
                await run_session(s, rconn, handler)
            finally:
                sem.release()

        task = asyncio.create_task(_worker())
        active_tasks.append(task)

        active_tasks[:] = [t for t in active_tasks if not t.done()]

    # Stop heartbeat
    shutdown_event.set()  # Ensure heartbeat sees shutdown
    await heartbeat_task

    for t in active_tasks:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(t, timeout=55)


async def main():  # pragma: no cover
    r = await connect_redis(REDIS_URL, decode_responses=True)
    await r.ping()

    handler = create_handler(WORKER_DIR)
    wt = handler.worker_type
    print(f"[server] worker type: {wt}")
    print(f"[server] connected to Redis at {REDIS_URL}")
    print(f"[server] max concurrent sessions: {MAX_CONCURRENT}")
    print(f"[server] workdir base: {WORKDIR_BASE}")
    print(f"[server] quota rules: {QUOTA_RULES}")
    print(f"[server] waiting for sessions on 'session:queue:{wt}' ...")

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: (
                print(
                    f"\n[server] received {_signal_name(s)}, draining active sessions..."
                ),
                shutdown_event.set(),
            ),
        )

    await serve(r, shutdown_event, MAX_CONCURRENT, handler)
    await r.aclose()
    print("[server] shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
