"""
Async Python Session Server backed by Redis.

Sessions are submitted via Redis. Each session runs in an isolated subprocess
with strict resource limits (CPU time, memory, disk). The server monitors
sessions and kills any that exceed their limits.

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
import sys
import tempfile
import time
import traceback

import psutil

from flute.redis_conn import connect_redis

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment)
# ---------------------------------------------------------------------------
DEFAULT_MAX_RUNTIME = 30  # seconds
DEFAULT_MAX_MEMORY_MB = 128  # megabytes
DEFAULT_MAX_DISK_MB = 50  # megabytes
POLL_INTERVAL = 0.05  # 50ms
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "4"))
WORKDIR_BASE = os.environ.get("WORKDIR_BASE", tempfile.gettempdir())
PYTHON = sys.executable
QUOTA_SECONDS = int(os.environ.get("QUOTA_SECONDS", "600"))  # 10 min default
QUOTA_INTERVAL = int(os.environ.get("QUOTA_INTERVAL", "3600"))  # 1 hour window

# ---------------------------------------------------------------------------
# Runner wrapper script written into the workdir
# ---------------------------------------------------------------------------

RUNNER_TEMPLATE = """\
import os, sys, traceback

os.chdir({workdir!r})

try:
    exec(compile(open("__session_code.py").read(), "<session>", "exec"),
         {{"__builtins__": __builtins__}})
except SystemExit:
    raise
except Exception:
    traceback.print_exc()
    sys.exit(1)
"""


def _make_preexec(max_disk_mb: int):
    """Return a preexec_fn that sets resource limits in the child before exec.

    Memory is enforced by psutil RSS monitoring + container cgroup limits,
    NOT rlimits - RLIMIT_AS breaks scientific libraries (numpy/OpenBLAS)
    that map large virtual regions without actually committing them.
    """

    def _preexec():
        import resource as _r

        disk_bytes = max_disk_mb * 1024 * 1024
        with contextlib.suppress(ValueError, OSError):
            _r.setrlimit(_r.RLIMIT_FSIZE, (disk_bytes, disk_bytes))

    return _preexec


# ---------------------------------------------------------------------------
# Session execution (runs as an asyncio task)
# ---------------------------------------------------------------------------


async def _charge_quota(rconn, user_id, elapsed):
    """Atomically add elapsed seconds to the user's quota key, setting TTL on first use."""
    key = f"quota:{user_id}"
    await rconn.incrbyfloat(key, round(elapsed, 2))
    if await rconn.ttl(key) < 0:
        await rconn.expire(key, QUOTA_INTERVAL)


async def run_session(spec: dict, rconn):
    sid = spec["session_id"]
    user_id = spec.get("user_id")

    async def _set(key, val, ex=3600):
        await rconn.set(f"session:{sid}:{key}", val, ex=ex)

    code = spec["code"]
    max_runtime = spec.get("max_runtime_seconds", DEFAULT_MAX_RUNTIME)
    max_memory = spec.get("max_memory_mb", DEFAULT_MAX_MEMORY_MB)
    max_disk = spec.get("max_disk_mb", DEFAULT_MAX_DISK_MB)

    # --- quota pre-check ---
    if user_id:
        current = await rconn.get(f"quota:{user_id}")
        if current is not None and float(current) >= QUOTA_SECONDS:
            await _set("status", "quota_exceeded")
            await _set("error", (
                f"quota exceeded: {float(current):.0f}s / {QUOTA_SECONDS}s"
                f" used in current {QUOTA_INTERVAL}s window"
            ))
            return

    workdir = tempfile.mkdtemp(prefix=f"session_{sid}_", dir=WORKDIR_BASE)
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

        # --- write the user code ---
        code_path = os.path.join(workdir, "__session_code.py")
        with open(code_path, "w") as f:
            f.write(code)

        # --- write the runner wrapper ---
        runner_code = RUNNER_TEMPLATE.format(workdir=workdir)
        runner_path = os.path.join(workdir, "__session_runner.py")
        with open(runner_path, "w") as f:
            f.write(runner_code)

        # --- spawn subprocess with rlimits via preexec_fn ---
        await _set("status", "running")
        log_path = os.path.join(workdir, "__session.log")
        log_file = open(log_path, "w")  # noqa: SIM115

        proc = await asyncio.create_subprocess_exec(
            PYTHON, "-u", runner_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=workdir,
            preexec_fn=_make_preexec(max_disk),
        )

        channel = f"session:{sid}:logs:stream"
        streamer = asyncio.create_task(
            _stream_output(proc.stdout, log_file, rconn, channel)
        )

        # --- monitor loop ---
        start = time.monotonic()
        session_start = start
        killed_reason = None
        wait_task = asyncio.create_task(proc.wait())

        while not wait_task.done():
            elapsed = time.monotonic() - start

            if elapsed > max_runtime:
                killed_reason = f"exceeded max runtime ({max_runtime}s)"
                break

            try:
                ps = psutil.Process(proc.pid)
                rss_mb = ps.memory_info().rss / (1024 * 1024)
                if rss_mb > max_memory:
                    killed_reason = (
                        f"exceeded memory limit ({max_memory} MB, used {rss_mb:.1f} MB)"
                    )
                    break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            disk_usage = _dir_size_mb(workdir)
            if disk_usage > max_disk:
                killed_reason = (
                    f"exceeded disk limit ({max_disk} MB, used {disk_usage:.1f} MB)"
                )
                break

            await asyncio.sleep(POLL_INTERVAL)

        if killed_reason:
            _kill_tree(proc.pid)
            try:
                await asyncio.wait_for(asyncio.shield(wait_task), timeout=5)
            except (TimeoutError, ProcessLookupError):
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await asyncio.wait_for(asyncio.shield(wait_task), timeout=5)
            final_status = "killed"
            final_exit = -1
            await _set("error", killed_reason)
        else:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=5)
            final_exit = proc.returncode
            if final_exit < 0:
                final_status = "killed"
                sig_name = _signal_name(-final_exit)
                await _set("error", f"killed by signal {sig_name}")
            else:
                final_status = "completed" if final_exit == 0 else "error"

        await streamer
        log_file.close()

        # --- collect logs ---
        logs = ""
        if os.path.exists(log_path):
            with open(log_path) as f:
                logs = f.read()
        await _set("logs", logs)

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
        await rconn.expire(out_key, 3600)

        # --- charge quota ---
        if user_id and session_start is not None:
            await _charge_quota(rconn, user_id, time.monotonic() - session_start)

        # --- set terminal status LAST so clients see complete data ---
        await _set("exit_code", final_exit)
        await _set("status", final_status)

    except Exception:
        if user_id and session_start is not None:
            await _charge_quota(rconn, user_id, time.monotonic() - session_start)
        await _set("status", "error")
        await _set("error", traceback.format_exc())
        await _set("logs", traceback.format_exc())
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def _stream_output(pipe, log_file, rconn, channel):
    """Read lines from subprocess pipe, write to log file, publish to Redis."""
    try:
        while True:
            line = await pipe.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace")
            log_file.write(text)
            log_file.flush()
            with contextlib.suppress(Exception):
                await rconn.publish(channel, text)
    finally:
        with contextlib.suppress(Exception):
            await rconn.publish(channel, "")


def _dir_size_mb(path: str) -> float:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            with contextlib.suppress(OSError):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)


def _kill_tree(pid: int):
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            child.kill()
        parent.kill()
    except psutil.NoSuchProcess:
        pass


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except (ValueError, AttributeError):
        return str(signum)


# ---------------------------------------------------------------------------
# Server loop
# ---------------------------------------------------------------------------


async def serve(rconn, shutdown_event, max_concurrent):
    """Main async serve loop. Takes explicit parameters for testability."""
    # Publish quota config so clients can read limits via quota()
    await rconn.set("quota:__config:limit", QUOTA_SECONDS)
    await rconn.set("quota:__config:interval", QUOTA_INTERVAL)

    slots = asyncio.Semaphore(max_concurrent)
    active_tasks: list[asyncio.Task] = []

    while not shutdown_event.is_set():
        await slots.acquire()

        if shutdown_event.is_set():
            slots.release()
            break

        result = await rconn.blpop("session:queue", timeout=1)
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
        await rconn.set(f"session:{sid}:status", "queued", ex=3600)

        async def _worker(s=spec, sem=slots):
            try:
                await run_session(s, rconn)
            finally:
                sem.release()

        task = asyncio.create_task(_worker())
        active_tasks.append(task)

        active_tasks[:] = [t for t in active_tasks if not t.done()]

    for t in active_tasks:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(t, timeout=55)


async def main():  # pragma: no cover
    r = await connect_redis(REDIS_URL, decode_responses=True)
    await r.ping()
    print(f"[server] connected to Redis at {REDIS_URL}")
    print(f"[server] max concurrent sessions: {MAX_CONCURRENT}")
    print(f"[server] workdir base: {WORKDIR_BASE}")
    print("[server] waiting for sessions on 'session:queue' ...")

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: (
                print(f"\n[server] received {_signal_name(s)}, draining active sessions..."),
                shutdown_event.set(),
            ),
        )

    await serve(r, shutdown_event, MAX_CONCURRENT)
    await r.aclose()
    print("[server] shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
