"""Worker handlers for different execution types.

PythonHandler runs arbitrary Python code in a subprocess with resource monitoring.
TaskRunnerHandler wraps a user-defined TaskHandler for in-process Pydantic-typed tasks.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from dataclasses import dataclass

import psutil

# ---------------------------------------------------------------------------
# Handler result
# ---------------------------------------------------------------------------


@dataclass
class HandlerResult:
    """Result of handler execution."""

    status: str = "completed"
    exit_code: int | None = None
    logs: str = ""
    error: str = ""
    response: str | None = None  # JSON string for task runners


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

HANDLER_REGISTRY: dict[str, type[WorkerHandler]] = {}


def register_handler(cls):
    """Class decorator: register a handler by its worker_type."""
    HANDLER_REGISTRY[cls.worker_type] = cls
    return cls


class WorkerHandler:
    """Base class for worker handlers."""

    worker_type: str = ""
    worker_config: dict | None = None

    async def execute(
        self, spec: dict, workdir: str, rconn, sid: str, channel: str,
    ) -> HandlerResult:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

POLL_INTERVAL = 0.05
PYTHON = sys.executable


async def _stream_output(
    pipe, log_file, rconn, channel, *, log_lines_key=None, max_log_lines=0,
):
    """Read lines from subprocess pipe, write to log file, publish to Redis.

    If *log_lines_key* is set and *max_log_lines* > 0, each line is also
    buffered in a Redis list (capped to the last *max_log_lines* entries) so
    clients can retrieve recent output even without a pub/sub subscription.
    """
    buffer = log_lines_key and max_log_lines > 0
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
            if buffer:
                with contextlib.suppress(Exception):
                    await rconn.rpush(log_lines_key, text)
                    await rconn.ltrim(log_lines_key, -max_log_lines, -1)
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


def _make_preexec(max_disk_mb: int):
    """Return a preexec_fn that sets resource limits in the child.

    Memory is enforced by psutil RSS monitoring + container cgroup limits,
    NOT rlimits - RLIMIT_AS breaks scientific libraries (numpy/OpenBLAS)
    that map large virtual regions without actually committing them.
    """

    def _preexec():
        import resource as _r

        disk_bytes = max_disk_mb * 1024 * 1024
        with contextlib.suppress(ValueError, OSError):
            _r.setrlimit(_r.RLIMIT_FSIZE, (disk_bytes, disk_bytes))
        # Note: RLIMIT_NPROC removed — OpenBLAS/scikit-learn need threads.
        # Container pids_limit (128) provides the real defense-in-depth.

    return _preexec


# ---------------------------------------------------------------------------
# Python handler — runs arbitrary code in a subprocess
# ---------------------------------------------------------------------------

RUNNER_TEMPLATE = """\
import os, sys, traceback

# Clear sensitive environment variables before running user code
_safe = {{'HOME', 'PATH', 'LANG', 'TERM', 'TMPDIR', 'PYTHONPATH',
         'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'}}
for _k in list(os.environ):
    if _k not in _safe:
        del os.environ[_k]

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


@register_handler
class PythonHandler(WorkerHandler):
    worker_type = "python"

    def __init__(self, worker_type="python"):
        self.worker_type = worker_type

    async def execute(self, spec, workdir, rconn, sid, channel):
        code = spec["code"]
        max_runtime = spec.get("max_runtime_seconds", 30)
        max_memory = spec.get("max_memory_mb", 128)
        max_disk = spec.get("max_disk_mb", 50)

        # Write user code
        code_path = os.path.join(workdir, "__session_code.py")
        with open(code_path, "w") as f:
            f.write(code)

        # Write runner wrapper
        runner_code = RUNNER_TEMPLATE.format(workdir=workdir)
        runner_path = os.path.join(workdir, "__session_runner.py")
        with open(runner_path, "w") as f:
            f.write(runner_code)

        # Spawn subprocess
        log_path = os.path.join(workdir, "__session.log")
        log_file = open(log_path, "w")  # noqa: SIM115

        proc = await asyncio.create_subprocess_exec(
            PYTHON, "-u", runner_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=workdir,
            preexec_fn=_make_preexec(max_disk),
        )

        max_log_lines = spec.get("max_log_lines", 0)
        log_lines_key = f"session:{sid}:logs:lines" if max_log_lines > 0 else None
        streamer = asyncio.create_task(
            _stream_output(
                proc.stdout, log_file, rconn, channel,
                log_lines_key=log_lines_key, max_log_lines=max_log_lines,
            )
        )

        # Monitor loop
        start = time.monotonic()
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
            result = HandlerResult(status="killed", exit_code=-1, error=killed_reason)
        else:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=5)
            final_exit = proc.returncode
            if final_exit < 0:
                sig_name = _signal_name(-final_exit)
                result = HandlerResult(
                    status="killed", exit_code=final_exit,
                    error=f"killed by signal {sig_name}",
                )
            else:
                result = HandlerResult(
                    status="completed" if final_exit == 0 else "error",
                    exit_code=final_exit,
                )

        await streamer
        log_file.close()

        # Collect logs
        if os.path.exists(log_path):
            with open(log_path) as f:
                result.logs = f.read()

        return result


# ---------------------------------------------------------------------------
# Task runner handler — wraps a user-defined TaskHandler for typed tasks
# ---------------------------------------------------------------------------


class TaskRunnerHandler(WorkerHandler):
    """Wraps a TaskHandler for in-process Pydantic-typed task execution."""

    def __init__(self, worker_type: str, task_handler):
        self.worker_type = worker_type
        self.task_handler = task_handler

    async def execute(self, spec, workdir, rconn, sid, channel):
        max_runtime = spec.get("max_runtime_seconds", 30)
        try:
            return await asyncio.wait_for(
                self._run_task(spec, workdir),
                timeout=max_runtime,
            )
        except TimeoutError:
            return HandlerResult(
                status="killed",
                exit_code=-1,
                error=f"exceeded max runtime ({max_runtime}s)",
            )

    async def _run_task(self, spec, workdir):
        try:
            # Deserialize task
            task_json = spec.get("task", "{}")
            task = self.task_handler.task_model.model_validate_json(task_json)

            # Read input files from workdir (already written by run_session)
            input_files_spec = spec.get("input_files", {})
            input_files = {}
            for name in input_files_spec:
                safe_name = os.path.basename(name)
                fpath = os.path.join(workdir, safe_name)
                if os.path.exists(fpath):
                    with open(fpath, "rb") as f:
                        input_files[safe_name] = f.read()

            # Execute
            response, output_files = await self.task_handler.execute(task, input_files)

            # Write output files to workdir (collected by run_session)
            for fname, data in output_files.items():
                with open(os.path.join(workdir, fname), "wb") as f:
                    f.write(data)

            # Serialize response
            response_json = response.model_dump_json()
            return HandlerResult(
                status="completed",
                exit_code=0,
                logs=response_json + "\n",
                response=response_json,
            )
        except Exception as exc:
            return HandlerResult(
                status="error",
                exit_code=1,
                error=str(exc),
                logs=f"Error: {exc}\n",
            )
