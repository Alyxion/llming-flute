"""Base class for long-running data processing services.

Services stay alive between jobs, loading expensive models once on startup.
Protocol: files in, files out -- same as Python runner.

Example::

    from flute.service import Service

    class MyService(Service):
        operations = ["grayscale", "resize"]

        def setup(self):
            import time
            time.sleep(5)           # simulate heavy model loading
            self.model = "loaded"

        def process(self, job):
            img = job.read_file("input.png")
            result = self.transform(img)
            job.write_file("output.png", result)

            job.progress(0.5, "grayscale")      # reports ETA from history

            if job.check_quota() <= 0:
                raise RuntimeError("Quota exceeded")
"""

from __future__ import annotations

import json
import os
import sys
import time

# ---------------------------------------------------------------------------
# Job -- represents a single processing request
# ---------------------------------------------------------------------------


class Job:
    """A single processing request with file I/O and progress helpers."""

    def __init__(
        self,
        job_id: str,
        workdir: str,
        params: dict,
        steps: list[str],
        quota_budget: float,
        max_runtime: float,
        service: Service,
    ):
        self.job_id = job_id
        self.workdir = workdir
        self.params = params
        self.steps = steps
        self.quota_budget = quota_budget
        self.max_runtime = max_runtime
        self._service = service
        self._start = time.monotonic()

    # -- File I/O --

    def read_file(self, name: str, max_bytes: int = 100 * 1024 * 1024) -> bytes:
        """Read an input file.  Raises ValueError if *name* exceeds *max_bytes*."""
        path = os.path.join(self.workdir, name)
        size = os.path.getsize(path)
        if size > max_bytes:
            raise ValueError(
                f"{name}: {size / (1024 * 1024):.1f}MB exceeds "
                f"limit of {max_bytes / (1024 * 1024):.0f}MB"
            )
        with open(path, "rb") as f:
            return f.read()

    def write_file(self, name: str, data: bytes) -> None:
        """Write an output file to the workdir."""
        with open(os.path.join(self.workdir, name), "wb") as f:
            f.write(data)

    def input_files(self) -> list[str]:
        """List files currently in the workdir."""
        return [
            f
            for f in os.listdir(self.workdir)
            if os.path.isfile(os.path.join(self.workdir, f))
        ]

    # -- Progress & quota --

    def progress(
        self, value: float, step: str = "", eta_seconds: float | None = None,
    ) -> None:
        """Report progress (0.0--1.0) with an optional step label and ETA."""
        msg: dict = {
            "type": "progress",
            "job_id": self.job_id,
            "value": round(value, 3),
            "step": step,
        }
        if eta_seconds is not None:
            msg["eta_seconds"] = round(eta_seconds, 1)
        elif step and step in self._service._perf:
            avg = self._service._perf_avg(step)
            elapsed = time.monotonic() - self._service._step_start
            msg["eta_seconds"] = round(max(0.0, avg - elapsed), 1)
        self._service._send(msg)

    def check_quota(self) -> float:
        """Remaining quota budget in seconds (0 when exceeded)."""
        return max(0.0, self.quota_budget - self.elapsed)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start


# ---------------------------------------------------------------------------
# stdout capture -- redirect print() to JSON protocol messages
# ---------------------------------------------------------------------------


class _Capture:
    """Intercepts print() and converts to protocol ``log`` messages."""

    def __init__(self, service: Service):
        self._svc = service
        self.job_id: str | None = None

    def write(self, text: str) -> None:
        if text and text.strip():
            self._svc._send({
                "type": "log",
                "job_id": self.job_id or "__setup__",
                "text": text if text.endswith("\n") else text + "\n",
            })

    def flush(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Service base class
# ---------------------------------------------------------------------------


class Service:
    """Base class for long-running data processing services.

    Subclass and override :meth:`setup` and :meth:`process`.
    The process stays alive between requests -- use ``setup()`` to
    load heavy models once.
    """

    #: List of supported operation names (exposed via service discovery).
    operations: list[str] = []
    #: Max input file size in bytes.  Files exceeding this are rejected
    #: *before* ``process()`` is called, preventing memory blowouts.
    max_input_bytes: int = 50 * 1024 * 1024

    def __init__(self) -> None:
        self._perf: dict[str, list[float]] = {}
        self._step_start: float = 0.0
        self._proto_out = None

    # -- Override these --

    def setup(self) -> None:
        """Called once on startup.  Load models, warm caches, etc."""

    def process(self, job: Job) -> None:
        """Handle one request.  Read from *job*, write outputs, call progress."""
        raise NotImplementedError

    # -- Performance tracking --

    def _perf_avg(self, step: str) -> float:
        d = self._perf.get(step, [])
        return sum(d) / len(d) if d else 0.0

    def _record_perf(self, step: str, duration: float) -> None:
        """Record a step duration (kept for the last 20 samples)."""
        self._perf.setdefault(step, []).append(duration)
        if len(self._perf[step]) > 20:
            self._perf[step] = self._perf[step][-20:]

    # -- Protocol plumbing --

    def _send(self, msg: dict) -> None:
        self._proto_out.write(json.dumps(msg) + "\n")
        self._proto_out.flush()

    def run(self) -> None:
        """Main loop -- called by the generated runner script.  Do **not** override."""
        self._proto_out = sys.stdout
        capture = _Capture(self)
        sys.stdout = capture  # print() -> protocol log messages

        self.setup()
        self._send({"type": "ready"})

        while True:
            try:
                line = sys.stdin.readline()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                break

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            if msg.get("type") != "job":
                continue

            job_id = msg["job_id"]
            capture.job_id = job_id
            job = Job(
                job_id=job_id,
                workdir=msg["workdir"],
                params=msg.get("params", {}),
                steps=msg.get("steps", []),
                quota_budget=msg.get("quota_budget_seconds", 3600),
                max_runtime=msg.get("max_runtime_seconds", 30),
                service=self,
            )

            try:
                # Memory safety: reject oversized input files early
                for fname in job.input_files():
                    fpath = os.path.join(job.workdir, fname)
                    fsize = os.path.getsize(fpath)
                    if fsize > self.max_input_bytes:
                        raise ValueError(
                            f"{fname}: {fsize / (1024 * 1024):.1f}MB exceeds "
                            f"limit of {self.max_input_bytes / (1024 * 1024):.0f}MB"
                        )

                self._step_start = time.monotonic()
                self.process(job)

                perf = {
                    op: round(self._perf_avg(op), 3) for op in self._perf
                }
                self._send({
                    "type": "done",
                    "job_id": job_id,
                    "elapsed": round(job.elapsed, 3),
                    "perf": perf,
                })
            except Exception as exc:
                self._send({
                    "type": "error",
                    "job_id": job_id,
                    "error": str(exc),
                    "elapsed": round(time.monotonic() - job._start, 3),
                })
            finally:
                capture.job_id = None
