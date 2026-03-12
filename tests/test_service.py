"""Unit tests for flute.service — Service base class, Job, and _Capture."""

import io
import json
import os
import time

import pytest
from flute.service import Job, Service, _Capture

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class DummyService(Service):
    """Minimal service for testing Job and _Capture without subprocess."""

    def setup(self):
        pass

    def process(self, job):
        pass


def _make_service(proto_out=None):
    """Create a DummyService with protocol output wired to a StringIO."""
    svc = DummyService()
    svc._proto_out = proto_out or io.StringIO()
    return svc


def _make_job(
    svc=None, workdir="/tmp", job_id="j1", params=None,
    steps=None, quota_budget=3600, max_runtime=30,
):
    if svc is None:
        svc = _make_service()
    return Job(
        job_id=job_id,
        workdir=workdir,
        params=params or {},
        steps=steps or [],
        quota_budget=quota_budget,
        max_runtime=max_runtime,
        service=svc,
    )


def _read_messages(sio):
    """Parse all JSON messages from a StringIO."""
    sio.seek(0)
    msgs = []
    for line in sio:
        line = line.strip()
        if line:
            msgs.append(json.loads(line))
    return msgs


# ---------------------------------------------------------------------------
# Job.read_file
# ---------------------------------------------------------------------------


class TestJobReadFile:
    def test_normal(self, tmp_path):
        """Reading a file within max_bytes returns its contents."""
        (tmp_path / "data.bin").write_bytes(b"hello")
        job = _make_job(workdir=str(tmp_path))
        assert job.read_file("data.bin") == b"hello"

    def test_exceeds_max_bytes(self, tmp_path):
        """Reading a file exceeding max_bytes raises ValueError."""
        (tmp_path / "big.bin").write_bytes(b"x" * 200)
        job = _make_job(workdir=str(tmp_path))
        with pytest.raises(ValueError, match="exceeds limit"):
            job.read_file("big.bin", max_bytes=100)

    def test_missing_file(self, tmp_path):
        """Reading a nonexistent file raises OSError."""
        job = _make_job(workdir=str(tmp_path))
        with pytest.raises(OSError):
            job.read_file("nonexistent.bin")


# ---------------------------------------------------------------------------
# Job.write_file
# ---------------------------------------------------------------------------


class TestJobWriteFile:
    def test_writes_data(self, tmp_path):
        """write_file creates a file with the given contents."""
        job = _make_job(workdir=str(tmp_path))
        job.write_file("output.bin", b"result data")
        assert (tmp_path / "output.bin").read_bytes() == b"result data"

    def test_overwrites_existing(self, tmp_path):
        """write_file overwrites an existing file."""
        (tmp_path / "out.bin").write_bytes(b"old")
        job = _make_job(workdir=str(tmp_path))
        job.write_file("out.bin", b"new")
        assert (tmp_path / "out.bin").read_bytes() == b"new"


# ---------------------------------------------------------------------------
# Job.input_files
# ---------------------------------------------------------------------------


class TestJobInputFiles:
    def test_lists_files(self, tmp_path):
        """input_files returns a list of file names in the workdir."""
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.png").write_bytes(b"\x89PNG")
        job = _make_job(workdir=str(tmp_path))
        names = sorted(job.input_files())
        assert names == ["a.txt", "b.png"]

    def test_excludes_directories(self, tmp_path):
        """input_files does not include directories."""
        (tmp_path / "subdir").mkdir()
        (tmp_path / "file.txt").write_text("ok")
        job = _make_job(workdir=str(tmp_path))
        assert job.input_files() == ["file.txt"]

    def test_empty_dir(self, tmp_path):
        """input_files returns an empty list for an empty directory."""
        job = _make_job(workdir=str(tmp_path))
        assert job.input_files() == []


# ---------------------------------------------------------------------------
# Job.progress
# ---------------------------------------------------------------------------


class TestJobProgress:
    def test_basic(self):
        """progress sends a progress message with value and step."""
        buf = io.StringIO()
        svc = _make_service(buf)
        job = _make_job(svc=svc, job_id="p1")
        job.progress(0.5, "resize")
        msgs = _read_messages(buf)
        assert len(msgs) == 1
        assert msgs[0]["type"] == "progress"
        assert msgs[0]["job_id"] == "p1"
        assert msgs[0]["value"] == 0.5
        assert msgs[0]["step"] == "resize"

    def test_with_explicit_eta(self):
        """progress with explicit eta_seconds includes it in the message."""
        buf = io.StringIO()
        svc = _make_service(buf)
        job = _make_job(svc=svc)
        job.progress(0.75, "encode", eta_seconds=12.34)
        msgs = _read_messages(buf)
        assert msgs[0]["eta_seconds"] == 12.3

    def test_with_historical_perf(self):
        """progress computes eta from historical performance data."""
        buf = io.StringIO()
        svc = _make_service(buf)
        svc._perf["grayscale"] = [10.0, 12.0, 8.0]
        svc._step_start = time.monotonic()
        job = _make_job(svc=svc)
        job.progress(0.5, "grayscale")
        msgs = _read_messages(buf)
        assert "eta_seconds" in msgs[0]
        assert msgs[0]["eta_seconds"] >= 0

    def test_no_eta_without_perf_data(self):
        """progress without perf data or explicit eta omits eta_seconds."""
        buf = io.StringIO()
        svc = _make_service(buf)
        job = _make_job(svc=svc)
        job.progress(0.5, "unknown_step")
        msgs = _read_messages(buf)
        assert "eta_seconds" not in msgs[0]

    def test_value_rounded(self):
        """progress value is rounded to 3 decimal places."""
        buf = io.StringIO()
        svc = _make_service(buf)
        job = _make_job(svc=svc)
        job.progress(0.33333333)
        msgs = _read_messages(buf)
        assert msgs[0]["value"] == 0.333


# ---------------------------------------------------------------------------
# Job.check_quota
# ---------------------------------------------------------------------------


class TestJobCheckQuota:
    def test_under_budget(self):
        """check_quota returns remaining budget when under limit."""
        job = _make_job(quota_budget=100.0)
        remaining = job.check_quota()
        assert remaining > 99.0  # just started, nearly full budget

    def test_exceeded(self):
        """check_quota returns 0 when budget is exceeded."""
        job = _make_job(quota_budget=0.0)
        assert job.check_quota() == 0.0


# ---------------------------------------------------------------------------
# Job.elapsed
# ---------------------------------------------------------------------------


class TestJobElapsed:
    def test_increases_over_time(self):
        """elapsed property returns time since job creation."""
        job = _make_job()
        first = job.elapsed
        # Sleep a tiny bit to ensure elapsed increases
        time.sleep(0.01)
        second = job.elapsed
        assert second > first
        assert first >= 0.0


# ---------------------------------------------------------------------------
# Service._perf_avg
# ---------------------------------------------------------------------------


class TestServicePerfAvg:
    def test_empty(self):
        """_perf_avg returns 0.0 for an unknown step."""
        svc = _make_service()
        assert svc._perf_avg("unknown") == 0.0

    def test_with_values(self):
        """_perf_avg returns the arithmetic mean of recorded durations."""
        svc = _make_service()
        svc._perf["step1"] = [10.0, 20.0, 30.0]
        assert svc._perf_avg("step1") == 20.0


# ---------------------------------------------------------------------------
# Service._record_perf
# ---------------------------------------------------------------------------


class TestServiceRecordPerf:
    def test_basic(self):
        """_record_perf appends duration to the step's perf list."""
        svc = _make_service()
        svc._record_perf("resize", 1.5)
        svc._record_perf("resize", 2.5)
        assert svc._perf["resize"] == [1.5, 2.5]

    def test_caps_at_20(self):
        """_record_perf keeps only the last 20 samples."""
        svc = _make_service()
        for i in range(25):
            svc._record_perf("step", float(i))
        assert len(svc._perf["step"]) == 20
        assert svc._perf["step"][0] == 5.0  # first 5 dropped
        assert svc._perf["step"][-1] == 24.0


# ---------------------------------------------------------------------------
# _Capture.write
# ---------------------------------------------------------------------------


class TestCapture:
    def test_with_job_id(self):
        """_Capture.write sends a log message with the current job_id."""
        buf = io.StringIO()
        svc = _make_service(buf)
        cap = _Capture(svc)
        cap.job_id = "job-42"
        cap.write("hello world\n")
        msgs = _read_messages(buf)
        assert len(msgs) == 1
        assert msgs[0]["type"] == "log"
        assert msgs[0]["job_id"] == "job-42"
        assert msgs[0]["text"] == "hello world\n"

    def test_without_job_id(self):
        """_Capture.write uses __setup__ when no job_id is set."""
        buf = io.StringIO()
        svc = _make_service(buf)
        cap = _Capture(svc)
        cap.job_id = None
        cap.write("setup log\n")
        msgs = _read_messages(buf)
        assert msgs[0]["job_id"] == "__setup__"

    def test_empty_text_ignored(self):
        """_Capture.write ignores empty or whitespace-only text."""
        buf = io.StringIO()
        svc = _make_service(buf)
        cap = _Capture(svc)
        cap.write("")
        cap.write("   ")
        cap.write("\n")
        msgs = _read_messages(buf)
        assert len(msgs) == 0

    def test_appends_newline(self):
        """_Capture.write appends a newline if missing."""
        buf = io.StringIO()
        svc = _make_service(buf)
        cap = _Capture(svc)
        cap.job_id = "j1"
        cap.write("no newline")
        msgs = _read_messages(buf)
        assert msgs[0]["text"] == "no newline\n"

    def test_flush_is_noop(self):
        """_Capture.flush does nothing and does not raise."""
        buf = io.StringIO()
        svc = _make_service(buf)
        cap = _Capture(svc)
        cap.flush()  # should not raise


# ---------------------------------------------------------------------------
# Service.run() — full protocol via subprocess
# ---------------------------------------------------------------------------


class TestServiceRun:
    """Test Service.run() by spawning a real subprocess that runs a tiny service."""

    def _run_service_subprocess(self, service_code, input_msgs, timeout=10):
        """Spawn a subprocess running the given service code.

        Sends input_msgs (list of dicts) as JSON lines to stdin,
        returns all JSON messages received on stdout.
        """
        import subprocess
        import sys
        import textwrap

        # Build a script that defines the service and calls .run()
        script = textwrap.dedent(service_code)

        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Collect lines until we see "ready", then send jobs
            messages = []
            import selectors
            sel = selectors.DefaultSelector()
            sel.register(proc.stdout, selectors.EVENT_READ)

            deadline = time.monotonic() + timeout
            ready = False
            input_iter = iter(input_msgs)
            all_sent = False

            while time.monotonic() < deadline:
                events = sel.select(timeout=max(0.1, deadline - time.monotonic()))
                for _key, _ in events:
                    line = proc.stdout.readline()
                    if not line:
                        sel.unregister(proc.stdout)
                        # Process closed stdout
                        proc.wait(timeout=2)
                        return messages
                    line = line.strip()
                    if line:
                        msg = json.loads(line)
                        messages.append(msg)

                        if msg.get("type") == "ready" and not ready:
                            ready = True
                            # Send all input messages
                            for m in input_iter:
                                proc.stdin.write(json.dumps(m) + "\n")
                                proc.stdin.flush()
                            all_sent = True

                        if all_sent and msg.get("type") in ("done", "error"):
                            # Got terminal response, close stdin to exit
                            proc.stdin.close()

                if not events and proc.poll() is not None:
                    break

            sel.close()
            return messages
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)

    def test_ready_after_setup(self, tmp_path):
        """Service sends 'ready' after setup() completes."""
        code = f"""\
import sys
sys.path.insert(0, {str(tmp_path)!r})
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        pass

svc = TestSvc()
svc.run()
"""
        msgs = self._run_service_subprocess(code, [])
        types = [m["type"] for m in msgs]
        assert "ready" in types

    def test_processes_job_and_sends_done(self, tmp_path):
        """Service processes a job and sends done with perf data."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)
        (tmp_path / "work" / "input.txt").write_bytes(b"hello")

        code = """\
import sys
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        data = job.read_file("input.txt")
        job.write_file("output.txt", data + b" world")

svc = TestSvc()
svc.run()
"""
        job_msg = {
            "type": "job",
            "job_id": "test-1",
            "workdir": workdir,
        }
        msgs = self._run_service_subprocess(code, [job_msg])

        types = [m["type"] for m in msgs]
        assert "ready" in types
        assert "done" in types

        done_msg = [m for m in msgs if m["type"] == "done"][0]
        assert done_msg["job_id"] == "test-1"
        assert "elapsed" in done_msg
        assert done_msg["elapsed"] >= 0

        # Verify the output file was written
        assert (tmp_path / "work" / "output.txt").read_bytes() == b"hello world"

    def test_handles_error_in_process(self, tmp_path):
        """Service catches exceptions in process() and sends error."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        raise ValueError("intentional failure")

svc = TestSvc()
svc.run()
"""
        job_msg = {
            "type": "job",
            "job_id": "err-1",
            "workdir": workdir,
        }
        msgs = self._run_service_subprocess(code, [job_msg])
        error_msgs = [m for m in msgs if m["type"] == "error"]
        assert len(error_msgs) == 1
        assert error_msgs[0]["job_id"] == "err-1"
        assert "intentional failure" in error_msgs[0]["error"]
        assert error_msgs[0]["elapsed"] >= 0

    def test_rejects_oversized_input_files(self, tmp_path):
        """Service rejects files exceeding max_input_bytes before process()."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)
        # Create a file larger than the max_input_bytes (set to 100 in service code)
        (tmp_path / "work" / "big.bin").write_bytes(b"x" * 200)

        code = """\
from flute.service import Service

class TestSvc(Service):
    max_input_bytes = 100

    def setup(self):
        pass
    def process(self, job):
        pass  # should not be called

svc = TestSvc()
svc.run()
"""
        job_msg = {
            "type": "job",
            "job_id": "big-1",
            "workdir": workdir,
        }
        msgs = self._run_service_subprocess(code, [job_msg])
        error_msgs = [m for m in msgs if m["type"] == "error"]
        assert len(error_msgs) == 1
        assert "exceeds limit" in error_msgs[0]["error"]

    def test_eof_on_stdin_exits(self, tmp_path):
        """Service exits cleanly when stdin is closed (EOF)."""
        import subprocess
        import sys

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        pass

svc = TestSvc()
svc.run()
"""
        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # Wait for ready
            line = proc.stdout.readline()
            msg = json.loads(line.strip())
            assert msg["type"] == "ready"

            # Close stdin to signal EOF
            proc.stdin.close()
            proc.wait(timeout=5)
            assert proc.returncode == 0
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

    def test_print_becomes_log_message(self, tmp_path):
        """print() inside process() generates protocol log messages."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        print("hello from process")

svc = TestSvc()
svc.run()
"""
        job_msg = {
            "type": "job",
            "job_id": "log-1",
            "workdir": workdir,
        }
        msgs = self._run_service_subprocess(code, [job_msg])
        log_msgs = [m for m in msgs if m["type"] == "log"]
        assert any("hello from process" in m.get("text", "") for m in log_msgs)
        # Log messages during a job should have the job's id
        for m in log_msgs:
            if "hello from process" in m.get("text", ""):
                assert m["job_id"] == "log-1"

    def test_print_during_setup_uses_setup_job_id(self, tmp_path):
        """print() during setup() uses __setup__ as the job_id."""
        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        print("loading model...")
    def process(self, job):
        pass

svc = TestSvc()
svc.run()
"""
        msgs = self._run_service_subprocess(code, [])
        log_msgs = [m for m in msgs if m["type"] == "log"]
        setup_logs = [m for m in log_msgs if "loading model" in m.get("text", "")]
        assert len(setup_logs) >= 1
        assert setup_logs[0]["job_id"] == "__setup__"

    def test_progress_message_from_process(self, tmp_path):
        """progress() calls from process() generate protocol messages."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        job.progress(0.5, "halfway")

svc = TestSvc()
svc.run()
"""
        job_msg = {
            "type": "job",
            "job_id": "prog-1",
            "workdir": workdir,
        }
        msgs = self._run_service_subprocess(code, [job_msg])
        prog_msgs = [m for m in msgs if m["type"] == "progress"]
        assert len(prog_msgs) >= 1
        assert prog_msgs[0]["value"] == 0.5
        assert prog_msgs[0]["step"] == "halfway"
        assert prog_msgs[0]["job_id"] == "prog-1"

    def test_non_job_messages_ignored(self, tmp_path):
        """Non-job messages on stdin are silently ignored."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        job.write_file("ok.txt", b"ok")

svc = TestSvc()
svc.run()
"""
        msgs_in = [
            {"type": "ping"},  # not a job, should be ignored
            {"type": "job", "job_id": "j1", "workdir": workdir},
        ]
        msgs = self._run_service_subprocess(code, msgs_in)
        done_msgs = [m for m in msgs if m["type"] == "done"]
        assert len(done_msgs) == 1
        assert done_msgs[0]["job_id"] == "j1"

    def test_invalid_json_on_stdin_ignored(self, tmp_path):
        """Invalid JSON lines on stdin are silently skipped."""
        import subprocess
        import sys

        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        pass

svc = TestSvc()
svc.run()
"""
        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # Wait for ready
            line = proc.stdout.readline()
            assert json.loads(line.strip())["type"] == "ready"

            # Send invalid JSON
            proc.stdin.write("not json at all\n")
            proc.stdin.flush()

            # Send valid job
            job = {"type": "job", "job_id": "j1", "workdir": workdir}
            proc.stdin.write(json.dumps(job) + "\n")
            proc.stdin.flush()

            # Read done
            line = proc.stdout.readline()
            msg = json.loads(line.strip())
            assert msg["type"] == "done"
            assert msg["job_id"] == "j1"

            proc.stdin.close()
            proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

    def test_done_includes_perf_data(self, tmp_path):
        """done message includes perf averages when _record_perf is used."""
        workdir = str(tmp_path / "work")
        os.makedirs(workdir)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        self._record_perf("grayscale", 1.0)
        self._record_perf("grayscale", 3.0)

svc = TestSvc()
svc.run()
"""
        job_msg = {"type": "job", "job_id": "perf-1", "workdir": workdir}
        msgs = self._run_service_subprocess(code, [job_msg])
        done_msgs = [m for m in msgs if m["type"] == "done"]
        assert len(done_msgs) == 1
        assert "perf" in done_msgs[0]
        assert done_msgs[0]["perf"]["grayscale"] == 2.0

    def test_capture_resets_job_id_after_job(self, tmp_path):
        """After a job completes, capture.job_id is reset to None (via finally block)."""
        # This is tested indirectly: print during setup uses __setup__,
        # print during job uses job_id, ensuring the finally block works.
        # We test two consecutive jobs to verify reset.
        workdir1 = str(tmp_path / "w1")
        workdir2 = str(tmp_path / "w2")
        os.makedirs(workdir1)
        os.makedirs(workdir2)

        code = """\
from flute.service import Service

class TestSvc(Service):
    def setup(self):
        pass
    def process(self, job):
        print(f"processing {job.job_id}")

svc = TestSvc()
svc.run()
"""
        import subprocess
        import sys

        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # Wait for ready
            line = proc.stdout.readline()
            assert json.loads(line.strip())["type"] == "ready"

            # Send first job
            j1 = {"type": "job", "job_id": "a", "workdir": workdir1}
            proc.stdin.write(json.dumps(j1) + "\n")
            proc.stdin.flush()

            # Read log + done for first job
            msgs = []
            while True:
                line = proc.stdout.readline().strip()
                msg = json.loads(line)
                msgs.append(msg)
                if msg["type"] == "done":
                    break

            log_a = [m for m in msgs if m["type"] == "log"]
            assert log_a[0]["job_id"] == "a"

            # Send second job
            j2 = {"type": "job", "job_id": "b", "workdir": workdir2}
            proc.stdin.write(json.dumps(j2) + "\n")
            proc.stdin.flush()

            msgs2 = []
            while True:
                line = proc.stdout.readline().strip()
                msg = json.loads(line)
                msgs2.append(msg)
                if msg["type"] == "done":
                    break

            log_b = [m for m in msgs2 if m["type"] == "log"]
            assert log_b[0]["job_id"] == "b"

            proc.stdin.close()
            proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Service.__init__ and class attributes
# ---------------------------------------------------------------------------


class TestServiceInit:
    def test_default_operations(self):
        """Default Service has empty operations list."""
        svc = DummyService()
        assert svc.operations == []

    def test_default_max_input_bytes(self):
        """Default max_input_bytes is 50MB."""
        svc = DummyService()
        assert svc.max_input_bytes == 50 * 1024 * 1024

    def test_perf_dict_empty(self):
        """New service has empty perf dict."""
        svc = DummyService()
        assert svc._perf == {}

    def test_process_not_implemented(self):
        """Base Service.process() raises NotImplementedError."""
        svc = Service()
        job = _make_job(svc=svc)
        with pytest.raises(NotImplementedError):
            svc.process(job)


# ---------------------------------------------------------------------------
# Service._send
# ---------------------------------------------------------------------------


class TestServiceSend:
    def test_sends_json_line(self):
        """_send writes a JSON line to proto_out."""
        buf = io.StringIO()
        svc = _make_service(buf)
        svc._send({"type": "test", "data": 42})
        buf.seek(0)
        line = buf.readline()
        msg = json.loads(line)
        assert msg == {"type": "test", "data": 42}

    def test_sends_newline_terminated(self):
        """_send output ends with a newline."""
        buf = io.StringIO()
        svc = _make_service(buf)
        svc._send({"type": "ready"})
        buf.seek(0)
        raw = buf.read()
        assert raw.endswith("\n")


# ---------------------------------------------------------------------------
# Service.run() — in-process tests (for coverage of lines 196-262)
# ---------------------------------------------------------------------------


class _FakeStdin:
    """Simulates stdin with queued lines, supporting readline()."""

    def __init__(self, lines):
        self._lines = list(lines)
        self._pos = 0

    def readline(self):
        if self._pos < len(self._lines):
            line = self._lines[self._pos]
            self._pos += 1
            return line
        return ""


class TestServiceRunInProcess:
    """Test Service.run() in-process with mocked stdin/stdout for coverage."""

    def _run(self, svc, stdin_lines):
        """Run a service in-process with given stdin lines, return protocol messages."""
        proto_out = io.StringIO()
        fake_stdin = _FakeStdin(stdin_lines)
        import sys
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdout = proto_out
        sys.stdin = fake_stdin
        try:
            svc.run()
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout
        return _read_messages(proto_out)

    def test_ready_on_empty_stdin(self):
        """run() sends ready and exits when stdin is empty."""
        svc = DummyService()
        msgs = self._run(svc, [])
        assert len(msgs) == 1
        assert msgs[0]["type"] == "ready"

    def test_processes_job_and_sends_done(self, tmp_path):
        """run() processes a job and sends done."""
        workdir = str(tmp_path)
        (tmp_path / "input.txt").write_bytes(b"hello")

        class WriteService(Service):
            def setup(self): pass
            def process(self, job):
                data = job.read_file("input.txt")
                job.write_file("output.txt", data + b" world")

        job_msg = json.dumps({
            "type": "job", "job_id": "j1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(WriteService(), [job_msg])
        types = [m["type"] for m in msgs]
        assert "ready" in types
        assert "done" in types
        done = [m for m in msgs if m["type"] == "done"][0]
        assert done["job_id"] == "j1"
        assert done["elapsed"] >= 0
        assert (tmp_path / "output.txt").read_bytes() == b"hello world"

    def test_error_in_process(self, tmp_path):
        """run() catches exceptions in process() and sends error."""
        workdir = str(tmp_path)

        class FailService(Service):
            def setup(self): pass
            def process(self, job):
                raise ValueError("boom")

        job_msg = json.dumps({
            "type": "job", "job_id": "e1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(FailService(), [job_msg])
        errors = [m for m in msgs if m["type"] == "error"]
        assert len(errors) == 1
        assert errors[0]["job_id"] == "e1"
        assert "boom" in errors[0]["error"]
        assert errors[0]["elapsed"] >= 0

    def test_oversized_input_rejected(self, tmp_path):
        """run() rejects files exceeding max_input_bytes before process()."""
        workdir = str(tmp_path)
        (tmp_path / "big.bin").write_bytes(b"x" * 500)

        class TinyService(Service):
            max_input_bytes = 100
            def setup(self): pass
            def process(self, job):
                pass  # should not be called

        job_msg = json.dumps({
            "type": "job", "job_id": "big1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(TinyService(), [job_msg])
        errors = [m for m in msgs if m["type"] == "error"]
        assert len(errors) == 1
        assert "exceeds limit" in errors[0]["error"]

    def test_invalid_json_skipped(self, tmp_path):
        """run() skips invalid JSON lines on stdin."""
        workdir = str(tmp_path)

        job_msg = json.dumps({
            "type": "job", "job_id": "j1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(DummyService(), ["not json\n", job_msg])
        types = [m["type"] for m in msgs]
        assert "ready" in types
        assert "done" in types

    def test_non_job_message_skipped(self, tmp_path):
        """run() skips non-job type messages."""
        workdir = str(tmp_path)

        job_msg = json.dumps({
            "type": "job", "job_id": "j1", "workdir": workdir,
        }) + "\n"
        ping = json.dumps({"type": "ping"}) + "\n"
        msgs = self._run(DummyService(), [ping, job_msg])
        done_msgs = [m for m in msgs if m["type"] == "done"]
        assert len(done_msgs) == 1
        assert done_msgs[0]["job_id"] == "j1"

    def test_print_becomes_log(self, tmp_path):
        """print() in process() generates log messages with correct job_id."""
        workdir = str(tmp_path)

        class PrintService(Service):
            def setup(self): pass
            def process(self, job):
                print("hello from job")

        job_msg = json.dumps({
            "type": "job", "job_id": "log1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(PrintService(), [job_msg])
        logs = [m for m in msgs if m["type"] == "log"]
        assert any("hello from job" in m["text"] for m in logs)
        for m in logs:
            if "hello from job" in m["text"]:
                assert m["job_id"] == "log1"

    def test_print_during_setup(self):
        """print() during setup() uses __setup__ as job_id."""
        class SetupPrintService(Service):
            def setup(self):
                print("loading model")
            def process(self, job):
                pass

        msgs = self._run(SetupPrintService(), [])
        logs = [m for m in msgs if m["type"] == "log"]
        assert any("loading model" in m["text"] for m in logs)
        for m in logs:
            if "loading model" in m["text"]:
                assert m["job_id"] == "__setup__"

    def test_capture_reset_after_job(self, tmp_path):
        """After a job, capture.job_id is reset (finally block)."""
        w1 = str(tmp_path / "w1")
        w2 = str(tmp_path / "w2")
        os.makedirs(w1)
        os.makedirs(w2)

        class PrintService(Service):
            def setup(self): pass
            def process(self, job):
                print(f"job={job.job_id}")

        j1 = json.dumps({"type": "job", "job_id": "a", "workdir": w1}) + "\n"
        j2 = json.dumps({"type": "job", "job_id": "b", "workdir": w2}) + "\n"
        msgs = self._run(PrintService(), [j1, j2])
        logs_a = [m for m in msgs if m["type"] == "log" and "job=a" in m.get("text", "")]
        logs_b = [m for m in msgs if m["type"] == "log" and "job=b" in m.get("text", "")]
        assert logs_a[0]["job_id"] == "a"
        assert logs_b[0]["job_id"] == "b"

    def test_done_includes_perf(self, tmp_path):
        """done message includes perf averages."""
        workdir = str(tmp_path)

        class PerfService(Service):
            def setup(self): pass
            def process(self, job):
                self._record_perf("step1", 2.0)
                self._record_perf("step1", 4.0)

        job_msg = json.dumps({
            "type": "job", "job_id": "p1", "workdir": workdir,
        }) + "\n"
        msgs = self._run(PerfService(), [job_msg])
        done = [m for m in msgs if m["type"] == "done"][0]
        assert done["perf"]["step1"] == 3.0

    def test_eof_error_exits_gracefully(self):
        """run() exits cleanly when stdin raises EOFError."""
        class EOFStdin:
            def readline(self):
                raise EOFError()

        proto_out = io.StringIO()
        import sys
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdout = proto_out
        sys.stdin = EOFStdin()
        try:
            DummyService().run()
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout
        msgs = _read_messages(proto_out)
        assert len(msgs) == 1
        assert msgs[0]["type"] == "ready"

    def test_job_params_and_steps(self, tmp_path):
        """Job receives params and steps from the message."""
        workdir = str(tmp_path)
        received = {}

        class ParamService(Service):
            def setup(self): pass
            def process(self, job):
                received["params"] = job.params
                received["steps"] = job.steps
                received["quota"] = job.quota_budget
                received["max_rt"] = job.max_runtime

        job_msg = json.dumps({
            "type": "job", "job_id": "p1", "workdir": workdir,
            "params": {"key": "val"}, "steps": ["a", "b"],
            "quota_budget_seconds": 100, "max_runtime_seconds": 15,
        }) + "\n"
        self._run(ParamService(), [job_msg])
        assert received["params"] == {"key": "val"}
        assert received["steps"] == ["a", "b"]
        assert received["quota"] == 100
        assert received["max_rt"] == 15
