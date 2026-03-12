"""Unit tests for flute.handlers."""

import asyncio
import json
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import psutil
import pytest
from conftest import FakeRedis
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
    register_handler,
)

# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_python_registered(self):
        assert "python" in HANDLER_REGISTRY
        assert HANDLER_REGISTRY["python"] is PythonHandler

    def test_register_custom_handler(self):
        @register_handler
        class CustomHandler(WorkerHandler):
            worker_type = "custom_test"

        assert "custom_test" in HANDLER_REGISTRY
        assert HANDLER_REGISTRY["custom_test"] is CustomHandler
        # Cleanup
        del HANDLER_REGISTRY["custom_test"]

    @pytest.mark.asyncio
    async def test_worker_handler_not_implemented(self):
        h = WorkerHandler()
        with pytest.raises(NotImplementedError):
            await h.execute({}, "/tmp", None, "s1", "ch")


# ---------------------------------------------------------------------------
# HandlerResult
# ---------------------------------------------------------------------------


class TestHandlerResult:
    def test_defaults(self):
        r = HandlerResult()
        assert r.status == "completed"
        assert r.exit_code is None
        assert r.logs == ""
        assert r.error == ""
        assert r.response is None

    def test_custom(self):
        r = HandlerResult(status="error", exit_code=1, logs="fail\n", error="boom")
        assert r.status == "error"
        assert r.exit_code == 1
        assert r.logs == "fail\n"
        assert r.error == "boom"

    def test_with_response(self):
        r = HandlerResult(status="completed", exit_code=0, response='{"result": "ok"}')
        assert r.response == '{"result": "ok"}'


# ---------------------------------------------------------------------------
# _dir_size_mb
# ---------------------------------------------------------------------------


class TestDirSizeMb:
    def test_empty(self, tmp_path):
        assert _dir_size_mb(str(tmp_path)) == 0.0

    def test_with_files(self, tmp_path):
        (tmp_path / "a.bin").write_bytes(b"x" * 1024)
        (tmp_path / "b.bin").write_bytes(b"x" * 2048)
        result = _dir_size_mb(str(tmp_path))
        assert abs(result - 3072 / (1024 * 1024)) < 0.001

    def test_nested(self, tmp_path):
        sub = tmp_path / "sub" / "deep"
        sub.mkdir(parents=True)
        (sub / "file.bin").write_bytes(b"x" * 4096)
        result = _dir_size_mb(str(tmp_path))
        assert abs(result - 4096 / (1024 * 1024)) < 0.001

    def test_handles_os_error(self, tmp_path):
        (tmp_path / "gone.bin").write_bytes(b"x" * 100)
        with patch("os.path.getsize", side_effect=OSError("gone")):
            assert _dir_size_mb(str(tmp_path)) == 0.0


# ---------------------------------------------------------------------------
# _kill_tree
# ---------------------------------------------------------------------------


class TestKillTree:
    def test_kills_parent_and_children(self):
        child1, child2 = MagicMock(), MagicMock()
        parent = MagicMock()
        parent.children.return_value = [child1, child2]
        with patch("flute.handlers.psutil.Process", return_value=parent):
            _kill_tree(12345)
        child1.kill.assert_called_once()
        child2.kill.assert_called_once()
        parent.kill.assert_called_once()

    def test_no_such_process(self):
        with patch(
            "flute.handlers.psutil.Process", side_effect=psutil.NoSuchProcess(12345)
        ):
            _kill_tree(12345)  # should not raise


# ---------------------------------------------------------------------------
# _signal_name
# ---------------------------------------------------------------------------


class TestSignalName:
    def test_known(self):
        assert _signal_name(9) == "SIGKILL"
        assert _signal_name(15) == "SIGTERM"

    def test_unknown(self):
        assert _signal_name(999) == "999"


# ---------------------------------------------------------------------------
# _make_preexec
# ---------------------------------------------------------------------------


class TestMakePreexec:
    def test_returns_callable(self):
        assert callable(_make_preexec(50))

    def test_sets_fsize_limit(self):
        import resource

        fn = _make_preexec(10)
        with patch("resource.setrlimit") as mock:
            fn()
        expected = 10 * 1024 * 1024
        mock.assert_any_call(resource.RLIMIT_FSIZE, (expected, expected))

    def test_no_nproc_limit(self):
        """RLIMIT_NPROC is NOT set — OpenBLAS/scikit-learn need threads."""
        import resource

        fn = _make_preexec(10)
        with patch("resource.setrlimit") as mock:
            fn()
        nproc_calls = [c for c in mock.call_args_list if c[0][0] == resource.RLIMIT_NPROC]
        assert nproc_calls == []

    def test_handles_os_error(self):
        fn = _make_preexec(10)
        with patch("resource.setrlimit", side_effect=OSError("unsupported")):
            fn()  # should not raise

    def test_handles_value_error(self):
        fn = _make_preexec(10)
        with patch("resource.setrlimit", side_effect=ValueError("bad")):
            fn()  # should not raise


# ---------------------------------------------------------------------------
# _stream_output
# ---------------------------------------------------------------------------


class TestStreamOutput:
    @pytest.mark.asyncio
    async def test_writes_to_log_and_publishes(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_data(b"line1\nline2\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()
        channel = "test:stream"

        await _stream_output(reader, log_file, r, channel)
        log_file.close()

        assert (tmp_path / "test.log").read_text() == "line1\nline2\n"
        assert r.published[channel] == ["line1\n", "line2\n", ""]

    @pytest.mark.asyncio
    async def test_handles_publish_failure(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_data(b"ok\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()

        async def failing_publish(channel, message):
            raise RuntimeError("redis down")

        r.publish = failing_publish

        await _stream_output(reader, log_file, r, "ch")
        log_file.close()

        assert (tmp_path / "test.log").read_text() == "ok\n"

    @pytest.mark.asyncio
    async def test_buffers_log_lines_in_redis(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_data(b"a\nb\nc\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()
        key = "session:buf1:logs:lines"

        await _stream_output(reader, log_file, r, "ch", log_lines_key=key, max_log_lines=10)
        log_file.close()

        lines = await r.lrange(key, 0, -1)
        assert lines == ["a\n", "b\n", "c\n"]

    @pytest.mark.asyncio
    async def test_log_lines_trimmed_to_max(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_data(b"1\n2\n3\n4\n5\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()
        key = "session:buf2:logs:lines"

        await _stream_output(reader, log_file, r, "ch", log_lines_key=key, max_log_lines=3)
        log_file.close()

        lines = await r.lrange(key, 0, -1)
        assert lines == ["3\n", "4\n", "5\n"]

    @pytest.mark.asyncio
    async def test_no_buffering_when_disabled(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_data(b"x\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()

        await _stream_output(reader, log_file, r, "ch")
        log_file.close()

        assert "session" not in str(r.lists)  # No list keys created

    @pytest.mark.asyncio
    async def test_log_lines_rpush_failure(self, tmp_path):
        """Failures in log line buffering are suppressed."""
        reader = asyncio.StreamReader()
        reader.feed_data(b"ok\n")
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()

        async def failing_rpush(key, val):
            raise RuntimeError("redis down")

        r.rpush = failing_rpush

        await _stream_output(
            reader, log_file, r, "ch",
            log_lines_key="session:fail:logs:lines", max_log_lines=100,
        )
        log_file.close()

        assert (tmp_path / "test.log").read_text() == "ok\n"

    @pytest.mark.asyncio
    async def test_empty_pipe(self, tmp_path):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()

        await _stream_output(reader, log_file, r, "ch")
        log_file.close()

        assert (tmp_path / "test.log").read_text() == ""
        assert r.published["ch"] == [""]  # EOF sentinel only


# ---------------------------------------------------------------------------
# PythonHandler
# ---------------------------------------------------------------------------


class TestPythonHandler:
    @pytest.mark.asyncio
    async def test_basic_completed(self):
        r = FakeRedis()
        h = PythonHandler()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"code": "print('hello')", "max_runtime_seconds": 10},
                workdir, r, "ph1", "ch:ph1",
            )
            assert result.status == "completed"
            assert result.exit_code == 0
            assert "hello" in result.logs
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_exit(self):
        r = FakeRedis()
        h = PythonHandler()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"code": "raise ValueError('boom')", "max_runtime_seconds": 10},
                workdir, r, "ph2", "ch:ph2",
            )
            assert result.status == "error"
            assert "boom" in result.logs
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_runtime_exceeded(self):
        r = FakeRedis()
        h = PythonHandler()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {
                    "code": "import time; time.sleep(60)",
                    "max_runtime_seconds": 1,
                    "max_memory_mb": 128,
                    "max_disk_mb": 50,
                },
                workdir, r, "ph3", "ch:ph3",
            )
            assert result.status == "killed"
            assert "runtime" in result.error.lower()
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_buffers_log_lines(self):
        r = FakeRedis()
        h = PythonHandler()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {
                    "code": "for i in range(3): print(f'line{i}')",
                    "max_runtime_seconds": 10,
                    "max_log_lines": 100,
                },
                workdir, r, "ph-buf", "ch:ph-buf",
            )
            assert result.status == "completed"
            lines = await r.lrange("session:ph-buf:logs:lines", 0, -1)
            assert len(lines) == 3
            assert "line0" in lines[0]
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    def test_custom_worker_type(self):
        h = PythonHandler(worker_type="my-python")
        assert h.worker_type == "my-python"


    @pytest.mark.asyncio
    async def test_cancel_during_execution(self, monkeypatch):
        import shutil
        import tempfile

        import flute.handlers as _hmod

        r = FakeRedis()
        h = PythonHandler()
        workdir = tempfile.mkdtemp()
        # Set cancel key so it's found on first check
        await r.set("session:phc:cancel", "1")
        # Speed up: make cancel check fire immediately (0s interval)
        monkeypatch.setattr(_hmod, "CANCEL_CHECK_INTERVAL", 0)
        try:
            result = await h.execute(
                {
                    "code": "import time; time.sleep(60)",
                    "max_runtime_seconds": 30,
                    "max_memory_mb": 128,
                    "max_disk_mb": 50,
                },
                workdir, r, "phc", "ch:phc",
            )
            assert result.status == "cancelled"
            assert "cancelled by user" in result.error
        finally:
            shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# TaskRunnerHandler
# ---------------------------------------------------------------------------


class TestTaskRunnerHandler:
    def _make_handler(self):
        """Create a TaskRunnerHandler with a simple test TaskHandler."""
        from pydantic import BaseModel

        class EchoTask(BaseModel):
            text: str

        class EchoResponse(BaseModel):
            echoed: str

        class EchoHandler:
            task_model = EchoTask
            response_model = EchoResponse

            async def execute(self, task, input_files):
                return EchoResponse(echoed=task.text), {}

        return TaskRunnerHandler(worker_type="echo", task_handler=EchoHandler())

    @pytest.mark.asyncio
    async def test_basic_task(self):
        h = self._make_handler()
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"task": '{"text": "hello"}', "max_runtime_seconds": 10},
                workdir, r, "tr1", "ch:tr1",
            )
            assert result.status == "completed"
            assert result.exit_code == 0
            assert result.response is not None
            import json
            resp = json.loads(result.response)
            assert resp["echoed"] == "hello"
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_invalid_task_json(self):
        h = self._make_handler()
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"task": "not valid json", "max_runtime_seconds": 10},
                workdir, r, "tr2", "ch:tr2",
            )
            assert result.status == "error"
            assert result.exit_code == 1
            assert result.error
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_timeout(self):
        from pydantic import BaseModel

        class SlowTask(BaseModel):
            duration: float

        class SlowResponse(BaseModel):
            done: bool

        class SlowHandler:
            task_model = SlowTask
            response_model = SlowResponse

            async def execute(self, task, input_files):
                await asyncio.sleep(task.duration)
                return SlowResponse(done=True), {}

        h = TaskRunnerHandler(worker_type="slow", task_handler=SlowHandler())
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"task": '{"duration": 60}', "max_runtime_seconds": 1},
                workdir, r, "tr3", "ch:tr3",
            )
            assert result.status == "killed"
            assert "runtime" in result.error.lower()
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_with_input_files(self):
        from pydantic import BaseModel

        class FileTask(BaseModel):
            filename: str

        class FileResponse(BaseModel):
            size: int

        class FileHandler:
            task_model = FileTask
            response_model = FileResponse

            async def execute(self, task, input_files):
                data = input_files.get(task.filename, b"")
                return FileResponse(size=len(data)), {}

        h = TaskRunnerHandler(worker_type="file-test", task_handler=FileHandler())
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            # Write input file to workdir (as run_session would)
            with open(os.path.join(workdir, "data.bin"), "wb") as f:
                f.write(b"hello world")

            result = await h.execute(
                {
                    "task": '{"filename": "data.bin"}',
                    "input_files": {"data.bin": "aGVsbG8gd29ybGQ="},
                    "max_runtime_seconds": 10,
                },
                workdir, r, "tr4", "ch:tr4",
            )
            assert result.status == "completed"
            import json
            resp = json.loads(result.response)
            assert resp["size"] == 11
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_output_files(self):
        from pydantic import BaseModel

        class GenTask(BaseModel):
            content: str

        class GenResponse(BaseModel):
            filename: str

        class GenHandler:
            task_model = GenTask
            response_model = GenResponse

            async def execute(self, task, input_files):
                return GenResponse(filename="out.txt"), {"out.txt": task.content.encode()}

        h = TaskRunnerHandler(worker_type="gen-test", task_handler=GenHandler())
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"task": '{"content": "generated"}', "max_runtime_seconds": 10},
                workdir, r, "tr5", "ch:tr5",
            )
            assert result.status == "completed"
            # Output file should be written to workdir
            with open(os.path.join(workdir, "out.txt"), "rb") as f:
                assert f.read() == b"generated"
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_handler_exception(self):
        from pydantic import BaseModel

        class FailTask(BaseModel):
            pass

        class FailResponse(BaseModel):
            pass

        class FailHandler:
            task_model = FailTask
            response_model = FailResponse

            async def execute(self, task, input_files):
                raise RuntimeError("handler exploded")

        h = TaskRunnerHandler(worker_type="fail-test", task_handler=FailHandler())
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {"task": "{}", "max_runtime_seconds": 10},
                workdir, r, "tr6", "ch:tr6",
            )
            assert result.status == "error"
            assert "handler exploded" in result.error
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

    def test_worker_type(self):
        h = self._make_handler()
        assert h.worker_type == "echo"

    @pytest.mark.asyncio
    async def test_missing_input_file(self):
        """Input files listed in spec but not present on disk are handled."""
        from pydantic import BaseModel

        class ReadTask(BaseModel):
            pass

        class ReadResponse(BaseModel):
            files: list[str]

        class ReadHandler:
            task_model = ReadTask
            response_model = ReadResponse

            async def execute(self, task, input_files):
                return ReadResponse(files=list(input_files.keys())), {}

        h = TaskRunnerHandler(worker_type="read-test", task_handler=ReadHandler())
        r = FakeRedis()
        import tempfile

        workdir = tempfile.mkdtemp()
        try:
            result = await h.execute(
                {
                    "task": "{}",
                    "input_files": {"missing.txt": "data"},
                    "max_runtime_seconds": 10,
                },
                workdir, r, "tr7", "ch:tr7",
            )
            assert result.status == "completed"
            import json
            resp = json.loads(result.response)
            assert resp["files"] == []
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# ServiceHandler — long-lived subprocess management
# ---------------------------------------------------------------------------

# A minimal Service that echoes input files to output files
_ECHO_SERVICE_CODE = """\
from flute.service import Service

class EchoService(Service):
    def setup(self):
        pass  # instant setup, no model loading

    def process(self, job):
        for name in job.input_files():
            data = job.read_file(name)
            job.write_file(f"out_{name}", data)
        job.progress(1.0, "done")
"""

# A service that raises an error during process()
_ERROR_SERVICE_CODE = """\
from flute.service import Service

class ErrorService(Service):
    def setup(self):
        pass

    def process(self, job):
        raise RuntimeError("service exploded")
"""

# A service that sleeps forever (for timeout tests)
_SLOW_SERVICE_CODE = """\
import time
from flute.service import Service

class SlowService(Service):
    def setup(self):
        pass

    def process(self, job):
        time.sleep(300)
"""

# A service that prints log messages
_LOGGING_SERVICE_CODE = """\
from flute.service import Service

class LoggingService(Service):
    def setup(self):
        pass

    def process(self, job):
        print("log line 1")
        print("log line 2")
"""

# A service that crashes immediately during startup
_CRASH_STARTUP_CODE = """\
import sys
sys.exit(1)
"""

# A service that crashes mid-job (after ready)
_CRASH_DURING_JOB_CODE = """\
import json, sys, os
from flute.service import Service

class CrashService(Service):
    def setup(self):
        pass

    def process(self, job):
        os._exit(1)  # hard crash
"""


def _write_service_file(tmp_path, filename, code):
    """Write a service module to tmp_path and return the module name (without .py)."""
    (tmp_path / filename).write_text(code)
    return filename.replace(".py", "")


def _make_service_handler(tmp_path, code, module_file="service.py", class_name=None):
    """Create a ServiceHandler from service code written to tmp_path."""
    module_name = _write_service_file(tmp_path, module_file, code)
    # Infer class name from code if not provided
    if class_name is None:
        for line in code.splitlines():
            if line.strip().startswith("class ") and "(Service)" in line:
                class_name = line.strip().split("class ")[1].split("(")[0]
                break
    return ServiceHandler(
        worker_type="test-svc",
        worker_dir=str(tmp_path),
        module_name=module_name,
        class_name=class_name,
    )


class TestServiceHandler:
    """Tests for ServiceHandler — manages a long-lived service subprocess."""

    @pytest.mark.asyncio
    async def test_start_service_sends_ready(self, tmp_path):
        """_start_service starts subprocess and waits for ready signal."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        try:
            await handler._start_service()
            assert handler._proc is not None
            assert handler._proc.returncode is None  # still running
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()

    @pytest.mark.asyncio
    async def test_execute_echo_service(self, tmp_path):
        """execute sends a job and returns HandlerResult with 'completed' status."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            # Write an input file
            with open(os.path.join(workdir, "hello.txt"), "wb") as f:
                f.write(b"hello world")

            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-1", "ch:svc-1",
            )
            assert result.status == "completed"
            assert result.exit_code == 0

            # Verify output file was created by the service
            out_path = os.path.join(workdir, "out_hello.txt")
            assert os.path.exists(out_path)
            with open(out_path, "rb") as f:
                assert f.read() == b"hello world"
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_handles_error_response(self, tmp_path):
        """execute returns error HandlerResult when service raises an exception."""
        handler = _make_service_handler(tmp_path, _ERROR_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-err", "ch:svc-err",
            )
            assert result.status == "error"
            assert result.exit_code == 1
            assert "service exploded" in result.error
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_timeout(self, tmp_path):
        """execute returns killed HandlerResult when service exceeds max_runtime."""
        handler = _make_service_handler(tmp_path, _SLOW_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 1},
                workdir, r, "svc-timeout", "ch:svc-timeout",
            )
            assert result.status == "killed"
            assert "runtime" in result.error.lower()
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_process_crash_during_job(self, tmp_path):
        """execute handles a service process that crashes mid-job."""
        handler = _make_service_handler(tmp_path, _CRASH_DURING_JOB_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-crash", "ch:svc-crash",
            )
            assert result.status == "error"
            assert "crash" in result.error.lower() or "closed" in result.error.lower()
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_progress_forwarded_to_redis(self, tmp_path):
        """Progress messages from the service are published to Redis."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            # Write a file so the echo service has something to process
            with open(os.path.join(workdir, "in.txt"), "wb") as f:
                f.write(b"data")

            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-prog", "ch:svc-prog",
            )
            assert result.status == "completed"

            # Check that progress was published to the channel
            published = r.published.get("ch:svc-prog", [])
            progress_msgs = [p for p in published if "__progress__" in str(p)]
            assert len(progress_msgs) >= 1

            # Check that progress was stored in Redis
            progress_json = await r.get("session:svc-prog:progress")
            assert progress_json is not None
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_log_messages_forwarded(self, tmp_path):
        """Log messages from print() in the service are forwarded to Redis."""
        handler = _make_service_handler(tmp_path, _LOGGING_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-log", "ch:svc-log",
            )
            assert result.status == "completed"
            assert "log line 1" in result.logs
            assert "log line 2" in result.logs

            # Check logs were published
            published = r.published.get("ch:svc-log", [])
            log_texts = [p for p in published if "log line" in str(p)]
            assert len(log_texts) >= 2
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_log_lines_buffered_in_redis(self, tmp_path):
        """Log lines are buffered in a Redis list when max_log_lines > 0."""
        handler = _make_service_handler(tmp_path, _LOGGING_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10, "max_log_lines": 100},
                workdir, r, "svc-buf", "ch:svc-buf",
            )
            assert result.status == "completed"

            lines = await r.lrange("session:svc-buf:logs:lines", 0, -1)
            assert len(lines) >= 2
            assert any("log line 1" in line for line in lines)
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_ensure_service_reuses_running_process(self, tmp_path):
        """_ensure_service reuses the existing process if still running."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        try:
            await handler._start_service()
            first_proc = handler._proc
            first_pid = first_proc.pid

            # Call _ensure_service — should NOT start a new process
            await handler._ensure_service()
            assert handler._proc is first_proc
            assert handler._proc.pid == first_pid
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()

    @pytest.mark.asyncio
    async def test_ensure_service_restarts_after_crash(self, tmp_path):
        """_ensure_service restarts the service if the previous process died."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        try:
            await handler._start_service()
            first_pid = handler._proc.pid

            # Kill the process to simulate a crash
            handler._proc.kill()
            await handler._proc.wait()
            assert handler._proc.returncode is not None

            # _ensure_service should start a new process
            await handler._ensure_service()
            assert handler._proc.returncode is None
            assert handler._proc.pid != first_pid
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()

    @pytest.mark.asyncio
    async def test_execute_broken_pipe_handled(self, tmp_path):
        """execute handles BrokenPipeError when the service dies before we send a job."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            await handler._start_service()

            # Kill the process to cause a broken pipe on write
            handler._proc.kill()
            await handler._proc.wait()
            # Bypass _ensure_service by keeping returncode set but faking it
            # Actually, _ensure_service will try to restart; we need to
            # simulate a crash after _ensure_service succeeds but before
            # stdin.write. Patch stdin.write to raise BrokenPipeError.
            await handler._ensure_service()  # restarts the process

            # Now patch stdin.write to simulate broken pipe
            def broken_write(data):
                raise BrokenPipeError("pipe broken")

            handler._proc.stdin.write = broken_write

            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-pipe", "ch:svc-pipe",
            )
            assert result.status == "error"
            assert "broken pipe" in result.error.lower()
            assert handler._proc is None  # process reference cleared
        finally:
            # handler._proc may be None after broken pipe
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_start_service_failure(self, tmp_path):
        """_start_service raises RuntimeError when the process dies during startup."""
        (tmp_path / "bad.py").write_text(_CRASH_STARTUP_CODE)
        handler = ServiceHandler(
            worker_type="test-bad",
            worker_dir=str(tmp_path),
            module_name="bad",
            class_name="NeverUsed",
        )
        with pytest.raises(RuntimeError, match="closed stdout during startup"):
            await handler._start_service()

    @pytest.mark.asyncio
    async def test_execute_service_start_failure_returns_error(self, tmp_path):
        """execute returns error HandlerResult when service fails to start."""
        (tmp_path / "bad.py").write_text(_CRASH_STARTUP_CODE)
        handler = ServiceHandler(
            worker_type="test-bad",
            worker_dir=str(tmp_path),
            module_name="bad",
            class_name="NeverUsed",
        )
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-bad", "ch:svc-bad",
            )
            assert result.status == "error"
            assert "failed to start" in result.error.lower()
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_execute_serialized_by_lock(self, tmp_path):
        """Only one job runs at a time (serialized by asyncio lock)."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        r = FakeRedis()
        workdir1 = tempfile.mkdtemp()
        workdir2 = tempfile.mkdtemp()
        try:
            # Run two jobs concurrently — they should be serialized
            async def run_job(workdir, sid):
                return await handler.execute(
                    {"max_runtime_seconds": 10},
                    workdir, r, sid, f"ch:{sid}",
                )

            r1, r2 = await asyncio.gather(
                run_job(workdir1, "svc-s1"),
                run_job(workdir2, "svc-s2"),
            )
            assert r1.status == "completed"
            assert r2.status == "completed"
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir1, ignore_errors=True)
            shutil.rmtree(workdir2, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_perf_data_stored_in_redis(self, tmp_path):
        """done message with perf data stores it in Redis under service key."""
        code = """\
from flute.service import Service

class PerfService(Service):
    def setup(self):
        pass

    def process(self, job):
        self._record_perf("resize", 1.5)
        self._record_perf("resize", 2.5)
"""
        handler = _make_service_handler(tmp_path, code, class_name="PerfService")
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-perf", "ch:svc-perf",
            )
            assert result.status == "completed"

            perf_json = await r.get("service:test-svc:perf")
            assert perf_json is not None
            import json
            perf = json.loads(perf_json)
            assert "resize" in perf
            assert perf["resize"] == 2.0
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    def test_worker_type(self, tmp_path):
        """ServiceHandler stores the worker_type."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        assert handler.worker_type == "test-svc"

    @pytest.mark.asyncio
    async def test_drain_stderr_handles_exception(self, tmp_path):
        """_drain_stderr catches exceptions from stderr.readline()."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)

        async def broken_readline():
            raise RuntimeError("stderr broken")

        mock_proc = MagicMock()
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readline = broken_readline
        handler._proc = mock_proc

        # Should complete without propagating the exception
        await handler._drain_stderr()

    @pytest.mark.asyncio
    async def test_start_service_dies_during_timeout(self, tmp_path):
        """_start_service detects process death during a readline timeout."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        handler._runner_path = "/dev/null"  # prevent regeneration

        mock_proc = MagicMock()
        mock_proc.returncode = 1  # already dead

        async def timeout_readline():
            raise TimeoutError("mock timeout")

        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = timeout_readline
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readline = timeout_readline

        async def mock_create(*args, **kw):
            return mock_proc

        with patch("flute.handlers.asyncio.create_subprocess_exec", mock_create), \
             pytest.raises(RuntimeError, match="died during startup"):
            await handler._start_service()

    @pytest.mark.asyncio
    async def test_start_service_timeout_then_ready(self, tmp_path):
        """_start_service retries after a readline timeout if the process is alive."""
        handler = ServiceHandler(
            worker_type="test", worker_dir=str(tmp_path),
            module_name="dummy", class_name="Dummy",
        )
        handler._runner_path = "/dummy"

        mock_proc = MagicMock()
        mock_proc.returncode = None  # alive

        call_count = [0]

        async def mock_stdout_readline():
            call_count[0] += 1
            if call_count[0] == 1:
                raise TimeoutError("mock timeout")  # first call times out
            return json.dumps({"type": "ready"}).encode() + b"\n"

        async def mock_stderr_readline():
            return b""

        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = mock_stdout_readline
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readline = mock_stderr_readline

        async def mock_create(*args, **kw):
            return mock_proc

        with patch("flute.handlers.asyncio.create_subprocess_exec", mock_create):
            await handler._start_service()
        # Should succeed after retry
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_start_service_deadline_exceeded(self, tmp_path):
        """_start_service raises after the 5-minute deadline."""
        import time as time_mod

        handler = ServiceHandler(
            worker_type="test", worker_dir=str(tmp_path),
            module_name="dummy", class_name="Dummy",
        )
        handler._runner_path = "/dummy"

        # Fully mock the subprocess so asyncio doesn't do real I/O
        mock_proc = MagicMock()
        mock_proc.returncode = None

        async def mock_stdout_readline():
            return json.dumps({"type": "log"}).encode() + b"\n"

        async def mock_stderr_readline():
            return b""  # EOF immediately so drain exits

        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = mock_stdout_readline
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readline = mock_stderr_readline

        async def mock_create(*args, **kw):
            return mock_proc

        base = time_mod.monotonic()
        calls = [0]

        def fast_monotonic():
            calls[0] += 1
            if calls[0] <= 1:
                return base  # deadline = base + 300
            return base + 400  # past deadline

        with patch("flute.handlers.asyncio.create_subprocess_exec", mock_create), \
             patch("flute.handlers.time.monotonic", fast_monotonic), \
             pytest.raises(RuntimeError, match="5 minutes"):
            await handler._start_service()

    @pytest.mark.asyncio
    async def test_read_responses_crash_during_timeout(self, tmp_path):
        """_read_responses detects process death during a readline timeout."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)

        mock_proc = MagicMock()
        mock_proc.returncode = 1  # dead

        async def timeout_readline():
            raise TimeoutError("mock timeout")

        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = timeout_readline
        handler._proc = mock_proc

        r = FakeRedis()
        result = await handler._read_responses("sid", "ch", r, max_runtime=10)
        assert result.status == "error"
        assert "crashed" in result.error.lower()
        assert handler._proc is None  # cleared after crash

    @pytest.mark.asyncio
    async def test_read_responses_bad_json_skipped(self, tmp_path):
        """_read_responses skips non-JSON lines and continues reading."""
        code = """\
from flute.service import Service

class BadJsonService(Service):
    def setup(self):
        pass

    def process(self, job):
        # Write non-JSON directly to protocol output
        self._proto_out.write("not valid json\\n")
        self._proto_out.flush()
"""
        handler = _make_service_handler(tmp_path, code, class_name="BadJsonService")
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-badjson", "ch:svc-badjson",
            )
            # Should still complete — bad JSON is skipped, then "done" is read
            assert result.status == "completed"
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_eof_sentinel_published_on_done(self, tmp_path):
        """An empty string (EOF sentinel) is published when job completes."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)
        r = FakeRedis()
        workdir = tempfile.mkdtemp()
        try:
            result = await handler.execute(
                {"max_runtime_seconds": 10},
                workdir, r, "svc-eof", "ch:svc-eof",
            )
            assert result.status == "completed"
            published = r.published.get("ch:svc-eof", [])
            assert "" in published  # EOF sentinel
        finally:
            if handler._proc and handler._proc.returncode is None:
                handler._proc.kill()
                await handler._proc.wait()
            shutil.rmtree(workdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_cancel_during_read_responses(self, tmp_path):
        """_read_responses returns cancelled when cancel key is set."""
        handler = _make_service_handler(tmp_path, _ECHO_SERVICE_CODE)

        mock_proc = MagicMock()
        mock_proc.returncode = None  # still alive

        async def timeout_readline():
            raise TimeoutError("mock timeout")

        mock_proc.stdout = MagicMock()
        mock_proc.stdout.readline = timeout_readline
        handler._proc = mock_proc

        r = FakeRedis()
        await r.set("session:svc-cancel:cancel", "1")
        result = await handler._read_responses("svc-cancel", "ch", r, max_runtime=30)
        assert result.status == "cancelled"
        assert "cancelled by user" in result.error
