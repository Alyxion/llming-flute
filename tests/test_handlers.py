"""Unit tests for flute.handlers."""

import asyncio
import os
from unittest.mock import MagicMock, patch

import psutil
import pytest
from conftest import FakeRedis
from flute.handlers import (
    HANDLER_REGISTRY,
    HandlerResult,
    PythonHandler,
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
