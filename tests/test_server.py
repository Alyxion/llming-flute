"""Unit tests for flute.server."""

import asyncio
import base64
import json
import os
from unittest.mock import patch

import pytest
from conftest import FakeRedis
from flute.handlers import PythonHandler, ServiceHandler, TaskRunnerHandler
from flute.server import (
    _charge_quota,
    _heartbeat,
    _register_service,
    create_handler,
    run_session,
    serve,
)

PYTHON_HANDLER = PythonHandler()


# ---------------------------------------------------------------------------
# create_handler
# ---------------------------------------------------------------------------


class TestCreateHandler:
    def test_no_worker_dir_defaults_to_registry(self):
        handler = create_handler(None)
        assert isinstance(handler, PythonHandler)
        assert handler.worker_type == "python"

    def test_empty_worker_dir_defaults_to_registry(self):
        handler = create_handler("")
        assert isinstance(handler, PythonHandler)

    def test_python_runner_from_dir(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "my-python"
dependencies = ["numpy", "pandas"]

[tool.flute.worker]
type = "python-runner"
description = "Python with science libs"
""")
        handler = create_handler(str(tmp_path))
        assert isinstance(handler, PythonHandler)
        assert handler.worker_type == "my-python"
        assert handler.worker_config is not None
        assert handler.worker_config["dependencies"] == ["numpy", "pandas"]
        assert handler.worker_config["description"] == "Python with science libs"

    def test_task_runner_from_dir(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "echo-worker"
dependencies = ["pydantic"]

[tool.flute.worker]
type = "task-runner"
handler = "handler:EchoHandler"
""")
        handler_py = tmp_path / "handler.py"
        handler_py.write_text("""\
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
""")
        handler = create_handler(str(tmp_path))
        assert isinstance(handler, TaskRunnerHandler)
        assert handler.worker_type == "echo-worker"
        assert handler.worker_config["dependencies"] == ["pydantic"]

    def test_no_dir_has_no_config(self):
        handler = create_handler(None)
        assert handler.worker_config is None

    def test_service_runner_from_dir(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "image-service"
dependencies = ["pillow"]

[tool.flute.worker]
type = "service-runner"
handler = "svc_handler:ImageService"
description = "Image processing service"
operations = ["resize", "crop", "watermark"]
""")
        handler = create_handler(str(tmp_path))
        assert isinstance(handler, ServiceHandler)
        assert handler.worker_type == "image-service"
        assert handler.worker_config is not None
        assert handler.worker_config["type"] == "service-runner"
        assert handler.worker_config["operations"] == ["resize", "crop", "watermark"]
        assert handler.worker_config["description"] == "Image processing service"
        assert handler.worker_config["dependencies"] == ["pillow"]


# ---------------------------------------------------------------------------
# _register_service
# ---------------------------------------------------------------------------


class TestRegisterService:
    @pytest.mark.asyncio
    async def test_python_runner_no_config(self):
        """Handler without worker_config still registers with empty defaults."""
        r = FakeRedis()
        handler = PythonHandler()
        await _register_service(r, handler)
        raw = await r.hgetall("services")
        assert "python" in raw
        info = json.loads(raw["python"])
        assert info["type"] == "python-runner"
        assert info["worker_type"] == "python"
        assert info["dependencies"] == []
        assert info["description"] == ""

    @pytest.mark.asyncio
    async def test_python_runner_with_config(self):
        """Handler with worker_config publishes dependencies and description."""
        r = FakeRedis()
        handler = PythonHandler(worker_type="science-python")
        handler.worker_config = {
            "type": "python-runner",
            "description": "Python with scientific libs",
            "dependencies": ["numpy", "pandas", "matplotlib"],
        }
        await _register_service(r, handler)
        info = json.loads((await r.hgetall("services"))["science-python"])
        assert info["type"] == "python-runner"
        assert info["description"] == "Python with scientific libs"
        assert info["dependencies"] == ["numpy", "pandas", "matplotlib"]

    @pytest.mark.asyncio
    async def test_task_runner(self):
        from pydantic import BaseModel

        class MyTask(BaseModel):
            x: int

        class MyResp(BaseModel):
            y: int

        class MyHandler:
            task_model = MyTask
            response_model = MyResp
            description = "Test handler"

            async def execute(self, task, input_files):
                return MyResp(y=task.x), {}

        r = FakeRedis()
        handler = TaskRunnerHandler(worker_type="my-task", task_handler=MyHandler())
        handler.worker_config = {
            "type": "task-runner",
            "description": "Config description",
            "dependencies": ["pydantic"],
        }
        await _register_service(r, handler)
        raw = await r.hgetall("services")
        assert "my-task" in raw
        info = json.loads(raw["my-task"])
        assert info["type"] == "task-runner"
        assert info["task_schema"]["properties"]["x"]["type"] == "integer"
        assert info["response_schema"]["properties"]["y"]["type"] == "integer"
        assert info["description"] == "Config description"
        assert info["dependencies"] == ["pydantic"]

    @pytest.mark.asyncio
    async def test_task_runner_description_fallback(self):
        """If config has no description, falls back to handler's description attr."""
        from pydantic import BaseModel

        class T(BaseModel):
            pass

        class R(BaseModel):
            pass

        class H:
            task_model = T
            response_model = R
            description = "From handler"

            async def execute(self, task, input_files):
                return R(), {}

        r = FakeRedis()
        handler = TaskRunnerHandler(worker_type="fallback", task_handler=H())
        handler.worker_config = {"type": "task-runner", "description": "", "dependencies": []}
        await _register_service(r, handler)
        info = json.loads((await r.hgetall("services"))["fallback"])
        assert info["description"] == "From handler"

    @pytest.mark.asyncio
    async def test_task_runner_no_description(self):
        from pydantic import BaseModel

        class T(BaseModel):
            pass

        class R(BaseModel):
            pass

        class H:
            task_model = T
            response_model = R

            async def execute(self, task, input_files):
                return R(), {}

        r = FakeRedis()
        handler = TaskRunnerHandler(worker_type="no-desc", task_handler=H())
        await _register_service(r, handler)
        info = json.loads((await r.hgetall("services"))["no-desc"])
        assert info["description"] == ""

    @pytest.mark.asyncio
    async def test_service_runner(self):
        """ServiceHandler registers with type='service-runner' and operations list."""
        r = FakeRedis()
        handler = ServiceHandler(
            worker_type="image-service",
            worker_dir="/fake/dir",
            module_name="svc",
            class_name="ImageSvc",
        )
        handler.worker_config = {
            "type": "service-runner",
            "description": "Image processing",
            "dependencies": ["pillow", "opencv-python"],
            "operations": ["resize", "crop", "watermark"],
        }
        await _register_service(r, handler)
        raw = await r.hgetall("services")
        assert "image-service" in raw
        info = json.loads(raw["image-service"])
        assert info["type"] == "service-runner"
        assert info["worker_type"] == "image-service"
        assert info["description"] == "Image processing"
        assert info["dependencies"] == ["pillow", "opencv-python"]
        assert info["operations"] == ["resize", "crop", "watermark"]

    @pytest.mark.asyncio
    async def test_service_runner_accept_files(self):
        """ServiceHandler registers accept_files from worker config."""
        r = FakeRedis()
        handler = ServiceHandler(
            worker_type="image-service",
            worker_dir="/fake/dir",
            module_name="svc",
            class_name="ImageSvc",
        )
        handler.worker_config = {
            "type": "service-runner",
            "description": "Image processing",
            "operations": ["resize"],
            "accept_files": ["image/*"],
        }
        await _register_service(r, handler)
        info = json.loads((await r.hgetall("services"))["image-service"])
        assert info["accept_files"] == ["image/*"]


# ---------------------------------------------------------------------------
# run_session
# ---------------------------------------------------------------------------


class TestRunSession:
    @pytest.mark.asyncio
    async def test_basic_completed(self):
        r = FakeRedis()
        await run_session({"session_id": "t1", "code": "print('hello')"}, r, PYTHON_HANDLER)
        assert await r.get("session:t1:status") == "completed"
        assert await r.get("session:t1:exit_code") == "0"
        assert "hello" in await r.get("session:t1:logs")

    @pytest.mark.asyncio
    async def test_error_exit(self):
        r = FakeRedis()
        await run_session(
            {"session_id": "t2", "code": "raise ValueError('boom')"}, r, PYTHON_HANDLER
        )
        assert await r.get("session:t2:status") == "error"
        assert "boom" in await r.get("session:t2:logs")

    @pytest.mark.asyncio
    async def test_syntax_error(self):
        r = FakeRedis()
        await run_session({"session_id": "t3", "code": "def ("}, r, PYTHON_HANDLER)
        assert await r.get("session:t3:status") == "error"
        assert "SyntaxError" in await r.get("session:t3:logs")

    @pytest.mark.asyncio
    async def test_with_input_files(self):
        r = FakeRedis()
        spec = {
            "session_id": "t4",
            "code": "print(open('data.txt').read())",
            "input_files": {"data.txt": base64.b64encode(b"test content").decode()},
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t4:status") == "completed"
        assert "test content" in await r.get("session:t4:logs")

    @pytest.mark.asyncio
    async def test_output_files(self):
        r = FakeRedis()
        await run_session(
            {"session_id": "t5", "code": "open('result.txt', 'w').write('output data')"},
            r, PYTHON_HANDLER,
        )
        assert await r.get("session:t5:status") == "completed"
        files = await r.hgetall("session:t5:output_files")
        assert "result.txt" in files
        assert base64.b64decode(files["result.txt"]) == b"output data"

    @pytest.mark.asyncio
    async def test_input_files_excluded_from_output(self):
        r = FakeRedis()
        spec = {
            "session_id": "t5b",
            "code": "open('out.txt','w').write('ok')",
            "input_files": {"in.txt": base64.b64encode(b"data").decode()},
        }
        await run_session(spec, r, PYTHON_HANDLER)
        files = await r.hgetall("session:t5b:output_files")
        assert "in.txt" not in files
        assert "out.txt" in files

    @pytest.mark.asyncio
    async def test_path_traversal_sanitized(self):
        r = FakeRedis()
        spec = {
            "session_id": "t6",
            "code": "import os; print(os.listdir('.'))",
            "input_files": {
                "../../../etc/passwd": base64.b64encode(b"hacked").decode(),
            },
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t6:status") == "completed"
        assert "passwd" in await r.get("session:t6:logs")

    @pytest.mark.asyncio
    async def test_runtime_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t7",
            "code": "import time; time.sleep(60)",
            "max_runtime_seconds": 1,
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t7:status") == "killed"
        assert "runtime" in (await r.get("session:t7:error")).lower()

    @pytest.mark.asyncio
    async def test_memory_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t8",
            "code": "x = bytearray(200_000_000)\nimport time; time.sleep(30)",
            "max_memory_mb": 32,
            "max_runtime_seconds": 15,
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t8:status") == "killed"
        assert "memory" in (await r.get("session:t8:error")).lower()

    @pytest.mark.asyncio
    async def test_disk_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t9",
            "code": "import time; time.sleep(30)",
            "max_disk_mb": 5,
            "max_runtime_seconds": 10,
        }
        with patch("flute.handlers._dir_size_mb", return_value=100.0):
            await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t9:status") == "killed"
        assert "disk" in (await r.get("session:t9:error")).lower()

    @pytest.mark.asyncio
    async def test_negative_exit_code(self):
        r = FakeRedis()
        spec = {
            "session_id": "t10",
            "code": "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
            "max_runtime_seconds": 10,
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t10:status") == "killed"
        assert "signal" in (await r.get("session:t10:error")).lower()

    @pytest.mark.asyncio
    async def test_uses_defaults(self):
        r = FakeRedis()
        await run_session({"session_id": "t11", "code": "print(1)"}, r, PYTHON_HANDLER)
        assert await r.get("session:t11:status") == "completed"

    @pytest.mark.asyncio
    async def test_workdir_cleaned_up(self):
        r = FakeRedis()
        await run_session({"session_id": "t12", "code": "print(1)"}, r, PYTHON_HANDLER)
        workdir = await r.get("session:t12:workdir")
        assert not os.path.exists(workdir)

    @pytest.mark.asyncio
    async def test_internal_exception(self):
        r = FakeRedis()
        with patch(
            "flute.handlers.asyncio.create_subprocess_exec",
            side_effect=RuntimeError("popen failed"),
        ):
            await run_session({"session_id": "t13", "code": "print(1)"}, r, PYTHON_HANDLER)
        assert await r.get("session:t13:status") == "error"
        assert "popen failed" in await r.get("session:t13:error")
        assert "popen failed" in await r.get("session:t13:logs")

    @pytest.mark.asyncio
    async def test_psutil_access_denied(self):
        """psutil.AccessDenied in monitor loop is handled gracefully."""
        r = FakeRedis()
        spec = {
            "session_id": "t14",
            "code": "print('ok')",
            "max_runtime_seconds": 10,
        }
        with patch(
            "flute.handlers.psutil.Process",
            side_effect=__import__("psutil").AccessDenied(pid=1),
        ):
            await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t14:status") == "completed"

    @pytest.mark.asyncio
    async def test_proc_wait_timeout_on_kill(self):
        """When _kill_tree fails to kill, proc.kill() is the fallback."""
        r = FakeRedis()
        spec = {
            "session_id": "t15",
            "code": "import time; time.sleep(60)",
            "max_runtime_seconds": 1,
        }
        with patch("flute.handlers._kill_tree"):
            await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:t15:status") == "killed"

    @pytest.mark.asyncio
    async def test_logs_streamed_via_pubsub(self):
        """Log lines are published to Redis pub/sub during execution."""
        r = FakeRedis()
        await run_session(
            {"session_id": "tpub", "code": "print('streamed')"}, r, PYTHON_HANDLER
        )
        messages = r.published.get("session:tpub:logs:stream", [])
        assert any("streamed" in m for m in messages if m)
        assert "" in messages  # EOF sentinel

    @pytest.mark.asyncio
    async def test_workdir_permissions(self):
        """Workdir is created with 0o700 permissions."""
        r = FakeRedis()
        original_rmtree = __import__("shutil").rmtree
        saved_mode = []

        def capture_rmtree(path, **kwargs):
            if os.path.exists(path):
                saved_mode.append(os.stat(path).st_mode & 0o777)
            original_rmtree(path, **kwargs)

        with patch("flute.server.shutil.rmtree", side_effect=capture_rmtree):
            await run_session(
                {"session_id": "t16", "code": "print(1)"}, r, PYTHON_HANDLER
            )
        assert saved_mode[0] == 0o700

    @pytest.mark.asyncio
    async def test_response_stored_for_task_runner(self):
        """Task runner response is stored in Redis."""
        from pydantic import BaseModel

        class T(BaseModel):
            x: int

        class R(BaseModel):
            y: int

        class H:
            task_model = T
            response_model = R

            async def execute(self, task, input_files):
                return R(y=task.x * 2), {}

        handler = TaskRunnerHandler(worker_type="resp-test", task_handler=H())
        r = FakeRedis()
        await run_session(
            {"session_id": "tr-resp", "task": '{"x": 5}', "max_runtime_seconds": 10},
            r, handler,
        )
        assert await r.get("session:tr-resp:status") == "completed"
        resp = await r.get("session:tr-resp:response")
        assert resp is not None
        assert json.loads(resp)["y"] == 10

    @pytest.mark.asyncio
    async def test_log_lines_buffered(self):
        """Log lines are buffered in a Redis list during execution."""
        r = FakeRedis()
        await run_session(
            {"session_id": "tlog", "code": "for i in range(3): print(f'line{i}')"},
            r, PYTHON_HANDLER,
        )
        assert await r.get("session:tlog:status") == "completed"
        lines = await r.lrange("session:tlog:logs:lines", 0, -1)
        assert len(lines) == 3
        assert "line0" in lines[0]

    @pytest.mark.asyncio
    async def test_log_lines_ttl_set(self):
        """Log lines list gets TTL after execution."""
        r = FakeRedis()
        await run_session(
            {"session_id": "ttl-log", "code": "print('hi')"}, r, PYTHON_HANDLER
        )
        assert r._expires.get("session:ttl-log:logs:lines") is not None

    @pytest.mark.asyncio
    async def test_output_files_ttl_uses_session_ttl(self):
        """Output files hash TTL matches SESSION_TTL."""
        r = FakeRedis()
        with patch("flute.server.SESSION_TTL", 900):
            await run_session(
                {
                    "session_id": "ttl-out",
                    "code": "open('f.txt','w').write('data')",
                },
                r, PYTHON_HANDLER,
            )
        assert r._expires.get("session:ttl-out:output_files") == 900

    @pytest.mark.asyncio
    async def test_no_response_for_python_runner(self):
        """Python runner sessions don't store a response key."""
        r = FakeRedis()
        await run_session(
            {"session_id": "no-resp", "code": "print(1)"}, r, PYTHON_HANDLER
        )
        assert await r.get("session:no-resp:response") is None


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


class TestServe:
    @pytest.mark.asyncio
    async def test_processes_session(self):
        fake_r = FakeRedis()
        await fake_r.rpush(
            "session:queue:python",
            json.dumps({"session_id": "srv1", "code": "print('served')"}),
        )

        shutdown = asyncio.Event()
        original_set = fake_r.set

        async def trigger_shutdown(key, val, ex=None):
            await original_set(key, val, ex=ex)
            if key == "session:srv1:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)
        assert await fake_r.get("session:srv1:status") == "completed"
        assert "served" in await fake_r.get("session:srv1:logs")

    @pytest.mark.asyncio
    async def test_skips_invalid_json(self):
        fake_r = FakeRedis()
        await fake_r.rpush("session:queue:python", "not json")
        await fake_r.rpush(
            "session:queue:python",
            json.dumps({"session_id": "srv2", "code": "print('ok')"}),
        )

        shutdown = asyncio.Event()
        original_set = fake_r.set

        async def trigger_shutdown(key, val, ex=None):
            await original_set(key, val, ex=ex)
            if key == "session:srv2:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)
        assert await fake_r.get("session:srv2:status") == "completed"

    @pytest.mark.asyncio
    async def test_immediate_shutdown(self):
        fake_r = FakeRedis()
        shutdown = asyncio.Event()
        shutdown.set()
        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)

    @pytest.mark.asyncio
    async def test_slots_full_then_shutdown(self):
        """When all slots are busy, serve() waits on acquire."""
        fake_r = FakeRedis()
        await fake_r.rpush(
            "session:queue:python",
            json.dumps({"session_id": "srv-slot", "code": "import time; time.sleep(5)"}),
        )
        shutdown = asyncio.Event()

        async def delayed_shutdown():
            await asyncio.sleep(2)
            shutdown.set()

        task = asyncio.create_task(delayed_shutdown())
        await serve(fake_r, shutdown, max_concurrent=1, handler=PYTHON_HANDLER)
        await task

    @pytest.mark.asyncio
    async def test_empty_queue_then_shutdown(self):
        fake_r = FakeRedis()
        shutdown = asyncio.Event()

        call_count = 0
        original_blpop = fake_r.blpop

        async def counting_blpop(key, timeout=0):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                shutdown.set()
            return await original_blpop(key, timeout)

        fake_r.blpop = counting_blpop

        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_default_handler(self):
        """When no handler is passed, creates handler from config."""
        fake_r = FakeRedis()
        await fake_r.rpush(
            "session:queue:python",
            json.dumps({"session_id": "srv-def", "code": "print('default')"}),
        )

        shutdown = asyncio.Event()
        original_set = fake_r.set

        async def trigger_shutdown(key, val, ex=None):
            await original_set(key, val, ex=ex)
            if key == "session:srv-def:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        # No handler argument — should default to PythonHandler
        await serve(fake_r, shutdown, max_concurrent=2)
        assert await fake_r.get("session:srv-def:status") == "completed"

    @pytest.mark.asyncio
    async def test_publishes_quota_rules(self):
        fake_r = FakeRedis()
        shutdown = asyncio.Event()
        shutdown.set()
        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)
        assert await fake_r.get("quota:__config:rules") is not None

    @pytest.mark.asyncio
    async def test_registers_service(self):
        """serve() registers the handler in the services hash."""
        fake_r = FakeRedis()
        shutdown = asyncio.Event()
        shutdown.set()
        await serve(fake_r, shutdown, max_concurrent=2, handler=PYTHON_HANDLER)
        services = await fake_r.hgetall("services")
        assert "python" in services
        info = json.loads(services["python"])
        assert info["type"] == "python-runner"


# ---------------------------------------------------------------------------
# _charge_quota
# ---------------------------------------------------------------------------


class TestChargeQuota:
    @pytest.mark.asyncio
    async def test_creates_key_with_ttl(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", "python", 5.0, 3600)
        assert float(r.store["quota:user1:python"]) == pytest.approx(5.0)
        assert r._expires["quota:user1:python"] > 0

    @pytest.mark.asyncio
    async def test_accumulates(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", "python", 3.0, 3600)
        await _charge_quota(r, "user1", "python", 2.5, 3600)
        assert float(r.store["quota:user1:python"]) == pytest.approx(5.5)

    @pytest.mark.asyncio
    async def test_ttl_not_reset_on_subsequent_calls(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", "python", 1.0, 3600)
        original_ttl = r._expires["quota:user1:python"]
        # Simulate TTL already set (positive value from first call)
        await _charge_quota(r, "user1", "python", 1.0, 3600)
        assert r._expires["quota:user1:python"] == original_ttl

    @pytest.mark.asyncio
    async def test_per_type_keys(self):
        """Different worker types have separate quota keys."""
        r = FakeRedis()
        await _charge_quota(r, "user1", "python", 5.0, 3600)
        await _charge_quota(r, "user1", "image", 2.0, 1800)
        assert float(r.store["quota:user1:python"]) == pytest.approx(5.0)
        assert float(r.store["quota:user1:image"]) == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# _heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_registers_and_deregisters(self):
        r = FakeRedis()
        shutdown = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.1)
            shutdown.set()

        task = asyncio.create_task(stop_soon())
        await _heartbeat(r, "python", "w123", shutdown)
        await task

        # Should have registered
        workers = await r.hgetall("workers")
        # After shutdown, heartbeat deletes its key
        assert "python:w123" not in workers

    @pytest.mark.asyncio
    async def test_heartbeat_timeout_branch(self):
        """Heartbeat loops via TimeoutError when shutdown isn't set quickly."""
        r = FakeRedis()
        shutdown = asyncio.Event()

        loop_count = 0
        original_hset = r.hset

        async def counting_hset(key, field, val):
            nonlocal loop_count
            await original_hset(key, field, val)
            loop_count += 1
            if loop_count >= 2:
                shutdown.set()

        r.hset = counting_hset

        # Use very short timeout to trigger the TimeoutError branch
        with patch("flute.server.asyncio.wait_for", side_effect=TimeoutError):
            # Need to set shutdown after a couple of iterations
            async def stop_after_iterations():
                while loop_count < 2:
                    await asyncio.sleep(0.01)
                shutdown.set()

            task = asyncio.create_task(stop_after_iterations())
            await _heartbeat(r, "python", "w789", shutdown)
            await task
        assert loop_count >= 2

    @pytest.mark.asyncio
    async def test_heartbeat_refreshes_infrastructure_ttls(self):
        """Heartbeat sets TTLs on workers, services, and quota config."""
        r = FakeRedis()
        # Pre-populate services so expire has something to act on
        await r.hset("services", "python", "{}")
        shutdown = asyncio.Event()

        async def check_and_stop():
            for _ in range(50):
                if "workers" in r._expires:
                    shutdown.set()
                    return
                await asyncio.sleep(0.01)
            shutdown.set()

        task = asyncio.create_task(check_and_stop())
        await _heartbeat(r, "python", "w-infra", shutdown)
        await task

        assert r._expires.get("workers") == 120
        assert r._expires.get("services") == 120
        assert "quota:__config:rules" in r.store

    @pytest.mark.asyncio
    async def test_heartbeat_writes_info(self):
        r = FakeRedis()
        shutdown = asyncio.Event()

        async def check_and_stop():
            # Wait for at least one heartbeat write
            for _ in range(50):
                workers = await r.hgetall("workers")
                if "python:w456" in workers:
                    import json as _json
                    info = _json.loads(workers["python:w456"])
                    assert info["worker_type"] == "python"
                    assert "last_heartbeat" in info
                    assert "pid" in info
                    shutdown.set()
                    return
                await asyncio.sleep(0.01)
            shutdown.set()

        task = asyncio.create_task(check_and_stop())
        await _heartbeat(r, "python", "w456", shutdown)
        await task


# ---------------------------------------------------------------------------
# Quota enforcement in run_session
# ---------------------------------------------------------------------------


class TestQuota:
    @pytest.mark.asyncio
    async def test_no_user_id_skips_quota(self):
        """Sessions without user_id run normally, no quota tracking."""
        r = FakeRedis()
        await run_session({"session_id": "q1", "code": "print('ok')"}, r, PYTHON_HANDLER)
        assert await r.get("session:q1:status") == "completed"
        # No quota key created
        assert await r.get("quota:None:python") is None

    @pytest.mark.asyncio
    async def test_under_quota_runs(self):
        r = FakeRedis()
        spec = {
            "session_id": "q2",
            "code": "print('ok')",
            "user_id": "alice",
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:q2:status") == "completed"
        # Quota key should be created with some elapsed time
        assert await r.get("quota:alice:python") is not None
        assert float(await r.get("quota:alice:python")) > 0

    @pytest.mark.asyncio
    async def test_exceeded_quota_rejected(self):
        r = FakeRedis()
        # Pre-load quota at the limit
        with (
            patch("flute.server.QUOTA_SECONDS", 10),
            patch(
                "flute.server._quota_rules",
                __import__("flute.quota_rules", fromlist=["parse_quota_rules"])
                .parse_quota_rules("*:10/3600"),
            ),
        ):
                await r.set("quota:bob:python", "10.0")
                spec = {
                    "session_id": "q3",
                    "code": "print('should not run')",
                    "user_id": "bob",
                }
                await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:q3:status") == "quota_exceeded"
        assert "quota exceeded" in (await r.get("session:q3:error")).lower()

    @pytest.mark.asyncio
    async def test_quota_accumulates_across_sessions(self):
        r = FakeRedis()
        spec1 = {"session_id": "q4a", "code": "print(1)", "user_id": "carol"}
        spec2 = {"session_id": "q4b", "code": "print(2)", "user_id": "carol"}
        await run_session(spec1, r, PYTHON_HANDLER)
        await run_session(spec2, r, PYTHON_HANDLER)
        assert await r.get("session:q4a:status") == "completed"
        assert await r.get("session:q4b:status") == "completed"
        # Both charged to same user
        total = float(await r.get("quota:carol:python"))
        assert total > 0

    @pytest.mark.asyncio
    async def test_quota_charged_on_killed_session(self):
        """Quota is still charged when a session is killed."""
        r = FakeRedis()
        spec = {
            "session_id": "q5",
            "code": "import time; time.sleep(60)",
            "user_id": "dave",
            "max_runtime_seconds": 1,
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:q5:status") == "killed"
        assert float(await r.get("quota:dave:python")) >= 1.0

    @pytest.mark.asyncio
    async def test_quota_charged_on_error_session(self):
        """Quota is charged even when subprocess raises an exception."""
        r = FakeRedis()
        spec = {
            "session_id": "q6",
            "code": "raise RuntimeError('boom')",
            "user_id": "eve",
        }
        await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:q6:status") == "error"
        assert float(await r.get("quota:eve:python")) > 0

    @pytest.mark.asyncio
    async def test_quota_charged_on_internal_exception(self):
        """Quota is charged when an internal exception occurs after subprocess start."""
        r = FakeRedis()
        spec = {
            "session_id": "q7",
            "code": "import time; time.sleep(30)",
            "user_id": "frank",
            "max_runtime_seconds": 10,
        }
        # Make create_subprocess_exec raise to trigger except branch
        original_create = asyncio.create_subprocess_exec

        async def mock_create(*args, **kwargs):
            proc = await original_create(*args, **kwargs)
            original_wait = proc.wait

            async def failing_wait():
                proc.kill()
                await original_wait()
                raise RuntimeError("injected error")

            proc.wait = failing_wait
            return proc

        with patch("flute.handlers.asyncio.create_subprocess_exec", side_effect=mock_create):
            await run_session(spec, r, PYTHON_HANDLER)
        assert await r.get("session:q7:status") == "error"
        assert await r.get("quota:frank:python") is not None
