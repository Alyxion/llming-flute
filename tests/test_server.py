"""Unit tests for flute.server."""

import asyncio
import base64
import json
import os
from unittest.mock import MagicMock, patch

import psutil
import pytest
from conftest import FakeRedis
from flute.server import (
    _charge_quota,
    _dir_size_mb,
    _kill_tree,
    _make_preexec,
    _signal_name,
    _stream_output,
    run_session,
    serve,
)

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
        with patch("flute.server.psutil.Process", return_value=parent):
            _kill_tree(12345)
        child1.kill.assert_called_once()
        child2.kill.assert_called_once()
        parent.kill.assert_called_once()

    def test_no_such_process(self):
        with patch(
            "flute.server.psutil.Process", side_effect=psutil.NoSuchProcess(12345)
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
        mock.assert_called_once_with(resource.RLIMIT_FSIZE, (expected, expected))

    def test_handles_os_error(self):
        fn = _make_preexec(10)
        with patch("resource.setrlimit", side_effect=OSError("unsupported")):
            fn()  # should not raise

    def test_handles_value_error(self):
        fn = _make_preexec(10)
        with patch("resource.setrlimit", side_effect=ValueError("bad")):
            fn()  # should not raise


# ---------------------------------------------------------------------------
# run_session
# ---------------------------------------------------------------------------


class TestRunSession:
    @pytest.mark.asyncio
    async def test_basic_completed(self):
        r = FakeRedis()
        await run_session({"session_id": "t1", "code": "print('hello')"}, r)
        assert await r.get("session:t1:status") == "completed"
        assert await r.get("session:t1:exit_code") == "0"
        assert "hello" in await r.get("session:t1:logs")

    @pytest.mark.asyncio
    async def test_error_exit(self):
        r = FakeRedis()
        await run_session({"session_id": "t2", "code": "raise ValueError('boom')"}, r)
        assert await r.get("session:t2:status") == "error"
        assert "boom" in await r.get("session:t2:logs")

    @pytest.mark.asyncio
    async def test_syntax_error(self):
        r = FakeRedis()
        await run_session({"session_id": "t3", "code": "def ("}, r)
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
        await run_session(spec, r)
        assert await r.get("session:t4:status") == "completed"
        assert "test content" in await r.get("session:t4:logs")

    @pytest.mark.asyncio
    async def test_output_files(self):
        r = FakeRedis()
        await run_session(
            {"session_id": "t5", "code": "open('result.txt', 'w').write('output data')"},
            r,
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
        await run_session(spec, r)
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
        await run_session(spec, r)
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
        await run_session(spec, r)
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
        await run_session(spec, r)
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
        with patch("flute.server._dir_size_mb", return_value=100.0):
            await run_session(spec, r)
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
        await run_session(spec, r)
        assert await r.get("session:t10:status") == "killed"
        assert "signal" in (await r.get("session:t10:error")).lower()

    @pytest.mark.asyncio
    async def test_uses_defaults(self):
        r = FakeRedis()
        await run_session({"session_id": "t11", "code": "print(1)"}, r)
        assert await r.get("session:t11:status") == "completed"

    @pytest.mark.asyncio
    async def test_workdir_cleaned_up(self):
        r = FakeRedis()
        await run_session({"session_id": "t12", "code": "print(1)"}, r)
        workdir = await r.get("session:t12:workdir")
        assert not os.path.exists(workdir)

    @pytest.mark.asyncio
    async def test_internal_exception(self):
        r = FakeRedis()
        with patch(
            "flute.server.asyncio.create_subprocess_exec",
            side_effect=RuntimeError("popen failed"),
        ):
            await run_session({"session_id": "t13", "code": "print(1)"}, r)
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
            "flute.server.psutil.Process",
            side_effect=psutil.AccessDenied(pid=1),
        ):
            await run_session(spec, r)
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
        with patch("flute.server._kill_tree"):
            await run_session(spec, r)
        assert await r.get("session:t15:status") == "killed"

    @pytest.mark.asyncio
    async def test_logs_streamed_via_pubsub(self):
        """Log lines are published to Redis pub/sub during execution."""
        r = FakeRedis()
        await run_session({"session_id": "tpub", "code": "print('streamed')"}, r)
        messages = r.published.get("session:tpub:logs:stream", [])
        assert any("streamed" in m for m in messages if m)
        assert "" in messages  # EOF sentinel


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
# serve
# ---------------------------------------------------------------------------


class TestServe:
    @pytest.mark.asyncio
    async def test_processes_session(self):
        fake_r = FakeRedis()
        await fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv1", "code": "print('served')"}),
        )

        shutdown = asyncio.Event()
        original_set = fake_r.set

        async def trigger_shutdown(key, val, ex=None):
            await original_set(key, val, ex=ex)
            if key == "session:srv1:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        await serve(fake_r, shutdown, max_concurrent=2)
        assert await fake_r.get("session:srv1:status") == "completed"
        assert "served" in await fake_r.get("session:srv1:logs")

    @pytest.mark.asyncio
    async def test_skips_invalid_json(self):
        fake_r = FakeRedis()
        await fake_r.rpush("session:queue", "not json")
        await fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv2", "code": "print('ok')"}),
        )

        shutdown = asyncio.Event()
        original_set = fake_r.set

        async def trigger_shutdown(key, val, ex=None):
            await original_set(key, val, ex=ex)
            if key == "session:srv2:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        await serve(fake_r, shutdown, max_concurrent=2)
        assert await fake_r.get("session:srv2:status") == "completed"

    @pytest.mark.asyncio
    async def test_immediate_shutdown(self):
        fake_r = FakeRedis()
        shutdown = asyncio.Event()
        shutdown.set()
        await serve(fake_r, shutdown, max_concurrent=2)

    @pytest.mark.asyncio
    async def test_slots_full_then_shutdown(self):
        """When all slots are busy, serve() waits on acquire."""
        fake_r = FakeRedis()
        await fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv-slot", "code": "import time; time.sleep(5)"}),
        )
        shutdown = asyncio.Event()

        async def delayed_shutdown():
            await asyncio.sleep(2)
            shutdown.set()

        task = asyncio.create_task(delayed_shutdown())
        await serve(fake_r, shutdown, max_concurrent=1)
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

        await serve(fake_r, shutdown, max_concurrent=2)
        assert call_count >= 3


# ---------------------------------------------------------------------------
# _charge_quota
# ---------------------------------------------------------------------------


class TestChargeQuota:
    @pytest.mark.asyncio
    async def test_creates_key_with_ttl(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", 5.0)
        assert float(r.store["quota:user1"]) == pytest.approx(5.0)
        assert r._expires["quota:user1"] > 0

    @pytest.mark.asyncio
    async def test_accumulates(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", 3.0)
        await _charge_quota(r, "user1", 2.5)
        assert float(r.store["quota:user1"]) == pytest.approx(5.5)

    @pytest.mark.asyncio
    async def test_ttl_not_reset_on_subsequent_calls(self):
        r = FakeRedis()
        await _charge_quota(r, "user1", 1.0)
        original_ttl = r._expires["quota:user1"]
        # Simulate TTL already set (positive value from first call)
        await _charge_quota(r, "user1", 1.0)
        assert r._expires["quota:user1"] == original_ttl


# ---------------------------------------------------------------------------
# Quota enforcement in run_session
# ---------------------------------------------------------------------------


class TestQuota:
    @pytest.mark.asyncio
    async def test_no_user_id_skips_quota(self):
        """Sessions without user_id run normally, no quota tracking."""
        r = FakeRedis()
        await run_session({"session_id": "q1", "code": "print('ok')"}, r)
        assert await r.get("session:q1:status") == "completed"
        # No quota key created
        assert await r.get("quota:None") is None

    @pytest.mark.asyncio
    async def test_under_quota_runs(self):
        r = FakeRedis()
        spec = {
            "session_id": "q2",
            "code": "print('ok')",
            "user_id": "alice",
        }
        await run_session(spec, r)
        assert await r.get("session:q2:status") == "completed"
        # Quota key should be created with some elapsed time
        assert await r.get("quota:alice") is not None
        assert float(await r.get("quota:alice")) > 0

    @pytest.mark.asyncio
    async def test_exceeded_quota_rejected(self):
        r = FakeRedis()
        # Pre-load quota at the limit
        with patch("flute.server.QUOTA_SECONDS", 10):
            await r.set("quota:bob", "10.0")
            spec = {
                "session_id": "q3",
                "code": "print('should not run')",
                "user_id": "bob",
            }
            await run_session(spec, r)
        assert await r.get("session:q3:status") == "quota_exceeded"
        assert "quota exceeded" in (await r.get("session:q3:error")).lower()

    @pytest.mark.asyncio
    async def test_quota_accumulates_across_sessions(self):
        r = FakeRedis()
        spec1 = {"session_id": "q4a", "code": "print(1)", "user_id": "carol"}
        spec2 = {"session_id": "q4b", "code": "print(2)", "user_id": "carol"}
        await run_session(spec1, r)
        await run_session(spec2, r)
        assert await r.get("session:q4a:status") == "completed"
        assert await r.get("session:q4b:status") == "completed"
        # Both charged to same user
        total = float(await r.get("quota:carol"))
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
        await run_session(spec, r)
        assert await r.get("session:q5:status") == "killed"
        assert float(await r.get("quota:dave")) >= 1.0

    @pytest.mark.asyncio
    async def test_quota_charged_on_error_session(self):
        """Quota is charged even when subprocess raises an exception."""
        r = FakeRedis()
        spec = {
            "session_id": "q6",
            "code": "raise RuntimeError('boom')",
            "user_id": "eve",
        }
        await run_session(spec, r)
        assert await r.get("session:q6:status") == "error"
        assert float(await r.get("quota:eve")) > 0

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
        # Make _stream_output raise after process starts to trigger except branch
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

        with patch("flute.server.asyncio.create_subprocess_exec", side_effect=mock_create):
            await run_session(spec, r)
        assert await r.get("session:q7:status") == "error"
        assert await r.get("quota:frank") is not None
