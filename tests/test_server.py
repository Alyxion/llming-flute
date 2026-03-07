"""Unit tests for flute.server."""

import base64
import json
import os
import threading
from unittest.mock import MagicMock, patch

import psutil
from conftest import FakeRedis
from flute.server import (
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
    def test_basic_completed(self):
        r = FakeRedis()
        run_session({"session_id": "t1", "code": "print('hello')"}, r)
        assert r.get("session:t1:status") == "completed"
        assert r.get("session:t1:exit_code") == "0"
        assert "hello" in r.get("session:t1:logs")

    def test_error_exit(self):
        r = FakeRedis()
        run_session({"session_id": "t2", "code": "raise ValueError('boom')"}, r)
        assert r.get("session:t2:status") == "error"
        assert "boom" in r.get("session:t2:logs")

    def test_syntax_error(self):
        r = FakeRedis()
        run_session({"session_id": "t3", "code": "def ("}, r)
        assert r.get("session:t3:status") == "error"
        assert "SyntaxError" in r.get("session:t3:logs")

    def test_with_input_files(self):
        r = FakeRedis()
        spec = {
            "session_id": "t4",
            "code": "print(open('data.txt').read())",
            "input_files": {"data.txt": base64.b64encode(b"test content").decode()},
        }
        run_session(spec, r)
        assert r.get("session:t4:status") == "completed"
        assert "test content" in r.get("session:t4:logs")

    def test_output_files(self):
        r = FakeRedis()
        run_session(
            {"session_id": "t5", "code": "open('result.txt', 'w').write('output data')"},
            r,
        )
        assert r.get("session:t5:status") == "completed"
        files = r.hgetall("session:t5:output_files")
        assert "result.txt" in files
        assert base64.b64decode(files["result.txt"]) == b"output data"

    def test_input_files_excluded_from_output(self):
        r = FakeRedis()
        spec = {
            "session_id": "t5b",
            "code": "open('out.txt','w').write('ok')",
            "input_files": {"in.txt": base64.b64encode(b"data").decode()},
        }
        run_session(spec, r)
        files = r.hgetall("session:t5b:output_files")
        assert "in.txt" not in files
        assert "out.txt" in files

    def test_path_traversal_sanitized(self):
        r = FakeRedis()
        spec = {
            "session_id": "t6",
            "code": "import os; print(os.listdir('.'))",
            "input_files": {
                "../../../etc/passwd": base64.b64encode(b"hacked").decode(),
            },
        }
        run_session(spec, r)
        assert r.get("session:t6:status") == "completed"
        assert "passwd" in r.get("session:t6:logs")

    def test_runtime_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t7",
            "code": "import time; time.sleep(60)",
            "max_runtime_seconds": 1,
        }
        run_session(spec, r)
        assert r.get("session:t7:status") == "killed"
        assert "runtime" in r.get("session:t7:error").lower()

    def test_memory_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t8",
            "code": "x = bytearray(200_000_000)\nimport time; time.sleep(30)",
            "max_memory_mb": 32,
            "max_runtime_seconds": 15,
        }
        run_session(spec, r)
        assert r.get("session:t8:status") == "killed"
        assert "memory" in r.get("session:t8:error").lower()

    def test_disk_exceeded(self):
        r = FakeRedis()
        spec = {
            "session_id": "t9",
            "code": "import time; time.sleep(30)",
            "max_disk_mb": 5,
            "max_runtime_seconds": 10,
        }
        with patch("flute.server._dir_size_mb", return_value=100.0):
            run_session(spec, r)
        assert r.get("session:t9:status") == "killed"
        assert "disk" in r.get("session:t9:error").lower()

    def test_negative_exit_code(self):
        r = FakeRedis()
        spec = {
            "session_id": "t10",
            "code": "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
            "max_runtime_seconds": 10,
        }
        run_session(spec, r)
        assert r.get("session:t10:status") == "killed"
        assert "signal" in r.get("session:t10:error").lower()

    def test_uses_defaults(self):
        r = FakeRedis()
        run_session({"session_id": "t11", "code": "print(1)"}, r)
        assert r.get("session:t11:status") == "completed"

    def test_workdir_cleaned_up(self):
        r = FakeRedis()
        run_session({"session_id": "t12", "code": "print(1)"}, r)
        workdir = r.get("session:t12:workdir")
        assert not os.path.exists(workdir)

    def test_internal_exception(self):
        r = FakeRedis()
        with patch(
            "flute.server.subprocess.Popen", side_effect=RuntimeError("popen failed")
        ):
            run_session({"session_id": "t13", "code": "print(1)"}, r)
        assert r.get("session:t13:status") == "error"
        assert "popen failed" in r.get("session:t13:error")
        assert "popen failed" in r.get("session:t13:logs")

    def test_psutil_access_denied(self):
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
            run_session(spec, r)
        # Should complete normally (AccessDenied is caught, monitor continues)
        assert r.get("session:t14:status") == "completed"

    def test_proc_wait_timeout_on_kill(self):
        """When _kill_tree fails to kill, proc.kill() is the fallback."""
        r = FakeRedis()
        spec = {
            "session_id": "t15",
            "code": "import time; time.sleep(60)",
            "max_runtime_seconds": 1,
        }
        # Mock _kill_tree to be a no-op so the process survives
        with patch("flute.server._kill_tree"):
            run_session(spec, r)
        assert r.get("session:t15:status") == "killed"

    def test_logs_streamed_via_pubsub(self):
        """Log lines are published to Redis pub/sub during execution."""
        r = FakeRedis()
        run_session({"session_id": "tpub", "code": "print('streamed')"}, r)
        messages = r.published.get("session:tpub:logs:stream", [])
        assert any("streamed" in m for m in messages if m)
        assert "" in messages  # EOF sentinel


# ---------------------------------------------------------------------------
# _stream_output
# ---------------------------------------------------------------------------


class TestStreamOutput:
    def test_writes_to_log_and_publishes(self, tmp_path):
        import io

        pipe = io.BytesIO(b"line1\nline2\n")
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()
        channel = "test:stream"

        _stream_output(pipe, log_file, r, channel)
        log_file.close()

        assert (tmp_path / "test.log").read_text() == "line1\nline2\n"
        assert r.published[channel] == ["line1\n", "line2\n", ""]

    def test_handles_publish_failure(self, tmp_path):
        import io

        pipe = io.BytesIO(b"ok\n")
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()
        r.publish = MagicMock(side_effect=RuntimeError("redis down"))

        _stream_output(pipe, log_file, r, "ch")
        log_file.close()

        assert (tmp_path / "test.log").read_text() == "ok\n"

    def test_empty_pipe(self, tmp_path):
        import io

        pipe = io.BytesIO(b"")
        log_file = open(tmp_path / "test.log", "w")  # noqa: SIM115
        r = FakeRedis()

        _stream_output(pipe, log_file, r, "ch")
        log_file.close()

        assert (tmp_path / "test.log").read_text() == ""
        assert r.published["ch"] == [""]  # EOF sentinel only


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


class TestServe:
    def test_processes_session(self):
        fake_r = FakeRedis()
        fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv1", "code": "print('served')"}),
        )

        shutdown = threading.Event()
        original_set = fake_r.set

        def trigger_shutdown(key, val, ex=None):
            original_set(key, val, ex=ex)
            if key == "session:srv1:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        serve(fake_r, shutdown, max_concurrent=2)
        assert fake_r.get("session:srv1:status") == "completed"
        assert "served" in fake_r.get("session:srv1:logs")

    def test_skips_invalid_json(self):
        fake_r = FakeRedis()
        fake_r.rpush("session:queue", "not json")
        fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv2", "code": "print('ok')"}),
        )

        shutdown = threading.Event()
        original_set = fake_r.set

        def trigger_shutdown(key, val, ex=None):
            original_set(key, val, ex=ex)
            if key == "session:srv2:status" and val in ("completed", "error", "killed"):
                shutdown.set()

        fake_r.set = trigger_shutdown

        serve(fake_r, shutdown, max_concurrent=2)
        assert fake_r.get("session:srv2:status") == "completed"

    def test_immediate_shutdown(self):
        fake_r = FakeRedis()
        shutdown = threading.Event()
        shutdown.set()
        serve(fake_r, shutdown, max_concurrent=2)
        # Should return immediately without processing anything

    def test_slots_full_then_shutdown(self):
        """When all slots are busy, serve() loops on acquire timeout."""
        fake_r = FakeRedis()
        fake_r.rpush(
            "session:queue",
            json.dumps({"session_id": "srv-slot", "code": "import time; time.sleep(5)"}),
        )
        shutdown = threading.Event()

        def delayed_shutdown():
            import time

            time.sleep(2)
            shutdown.set()

        t = threading.Thread(target=delayed_shutdown, daemon=True)
        t.start()

        # max_concurrent=1: the job takes the slot, acquire times out on next iteration
        serve(fake_r, shutdown, max_concurrent=1)

    def test_shutdown_after_acquire(self):
        """Shutdown fires after slot acquired, triggers early break."""
        fake_r = FakeRedis()
        shutdown = threading.Event()
        acquire_count = 0

        class ShutdownSemaphore(threading.Semaphore):
            def acquire(self, blocking=True, timeout=None):
                nonlocal acquire_count
                result = super().acquire(blocking=blocking, timeout=timeout)
                acquire_count += 1
                if acquire_count == 2:
                    shutdown.set()
                return result

        with patch("flute.server.threading.Semaphore", ShutdownSemaphore):
            serve(fake_r, shutdown, max_concurrent=2)

    def test_empty_queue_then_shutdown(self):
        fake_r = FakeRedis()
        shutdown = threading.Event()

        call_count = 0
        original_blpop = fake_r.blpop

        def counting_blpop(key, timeout=0):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                shutdown.set()
            return original_blpop(key, timeout)

        fake_r.blpop = counting_blpop

        serve(fake_r, shutdown, max_concurrent=2)
        assert call_count >= 3
