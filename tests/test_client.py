"""Unit tests for flute.client.SessionClient."""

import asyncio
import base64
import json
import os
import time
from unittest.mock import AsyncMock, patch

import pytest
from flute.client import SessionClient


class TestInit:
    @pytest.mark.asyncio
    async def test_default_url(self, fake_redis):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("REDIS_URL", None)
            with patch("flute.client.connect_redis", new_callable=AsyncMock,
                       return_value=fake_redis) as mock:
                c = SessionClient()
                await c._ensure_connected()
                mock.assert_called_once_with("redis://localhost:6379/0", decode_responses=True)

    @pytest.mark.asyncio
    async def test_custom_url(self, fake_redis):
        with patch("flute.client.connect_redis", new_callable=AsyncMock,
                   return_value=fake_redis) as mock:
            c = SessionClient("redis://custom:1234/5")
            await c._ensure_connected()
            mock.assert_called_once_with("redis://custom:1234/5", decode_responses=True)

    @pytest.mark.asyncio
    async def test_env_url(self, fake_redis):
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://envhost:9999/1"}),
            patch("flute.client.connect_redis", new_callable=AsyncMock,
                  return_value=fake_redis) as mock,
        ):
            c = SessionClient()
            await c._ensure_connected()
            mock.assert_called_once_with("redis://envhost:9999/1", decode_responses=True)

    @pytest.mark.asyncio
    async def test_explicit_url_overrides_env(self, fake_redis):
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://envhost:9999/1"}),
            patch("flute.client.connect_redis", new_callable=AsyncMock,
                  return_value=fake_redis) as mock,
        ):
            c = SessionClient("redis://explicit:1234/0")
            await c._ensure_connected()
            mock.assert_called_once_with("redis://explicit:1234/0", decode_responses=True)


class TestSubmit:
    @pytest.mark.asyncio
    async def test_basic(self, client, fake_redis):
        sid = await client.submit("print('hi')", session_id="s1")
        assert sid == "s1"
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert payload["session_id"] == "s1"
        assert payload["code"] == "print('hi')"
        assert payload["worker_type"] == "python"
        assert payload["max_runtime_seconds"] == 30
        assert payload["max_memory_mb"] == 128
        assert payload["max_disk_mb"] == 50

    @pytest.mark.asyncio
    async def test_auto_id(self, client):
        sid = await client.submit("print(1)")
        assert len(sid) == 12

    @pytest.mark.asyncio
    async def test_custom_limits(self, client, fake_redis):
        await client.submit(
            "x", session_id="s2", max_runtime_seconds=5, max_memory_mb=64, max_disk_mb=20
        )
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert payload["max_runtime_seconds"] == 5
        assert payload["max_memory_mb"] == 64
        assert payload["max_disk_mb"] == 20

    @pytest.mark.asyncio
    async def test_with_input_files(self, client, fake_redis):
        await client.submit("x", session_id="s3", input_files={"data.csv": b"a,b,c"})
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert "input_files" in payload
        assert base64.b64decode(payload["input_files"]["data.csv"]) == b"a,b,c"

    @pytest.mark.asyncio
    async def test_no_input_files_omitted(self, client, fake_redis):
        await client.submit("x", session_id="s4")
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert "input_files" not in payload

    @pytest.mark.asyncio
    async def test_with_user_id(self, client, fake_redis):
        await client.submit("x", session_id="s6", user_id="alice")
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert payload["user_id"] == "alice"

    @pytest.mark.asyncio
    async def test_no_user_id_omitted(self, client, fake_redis):
        await client.submit("x", session_id="s7")
        payload = json.loads(fake_redis.lists["session:queue:python"][0])
        assert "user_id" not in payload

    @pytest.mark.asyncio
    async def test_clears_stale_keys(self, client, fake_redis):
        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            await fake_redis.set(f"session:s5:{suffix}", "stale")
        await fake_redis.hset("session:s5:output_files", "old.txt", "data")
        await fake_redis.rpush("session:s5:logs:lines", "old line")

        await client.submit("x", session_id="s5")

        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            assert await fake_redis.get(f"session:s5:{suffix}") is None
        assert await fake_redis.hgetall("session:s5:output_files") == {}
        assert await fake_redis.lrange("session:s5:logs:lines", 0, -1) == []

    @pytest.mark.asyncio
    async def test_worker_type(self, client, fake_redis):
        """Submit to a custom worker queue."""
        await client.submit(
            "x", session_id="s8", worker_type="custom",
            input_files={"photo.png": b"data"},
        )
        payload = json.loads(fake_redis.lists["session:queue:custom"][0])
        assert payload["worker_type"] == "custom"


class TestSubmitTask:
    @pytest.mark.asyncio
    async def test_basic(self, client, fake_redis):
        sid = await client.submit_task(
            '{"text": "hello"}',
            session_id="st1",
            worker_type="echo",
        )
        assert sid == "st1"
        payload = json.loads(fake_redis.lists["session:queue:echo"][0])
        assert payload["session_id"] == "st1"
        assert payload["task"] == '{"text": "hello"}'
        assert payload["worker_type"] == "echo"
        assert payload["max_runtime_seconds"] == 30

    @pytest.mark.asyncio
    async def test_auto_id(self, client):
        sid = await client.submit_task('{}', worker_type="test")
        assert len(sid) == 12

    @pytest.mark.asyncio
    async def test_with_user_id(self, client, fake_redis):
        await client.submit_task(
            '{}', session_id="st2", worker_type="test", user_id="alice"
        )
        payload = json.loads(fake_redis.lists["session:queue:test"][0])
        assert payload["user_id"] == "alice"

    @pytest.mark.asyncio
    async def test_with_input_files(self, client, fake_redis):
        await client.submit_task(
            '{}', session_id="st3", worker_type="test",
            input_files={"img.png": b"png-data"},
        )
        payload = json.loads(fake_redis.lists["session:queue:test"][0])
        assert base64.b64decode(payload["input_files"]["img.png"]) == b"png-data"

    @pytest.mark.asyncio
    async def test_custom_runtime(self, client, fake_redis):
        await client.submit_task(
            '{}', session_id="st4", worker_type="test", max_runtime_seconds=120,
        )
        payload = json.loads(fake_redis.lists["session:queue:test"][0])
        assert payload["max_runtime_seconds"] == 120

    @pytest.mark.asyncio
    async def test_clears_stale_keys(self, client, fake_redis):
        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            await fake_redis.set(f"session:st5:{suffix}", "stale")
        await fake_redis.hset("session:st5:output_files", "old.txt", "data")
        await fake_redis.rpush("session:st5:logs:lines", "old line")

        await client.submit_task('{}', session_id="st5", worker_type="test")

        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            assert await fake_redis.get(f"session:st5:{suffix}") is None
        assert await fake_redis.hgetall("session:st5:output_files") == {}
        assert await fake_redis.lrange("session:st5:logs:lines", 0, -1) == []


class TestWait:
    @pytest.mark.asyncio
    async def test_completed(self, client, fake_redis):
        await fake_redis.set("session:w1:status", "completed")
        await fake_redis.set("session:w1:logs", "output")
        await fake_redis.set("session:w1:exit_code", "0")
        result = await client.wait("w1", timeout=2, poll=0.05)
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_error(self, client, fake_redis):
        await fake_redis.set("session:w2:status", "error")
        result = await client.wait("w2", timeout=2, poll=0.05)
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_killed(self, client, fake_redis):
        await fake_redis.set("session:w3:status", "killed")
        result = await client.wait("w3", timeout=2, poll=0.05)
        assert result["status"] == "killed"

    @pytest.mark.asyncio
    async def test_quota_exceeded(self, client, fake_redis):
        await fake_redis.set("session:w6:status", "quota_exceeded")
        await fake_redis.set("session:w6:error", "quota exceeded")
        result = await client.wait("w6", timeout=2, poll=0.05)
        assert result["status"] == "quota_exceeded"

    @pytest.mark.asyncio
    async def test_timeout(self, client, fake_redis):
        await fake_redis.set("session:w4:status", "running")
        result = await client.wait("w4", timeout=0.3, poll=0.05)
        assert result["status"] == "timeout"

    @pytest.mark.asyncio
    async def test_no_status_times_out(self, client):
        result = await client.wait("w5", timeout=0.3, poll=0.05)
        assert result["status"] == "timeout"
        assert result["session_id"] == "w5"


class TestResult:
    @pytest.mark.asyncio
    async def test_full_result(self, client, fake_redis):
        await fake_redis.set("session:r1:status", "completed")
        await fake_redis.set("session:r1:logs", "hello\n")
        await fake_redis.set("session:r1:exit_code", "0")
        await fake_redis.set("session:r1:error", "")
        await fake_redis.hset(
            "session:r1:output_files", "out.txt", base64.b64encode(b"data").decode()
        )

        result = await client.result("r1")
        assert result["session_id"] == "r1"
        assert result["status"] == "completed"
        assert result["exit_code"] == 0
        assert result["logs"] == "hello\n"
        assert result["error"] == ""
        assert result["output_files"]["out.txt"] == b"data"
        assert result["response"] is None

    @pytest.mark.asyncio
    async def test_missing_fields(self, client):
        result = await client.result("r2")
        assert result["status"] == "unknown"
        assert result["logs"] == ""
        assert result["exit_code"] is None
        assert result["error"] == ""
        assert result["output_files"] == {}
        assert result["response"] is None

    @pytest.mark.asyncio
    async def test_multiple_output_files(self, client, fake_redis):
        await fake_redis.set("session:r3:status", "completed")
        await fake_redis.hset(
            "session:r3:output_files", "a.txt", base64.b64encode(b"aaa").decode()
        )
        await fake_redis.hset(
            "session:r3:output_files", "b.txt", base64.b64encode(b"bbb").decode()
        )

        result = await client.result("r3")
        assert len(result["output_files"]) == 2
        assert result["output_files"]["a.txt"] == b"aaa"
        assert result["output_files"]["b.txt"] == b"bbb"

    @pytest.mark.asyncio
    async def test_with_response(self, client, fake_redis):
        await fake_redis.set("session:r4:status", "completed")
        await fake_redis.set("session:r4:response", '{"result": "ok"}')
        result = await client.result("r4")
        assert result["response"] == '{"result": "ok"}'


class TestQuota:
    @pytest.mark.asyncio
    async def test_no_usage(self, client, fake_redis):
        q = await client.quota("fresh_user")
        assert q["user_id"] == "fresh_user"
        assert q["worker_type"] == "python"
        assert q["used_seconds"] == 0.0
        assert q["remaining_seconds"] == 600.0
        assert q["limit_seconds"] == 600
        assert q["resets_in_seconds"] == -1

    @pytest.mark.asyncio
    async def test_with_usage(self, client, fake_redis):
        await fake_redis.set("quota:alice:python", "120.5")
        await fake_redis.expire("quota:alice:python", 2400)
        q = await client.quota("alice")
        assert q["used_seconds"] == 120.5
        assert q["remaining_seconds"] == 479.5
        assert q["resets_in_seconds"] == 2400

    @pytest.mark.asyncio
    async def test_exceeded(self, client, fake_redis):
        await fake_redis.set("quota:bob:python", "700.0")
        await fake_redis.expire("quota:bob:python", 1800)
        q = await client.quota("bob")
        assert q["used_seconds"] == 700.0
        assert q["remaining_seconds"] == 0.0

    @pytest.mark.asyncio
    async def test_reads_server_config(self, client, fake_redis):
        await fake_redis.set("quota:__config:rules", "*:300/1800")
        q = await client.quota("new_user")
        assert q["limit_seconds"] == 300
        assert q["interval_seconds"] == 1800

    @pytest.mark.asyncio
    async def test_per_type_quota(self, client, fake_redis):
        """Custom worker type has different quota than python."""
        await fake_redis.set("quota:__config:rules", "*:600/3600 image:120/1800")
        await fake_redis.set("quota:alice:image", "50.0")
        await fake_redis.expire("quota:alice:image", 1200)
        q = await client.quota("alice", worker_type="image")
        assert q["worker_type"] == "image"
        assert q["used_seconds"] == 50.0
        assert q["limit_seconds"] == 120
        assert q["interval_seconds"] == 1800
        assert q["remaining_seconds"] == 70.0


class TestStreamLogs:
    @pytest.mark.asyncio
    async def test_receives_lines(self, client, fake_redis):
        await fake_redis.set("session:sl1:status", "running")

        async def publish_later():
            await asyncio.sleep(0.1)
            await fake_redis.publish("session:sl1:logs:stream", "hello\n")
            await fake_redis.publish("session:sl1:logs:stream", "world\n")
            await fake_redis.publish("session:sl1:logs:stream", "")  # EOF

        task = asyncio.create_task(publish_later())
        lines = [line async for line in client.stream_logs("sl1", timeout=5)]
        await task
        assert lines == ["hello\n", "world\n"]

    @pytest.mark.asyncio
    async def test_stops_on_session_complete(self, client, fake_redis):
        await fake_redis.set("session:sl2:status", "completed")
        lines = [line async for line in client.stream_logs("sl2", timeout=2)]
        assert lines == []

    @pytest.mark.asyncio
    async def test_timeout(self, client, fake_redis):
        await fake_redis.set("session:sl3:status", "running")
        import time as _time

        start = _time.monotonic()
        lines = [line async for line in client.stream_logs("sl3", timeout=0.5)]
        elapsed = _time.monotonic() - start
        assert lines == []
        assert elapsed < 3

    @pytest.mark.asyncio
    async def test_skips_non_message_types(self, client, fake_redis):
        """Non-message types (subscribe confirmations) are skipped."""
        await fake_redis.set("session:sl4:status", "running")

        original_pubsub = fake_redis.pubsub

        def patched_pubsub():
            ps = original_pubsub()
            original_subscribe = ps.subscribe

            async def subscribe_with_confirmation(channel):
                await original_subscribe(channel)
                ps._messages.insert(
                    0, {"type": "subscribe", "channel": channel, "data": 1}
                )

            ps.subscribe = subscribe_with_confirmation
            return ps

        fake_redis.pubsub = patched_pubsub

        async def publish_later():
            await asyncio.sleep(0.1)
            await fake_redis.publish("session:sl4:logs:stream", "ok\n")
            await fake_redis.publish("session:sl4:logs:stream", "")

        task = asyncio.create_task(publish_later())
        lines = [line async for line in client.stream_logs("sl4", timeout=5)]
        await task
        assert lines == ["ok\n"]


class TestWorkers:
    @pytest.mark.asyncio
    async def test_no_workers(self, client):
        workers = await client.workers()
        assert workers == []

    @pytest.mark.asyncio
    async def test_lists_workers(self, client, fake_redis):
        import json as _json

        now = time.time()
        await fake_redis.hset(
            "workers", "python:abc123",
            _json.dumps({
                "worker_type": "python",
                "last_heartbeat": now,
                "pid": 1234,
            }),
        )
        await fake_redis.hset(
            "workers", "image:def456",
            _json.dumps({
                "worker_type": "image",
                "last_heartbeat": now - 60,  # stale
                "pid": 5678,
            }),
        )

        workers = await client.workers()
        assert len(workers) == 2

        py_worker = next(w for w in workers if w["worker_type"] == "python")
        assert py_worker["alive"] is True
        assert py_worker["key"] == "python:abc123"

        img_worker = next(w for w in workers if w["worker_type"] == "image")
        assert img_worker["alive"] is False


class TestLogLines:
    @pytest.mark.asyncio
    async def test_basic(self, client, fake_redis):
        for line in ["hello\n", "world\n"]:
            await fake_redis.rpush("session:ll1:logs:lines", line)
        lines = await client.log_lines("ll1")
        assert lines == ["hello\n", "world\n"]

    @pytest.mark.asyncio
    async def test_empty(self, client):
        lines = await client.log_lines("ll-empty")
        assert lines == []

    @pytest.mark.asyncio
    async def test_with_range(self, client, fake_redis):
        for i in range(10):
            await fake_redis.rpush("session:ll2:logs:lines", f"line{i}\n")
        lines = await client.log_lines("ll2", start=-3, stop=-1)
        assert lines == ["line7\n", "line8\n", "line9\n"]

    @pytest.mark.asyncio
    async def test_partial_range(self, client, fake_redis):
        for i in range(5):
            await fake_redis.rpush("session:ll3:logs:lines", f"L{i}\n")
        lines = await client.log_lines("ll3", start=1, stop=3)
        assert lines == ["L1\n", "L2\n", "L3\n"]


class TestServices:
    @pytest.mark.asyncio
    async def test_no_services(self, client):
        services = await client.services()
        assert services == []

    @pytest.mark.asyncio
    async def test_lists_services(self, client, fake_redis):
        await fake_redis.hset("services", "python", json.dumps({
            "worker_type": "python",
            "type": "python-runner",
            "description": "General Python execution",
            "dependencies": ["numpy", "pandas", "matplotlib"],
        }))
        await fake_redis.hset("services", "echo", json.dumps({
            "worker_type": "echo",
            "type": "task-runner",
            "task_schema": {"type": "object", "properties": {"text": {"type": "string"}}},
            "response_schema": {"type": "object", "properties": {"echoed": {"type": "string"}}},
            "description": "Echo service",
            "dependencies": ["pydantic"],
        }))

        services = await client.services()
        assert len(services) == 2

        py = next(s for s in services if s["worker_type"] == "python")
        assert py["type"] == "python-runner"
        assert py["dependencies"] == ["numpy", "pandas", "matplotlib"]
        assert py["description"] == "General Python execution"

        echo = next(s for s in services if s["worker_type"] == "echo")
        assert echo["type"] == "task-runner"
        assert "text" in echo["task_schema"]["properties"]
        assert echo["description"] == "Echo service"
        assert echo["dependencies"] == ["pydantic"]


class TestSubmitService:
    @pytest.mark.asyncio
    async def test_basic(self, client, fake_redis):
        sid = await client.submit_service(
            worker_type="image-proc", session_id="ss1",
        )
        assert sid == "ss1"
        payload = json.loads(fake_redis.lists["session:queue:image-proc"][0])
        assert payload["session_id"] == "ss1"
        assert payload["worker_type"] == "image-proc"
        assert payload["params"] == {}
        assert payload["steps"] == []
        assert payload["max_runtime_seconds"] == 30
        assert payload["max_memory_mb"] == 512
        assert payload["max_disk_mb"] == 50

    @pytest.mark.asyncio
    async def test_auto_id(self, client):
        sid = await client.submit_service(worker_type="svc")
        assert len(sid) == 12

    @pytest.mark.asyncio
    async def test_with_files(self, client, fake_redis):
        await client.submit_service(
            worker_type="svc", session_id="ss2",
            input_files={"photo.png": b"png-data"},
        )
        payload = json.loads(fake_redis.lists["session:queue:svc"][0])
        assert "input_files" in payload
        assert base64.b64decode(payload["input_files"]["photo.png"]) == b"png-data"

    @pytest.mark.asyncio
    async def test_with_steps(self, client, fake_redis):
        await client.submit_service(
            worker_type="svc", session_id="ss3",
            steps=["resize", "watermark"],
        )
        payload = json.loads(fake_redis.lists["session:queue:svc"][0])
        assert payload["steps"] == ["resize", "watermark"]

    @pytest.mark.asyncio
    async def test_with_params(self, client, fake_redis):
        await client.submit_service(
            worker_type="svc", session_id="ss4",
            params={"width": 800, "quality": 90},
        )
        payload = json.loads(fake_redis.lists["session:queue:svc"][0])
        assert payload["params"] == {"width": 800, "quality": 90}

    @pytest.mark.asyncio
    async def test_clears_stale_keys(self, client, fake_redis):
        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            await fake_redis.set(f"session:ss5:{suffix}", "stale")
        await fake_redis.hset("session:ss5:output_files", "old.txt", "data")
        await fake_redis.rpush("session:ss5:logs:lines", "old line")

        await client.submit_service(worker_type="svc", session_id="ss5")

        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            assert await fake_redis.get(f"session:ss5:{suffix}") is None
        assert await fake_redis.hgetall("session:ss5:output_files") == {}
        assert await fake_redis.lrange("session:ss5:logs:lines", 0, -1) == []

    @pytest.mark.asyncio
    async def test_with_user_id(self, client, fake_redis):
        await client.submit_service(
            worker_type="svc", session_id="ss6", user_id="alice",
        )
        payload = json.loads(fake_redis.lists["session:queue:svc"][0])
        assert payload["user_id"] == "alice"


class TestPipeline:
    @pytest.mark.asyncio
    async def test_single_step(self, client, fake_redis):
        """Pipeline with one step works like submit_service + wait."""
        # Mock wait to return a completed result with output files
        async def mock_wait(sid, timeout=60):
            return {
                "session_id": sid,
                "status": "completed",
                "output_files": {
                    "result.png": b"output-data",
                },
            }

        client.wait = mock_wait

        result = await client.pipeline(
            [{"worker_type": "resize", "params": {"width": 100}}],
            input_files={"photo.png": b"input-data"},
        )
        assert result["status"] == "completed"
        assert result["output_files"]["result.png"] == b"output-data"

        # Verify the job was submitted to the correct queue
        payload = json.loads(fake_redis.lists["session:queue:resize"][0])
        assert payload["params"] == {"width": 100}
        assert base64.b64decode(payload["input_files"]["photo.png"]) == b"input-data"

    @pytest.mark.asyncio
    async def test_multi_step(self, client, fake_redis):
        """Output files from step 1 become input for step 2."""
        call_count = 0

        async def mock_wait(sid, timeout=60):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "session_id": sid,
                    "status": "completed",
                    "output_files": {
                        "intermediate.png": b"mid-data",
                    },
                }
            return {
                "session_id": sid,
                "status": "completed",
                "output_files": {
                    "final.png": b"final-data",
                },
            }

        client.wait = mock_wait

        result = await client.pipeline(
            [
                {"worker_type": "resize", "params": {"width": 800}},
                {"worker_type": "watermark", "steps": ["apply"]},
            ],
            input_files={"photo.png": b"original"},
        )
        assert result["status"] == "completed"
        assert result["output_files"]["final.png"] == b"final-data"

        # Step 2 should have received the output of step 1 as input
        payloads_step2 = fake_redis.lists["session:queue:watermark"]
        payload2 = json.loads(payloads_step2[0])
        assert payload2["steps"] == ["apply"]
        # The intermediate output file should be the input to step 2
        assert base64.b64decode(
            payload2["input_files"]["intermediate.png"]
        ) == b"mid-data"

    @pytest.mark.asyncio
    async def test_stops_on_error(self, client, fake_redis):
        """If any step fails, pipeline returns immediately."""
        async def mock_wait(sid, timeout=60):
            return {
                "session_id": sid,
                "status": "error",
                "error": "service crashed",
                "output_files": {},
            }

        client.wait = mock_wait

        result = await client.pipeline(
            [
                {"worker_type": "resize"},
                {"worker_type": "watermark"},
            ],
            input_files={"photo.png": b"data"},
        )
        assert result["status"] == "error"
        # Second step should NOT have been submitted
        assert "session:queue:watermark" not in fake_redis.lists


class TestProgress:
    @pytest.mark.asyncio
    async def test_with_progress(self, client, fake_redis):
        await fake_redis.set(
            "session:p1:progress",
            json.dumps({"progress": 0.5, "step": "resizing"}),
        )
        progress = await client.progress("p1")
        assert progress["progress"] == 0.5
        assert progress["step"] == "resizing"

    @pytest.mark.asyncio
    async def test_no_progress(self, client):
        progress = await client.progress("no-such-session")
        assert progress is None

    @pytest.mark.asyncio
    async def test_with_eta(self, client, fake_redis):
        await fake_redis.set(
            "session:p2:progress",
            json.dumps({"progress": 0.75, "step": "encoding", "eta_seconds": 12}),
        )
        progress = await client.progress("p2")
        assert progress["progress"] == 0.75
        assert progress["eta_seconds"] == 12


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_sets_key(self, client, fake_redis):
        result = await client.cancel("c1")
        assert result is True
        assert await fake_redis.get("session:c1:cancel") == "1"

    @pytest.mark.asyncio
    async def test_wait_recognises_cancelled(self, client, fake_redis):
        await fake_redis.set("session:c2:status", "cancelled")
        result = await client.wait("c2", timeout=2, poll=0.05)
        assert result["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_clear_session_removes_cancel_key(self, client, fake_redis):
        await fake_redis.set("session:c3:cancel", "1")
        await client._clear_session_keys("c3")
        assert await fake_redis.get("session:c3:cancel") is None
