"""Unit tests for flute.client.SessionClient."""

import asyncio
import base64
import json
import os
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
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert payload["session_id"] == "s1"
        assert payload["code"] == "print('hi')"
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
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert payload["max_runtime_seconds"] == 5
        assert payload["max_memory_mb"] == 64
        assert payload["max_disk_mb"] == 20

    @pytest.mark.asyncio
    async def test_with_input_files(self, client, fake_redis):
        await client.submit("x", session_id="s3", input_files={"data.csv": b"a,b,c"})
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert "input_files" in payload
        assert base64.b64decode(payload["input_files"]["data.csv"]) == b"a,b,c"

    @pytest.mark.asyncio
    async def test_no_input_files_omitted(self, client, fake_redis):
        await client.submit("x", session_id="s4")
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert "input_files" not in payload

    @pytest.mark.asyncio
    async def test_with_user_id(self, client, fake_redis):
        await client.submit("x", session_id="s6", user_id="alice")
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert payload["user_id"] == "alice"

    @pytest.mark.asyncio
    async def test_no_user_id_omitted(self, client, fake_redis):
        await client.submit("x", session_id="s7")
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert "user_id" not in payload

    @pytest.mark.asyncio
    async def test_clears_stale_keys(self, client, fake_redis):
        for suffix in ("status", "logs", "exit_code", "error", "workdir"):
            await fake_redis.set(f"session:s5:{suffix}", "stale")
        await fake_redis.hset("session:s5:output_files", "old.txt", "data")

        await client.submit("x", session_id="s5")

        for suffix in ("status", "logs", "exit_code", "error", "workdir"):
            assert await fake_redis.get(f"session:s5:{suffix}") is None
        assert await fake_redis.hgetall("session:s5:output_files") == {}


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

    @pytest.mark.asyncio
    async def test_missing_fields(self, client):
        result = await client.result("r2")
        assert result["status"] == "unknown"
        assert result["logs"] == ""
        assert result["exit_code"] is None
        assert result["error"] == ""
        assert result["output_files"] == {}

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


class TestQuota:
    @pytest.mark.asyncio
    async def test_no_usage(self, client, fake_redis):
        q = await client.quota("fresh_user")
        assert q["user_id"] == "fresh_user"
        assert q["used_seconds"] == 0.0
        assert q["remaining_seconds"] == 600.0
        assert q["limit_seconds"] == 600
        assert q["resets_in_seconds"] == -1

    @pytest.mark.asyncio
    async def test_with_usage(self, client, fake_redis):
        await fake_redis.set("quota:alice", "120.5")
        await fake_redis.expire("quota:alice", 2400)
        q = await client.quota("alice")
        assert q["used_seconds"] == 120.5
        assert q["remaining_seconds"] == 479.5
        assert q["resets_in_seconds"] == 2400

    @pytest.mark.asyncio
    async def test_exceeded(self, client, fake_redis):
        await fake_redis.set("quota:bob", "700.0")
        await fake_redis.expire("quota:bob", 1800)
        q = await client.quota("bob")
        assert q["used_seconds"] == 700.0
        assert q["remaining_seconds"] == 0.0

    @pytest.mark.asyncio
    async def test_reads_server_config(self, client, fake_redis):
        await fake_redis.set("quota:__config:limit", "300")
        await fake_redis.set("quota:__config:interval", "1800")
        q = await client.quota("new_user")
        assert q["limit_seconds"] == 300


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
        import time

        start = time.monotonic()
        lines = [line async for line in client.stream_logs("sl3", timeout=0.5)]
        elapsed = time.monotonic() - start
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
