"""Unit tests for flute.client.SessionClient."""

import base64
import json
import os
import threading
import time
from unittest.mock import patch

from flute.client import SessionClient


class TestInit:
    def test_default_url(self, fake_redis):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("REDIS_URL", None)
            with patch("flute.client.redis.Redis.from_url", return_value=fake_redis) as mock:
                SessionClient()
                mock.assert_called_once_with("redis://localhost:6379/0", decode_responses=True)

    def test_custom_url(self, fake_redis):
        with patch("flute.client.redis.Redis.from_url", return_value=fake_redis) as mock:
            SessionClient("redis://custom:1234/5")
            mock.assert_called_once_with("redis://custom:1234/5", decode_responses=True)

    def test_env_url(self, fake_redis):
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://envhost:9999/1"}),
            patch("flute.client.redis.Redis.from_url", return_value=fake_redis) as mock,
        ):
            SessionClient()
            mock.assert_called_once_with("redis://envhost:9999/1", decode_responses=True)

    def test_explicit_url_overrides_env(self, fake_redis):
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://envhost:9999/1"}),
            patch("flute.client.redis.Redis.from_url", return_value=fake_redis) as mock,
        ):
            SessionClient("redis://explicit:1234/0")
            mock.assert_called_once_with("redis://explicit:1234/0", decode_responses=True)


class TestSubmit:
    def test_basic(self, client, fake_redis):
        sid = client.submit("print('hi')", session_id="s1")
        assert sid == "s1"
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert payload["session_id"] == "s1"
        assert payload["code"] == "print('hi')"
        assert payload["max_runtime_seconds"] == 30
        assert payload["max_memory_mb"] == 128
        assert payload["max_disk_mb"] == 50

    def test_auto_id(self, client):
        sid = client.submit("print(1)")
        assert len(sid) == 12

    def test_custom_limits(self, client, fake_redis):
        client.submit("x", session_id="s2", max_runtime_seconds=5, max_memory_mb=64, max_disk_mb=20)
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert payload["max_runtime_seconds"] == 5
        assert payload["max_memory_mb"] == 64
        assert payload["max_disk_mb"] == 20

    def test_with_input_files(self, client, fake_redis):
        client.submit("x", session_id="s3", input_files={"data.csv": b"a,b,c"})
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert "input_files" in payload
        assert base64.b64decode(payload["input_files"]["data.csv"]) == b"a,b,c"

    def test_no_input_files_omitted(self, client, fake_redis):
        client.submit("x", session_id="s4")
        payload = json.loads(fake_redis.lists["session:queue"][0])
        assert "input_files" not in payload

    def test_clears_stale_keys(self, client, fake_redis):
        for suffix in ("status", "logs", "exit_code", "error", "workdir"):
            fake_redis.set(f"session:s5:{suffix}", "stale")
        fake_redis.hset("session:s5:output_files", "old.txt", "data")

        client.submit("x", session_id="s5")

        for suffix in ("status", "logs", "exit_code", "error", "workdir"):
            assert fake_redis.get(f"session:s5:{suffix}") is None
        assert fake_redis.hgetall("session:s5:output_files") == {}


class TestWait:
    def test_completed(self, client, fake_redis):
        fake_redis.set("session:w1:status", "completed")
        fake_redis.set("session:w1:logs", "output")
        fake_redis.set("session:w1:exit_code", "0")
        result = client.wait("w1", timeout=2, poll=0.05)
        assert result["status"] == "completed"

    def test_error(self, client, fake_redis):
        fake_redis.set("session:w2:status", "error")
        result = client.wait("w2", timeout=2, poll=0.05)
        assert result["status"] == "error"

    def test_killed(self, client, fake_redis):
        fake_redis.set("session:w3:status", "killed")
        result = client.wait("w3", timeout=2, poll=0.05)
        assert result["status"] == "killed"

    def test_timeout(self, client, fake_redis):
        fake_redis.set("session:w4:status", "running")
        result = client.wait("w4", timeout=0.3, poll=0.05)
        assert result["status"] == "timeout"

    def test_no_status_times_out(self, client):
        result = client.wait("w5", timeout=0.3, poll=0.05)
        assert result["status"] == "timeout"
        assert result["session_id"] == "w5"


class TestResult:
    def test_full_result(self, client, fake_redis):
        fake_redis.set("session:r1:status", "completed")
        fake_redis.set("session:r1:logs", "hello\n")
        fake_redis.set("session:r1:exit_code", "0")
        fake_redis.set("session:r1:error", "")
        fake_redis.hset(
            "session:r1:output_files", "out.txt", base64.b64encode(b"data").decode()
        )

        result = client.result("r1")
        assert result["session_id"] == "r1"
        assert result["status"] == "completed"
        assert result["exit_code"] == 0
        assert result["logs"] == "hello\n"
        assert result["error"] == ""
        assert result["output_files"]["out.txt"] == b"data"

    def test_missing_fields(self, client):
        result = client.result("r2")
        assert result["status"] == "unknown"
        assert result["logs"] == ""
        assert result["exit_code"] is None
        assert result["error"] == ""
        assert result["output_files"] == {}

    def test_multiple_output_files(self, client, fake_redis):
        fake_redis.set("session:r3:status", "completed")
        fake_redis.hset("session:r3:output_files", "a.txt", base64.b64encode(b"aaa").decode())
        fake_redis.hset("session:r3:output_files", "b.txt", base64.b64encode(b"bbb").decode())

        result = client.result("r3")
        assert len(result["output_files"]) == 2
        assert result["output_files"]["a.txt"] == b"aaa"
        assert result["output_files"]["b.txt"] == b"bbb"


class TestStreamLogs:
    def test_receives_lines(self, client, fake_redis):
        fake_redis.set("session:sl1:status", "running")

        def publish_later():
            time.sleep(0.1)
            fake_redis.publish("session:sl1:logs:stream", "hello\n")
            fake_redis.publish("session:sl1:logs:stream", "world\n")
            fake_redis.publish("session:sl1:logs:stream", "")  # EOF

        t = threading.Thread(target=publish_later, daemon=True)
        t.start()

        lines = list(client.stream_logs("sl1", timeout=5))
        assert lines == ["hello\n", "world\n"]

    def test_stops_on_session_complete(self, client, fake_redis):
        fake_redis.set("session:sl2:status", "completed")
        lines = list(client.stream_logs("sl2", timeout=2))
        assert lines == []

    def test_timeout(self, client, fake_redis):
        fake_redis.set("session:sl3:status", "running")
        start = time.monotonic()
        lines = list(client.stream_logs("sl3", timeout=0.5))
        elapsed = time.monotonic() - start
        assert lines == []
        assert elapsed < 3

    def test_skips_non_message_types(self, client, fake_redis):
        """Non-message types (subscribe confirmations) are skipped."""
        fake_redis.set("session:sl4:status", "running")

        original_pubsub = fake_redis.pubsub

        def patched_pubsub():
            ps = original_pubsub()
            original_subscribe = ps.subscribe

            def subscribe_with_confirmation(channel):
                original_subscribe(channel)
                # Inject a subscribe confirmation before any real messages
                ps._messages.insert(0, {"type": "subscribe", "channel": channel, "data": 1})

            ps.subscribe = subscribe_with_confirmation
            return ps

        fake_redis.pubsub = patched_pubsub

        def publish_later():
            time.sleep(0.1)
            fake_redis.publish("session:sl4:logs:stream", "ok\n")
            fake_redis.publish("session:sl4:logs:stream", "")

        t = threading.Thread(target=publish_later, daemon=True)
        t.start()

        lines = list(client.stream_logs("sl4", timeout=5))
        assert lines == ["ok\n"]
