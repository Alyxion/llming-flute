"""Client helper to submit sessions and retrieve results."""

import base64
import json
import os
import time
import uuid

import redis


class SessionClient:
    def __init__(self, redis_url=None):
        redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

    def submit(
        self,
        code: str,
        *,
        session_id: str | None = None,
        max_runtime_seconds: int = 30,
        max_memory_mb: int = 128,
        max_disk_mb: int = 50,
        input_files: dict[str, bytes] | None = None,
    ) -> str:
        """Submit a session. Returns session_id."""
        sid = session_id or uuid.uuid4().hex[:12]
        payload = {
            "session_id": sid,
            "code": code,
            "max_runtime_seconds": max_runtime_seconds,
            "max_memory_mb": max_memory_mb,
            "max_disk_mb": max_disk_mb,
        }
        if input_files:
            payload["input_files"] = {
                name: base64.b64encode(data).decode() for name, data in input_files.items()
            }
        for suffix in ("status", "logs", "exit_code", "error", "workdir"):
            self.r.delete(f"session:{sid}:{suffix}")
        self.r.delete(f"session:{sid}:output_files")

        self.r.rpush("session:queue", json.dumps(payload))
        return sid

    def wait(self, session_id: str, timeout: float = 60, poll: float = 0.3) -> dict:
        """Block until session finishes. Returns result dict."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.r.get(f"session:{session_id}:status")
            if status in ("completed", "error", "killed"):
                time.sleep(0.1)
                return self.result(session_id)
            time.sleep(poll)
        return {"session_id": session_id, "status": "timeout"}

    def result(self, session_id: str) -> dict:
        """Fetch the result of a session."""
        sid = session_id
        status = self.r.get(f"session:{sid}:status") or "unknown"
        logs = self.r.get(f"session:{sid}:logs") or ""
        exit_code = self.r.get(f"session:{sid}:exit_code")
        error = self.r.get(f"session:{sid}:error") or ""
        output_files_raw = self.r.hgetall(f"session:{sid}:output_files") or {}
        output_files = {
            name: base64.b64decode(data) for name, data in output_files_raw.items()
        }
        return {
            "session_id": sid,
            "status": status,
            "exit_code": int(exit_code) if exit_code else None,
            "logs": logs,
            "error": error,
            "output_files": output_files,
        }
