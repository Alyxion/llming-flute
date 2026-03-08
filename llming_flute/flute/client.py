"""Async client helper to submit sessions and retrieve results."""

import asyncio
import base64
import json
import os
import time
import uuid

from flute.quota_rules import get_quota_for_type, parse_quota_rules
from flute.redis_conn import connect_redis


class SessionClient:
    def __init__(self, redis_url=None):
        redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._redis_url = redis_url
        self.r = None

    async def _ensure_connected(self):
        if self.r is None:
            self.r = await connect_redis(self._redis_url, decode_responses=True)

    async def submit(
        self,
        code: str,
        *,
        session_id: str | None = None,
        user_id: str | None = None,
        worker_type: str = "python",
        max_runtime_seconds: int = 30,
        max_memory_mb: int = 128,
        max_disk_mb: int = 50,
        input_files: dict[str, bytes] | None = None,
    ) -> str:
        """Submit a Python runner session. Returns session_id."""
        await self._ensure_connected()
        sid = session_id or uuid.uuid4().hex[:12]
        payload = {
            "session_id": sid,
            "code": code,
            "worker_type": worker_type,
            "max_runtime_seconds": max_runtime_seconds,
            "max_memory_mb": max_memory_mb,
            "max_disk_mb": max_disk_mb,
        }
        if user_id:
            payload["user_id"] = user_id
        if input_files:
            payload["input_files"] = {
                name: base64.b64encode(data).decode() for name, data in input_files.items()
            }
        await self._clear_session_keys(sid)

        queue_key = f"session:queue:{worker_type}"
        await self.r.rpush(queue_key, json.dumps(payload))
        return sid

    async def submit_task(
        self,
        task_json: str,
        *,
        session_id: str | None = None,
        user_id: str | None = None,
        worker_type: str,
        max_runtime_seconds: int = 30,
        input_files: dict[str, bytes] | None = None,
    ) -> str:
        """Submit a Pydantic-typed task to a task runner worker. Returns session_id."""
        await self._ensure_connected()
        sid = session_id or uuid.uuid4().hex[:12]
        payload = {
            "session_id": sid,
            "task": task_json,
            "worker_type": worker_type,
            "max_runtime_seconds": max_runtime_seconds,
        }
        if user_id:
            payload["user_id"] = user_id
        if input_files:
            payload["input_files"] = {
                name: base64.b64encode(data).decode() for name, data in input_files.items()
            }
        await self._clear_session_keys(sid)

        queue_key = f"session:queue:{worker_type}"
        await self.r.rpush(queue_key, json.dumps(payload))
        return sid

    async def _clear_session_keys(self, sid: str):
        """Remove stale keys from a previous session with the same id."""
        for suffix in ("status", "logs", "exit_code", "error", "workdir", "response"):
            await self.r.delete(f"session:{sid}:{suffix}")
        await self.r.delete(f"session:{sid}:output_files")
        await self.r.delete(f"session:{sid}:logs:lines")

    async def wait(self, session_id: str, timeout: float = 60, poll: float = 0.3) -> dict:
        """Block until session finishes. Returns result dict."""
        await self._ensure_connected()
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            status = await self.r.get(f"session:{session_id}:status")
            if status in ("completed", "error", "killed", "quota_exceeded"):
                await asyncio.sleep(0.1)
                return await self.result(session_id)
            await asyncio.sleep(poll)
        return {"session_id": session_id, "status": "timeout"}

    async def stream_logs(self, session_id: str, timeout: float = 60):
        """Async generator yielding log lines via Redis pub/sub."""
        await self._ensure_connected()
        channel = f"session:{session_id}:logs:stream"
        pubsub = self.r.pubsub()
        await pubsub.subscribe(channel)
        try:
            deadline = asyncio.get_event_loop().time() + timeout
            while asyncio.get_event_loop().time() < deadline:
                msg = await pubsub.get_message(timeout=1)
                if msg is None:
                    status = await self.r.get(f"session:{session_id}:status")
                    if status in ("completed", "error", "killed", "quota_exceeded"):
                        break
                    continue
                if msg["type"] != "message":
                    continue
                data = msg["data"]
                if data == "":
                    break
                yield data
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    async def log_lines(self, session_id: str, start: int = 0, stop: int = -1) -> list[str]:
        """Fetch buffered log lines for a session.

        Lines are buffered in Redis up to the server's MAX_LOG_LINES limit.
        Returns a list of strings (each line includes its trailing newline).
        Use *start*/*stop* to fetch a slice (Redis LRANGE semantics).
        """
        await self._ensure_connected()
        return await self.r.lrange(f"session:{session_id}:logs:lines", start, stop)

    async def result(self, session_id: str) -> dict:
        """Fetch the result of a session.

        Returns a dict with keys:
            session_id:   str -- the session identifier
            status:       str -- "completed", "error", "killed", "quota_exceeded", or "unknown"
            exit_code:    int | None -- process exit code (None if not run)
            logs:         str -- captured stdout/stderr
            error:        str -- error description (resource limit, quota, signal, etc.)
            output_files: dict[str, bytes] -- files produced by the session
            response:     str | None -- JSON response from task runners
        """
        await self._ensure_connected()
        sid = session_id
        status = await self.r.get(f"session:{sid}:status") or "unknown"
        logs = await self.r.get(f"session:{sid}:logs") or ""
        exit_code = await self.r.get(f"session:{sid}:exit_code")
        error = await self.r.get(f"session:{sid}:error") or ""
        response = await self.r.get(f"session:{sid}:response")
        output_files_raw = await self.r.hgetall(f"session:{sid}:output_files") or {}
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
            "response": response,
        }

    async def quota(self, user_id: str, worker_type: str = "python") -> dict:
        """Query the current quota usage for a user and worker type.

        Returns a dict with keys:
            user_id:           str   -- the queried user
            worker_type:       str   -- the worker type
            used_seconds:      float -- seconds consumed in the current window
            limit_seconds:     int   -- the quota limit per window for this type
            interval_seconds:  int   -- the quota window duration
            remaining_seconds: float -- seconds remaining (0 if exceeded)
            resets_in_seconds: int   -- seconds until the quota window resets (-1 if no usage)
        """
        await self._ensure_connected()
        key = f"quota:{user_id}:{worker_type}"
        raw = await self.r.get(key)
        used = float(raw) if raw is not None else 0.0

        # Read quota rules from server config
        rules_str = await self.r.get("quota:__config:rules") or f"*:{600}/{3600}"
        rules = parse_quota_rules(rules_str)
        limit, interval = get_quota_for_type(rules, worker_type)

        ttl = await self.r.ttl(key) if raw is not None else -1
        return {
            "user_id": user_id,
            "worker_type": worker_type,
            "used_seconds": round(used, 2),
            "limit_seconds": limit,
            "interval_seconds": interval,
            "remaining_seconds": round(max(0.0, limit - used), 2),
            "resets_in_seconds": ttl if ttl >= 0 else -1,
        }

    async def workers(self) -> list[dict]:
        """List active workers across all types.

        Returns a list of dicts with keys:
            key:            str   -- "{worker_type}:{worker_id}"
            worker_type:    str   -- e.g. "python", "image"
            last_heartbeat: float -- unix timestamp of last heartbeat
            alive:          bool  -- True if heartbeat was within last 30s
            pid:            int   -- worker process ID
        """
        await self._ensure_connected()
        all_workers = await self.r.hgetall("workers")
        result = []
        now = time.time()
        for wkey, val in all_workers.items():
            info = json.loads(val)
            info["key"] = wkey
            info["alive"] = (now - info.get("last_heartbeat", 0)) < 30
            result.append(info)
        return result

    async def services(self) -> list[dict]:
        """Discover available worker services and their capabilities.

        Returns a list of dicts with keys:
            worker_type:     str  -- the service name
            type:            str  -- "python-runner" or "task-runner"
            task_schema:     dict | None -- Pydantic JSON schema for task input (task runners)
            response_schema: dict | None -- Pydantic JSON schema for response (task runners)
            description:     str  -- human-readable description
        """
        await self._ensure_connected()
        raw = await self.r.hgetall("services")
        result = []
        for val in raw.values():
            result.append(json.loads(val))
        return result
