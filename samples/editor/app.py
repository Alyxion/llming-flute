"""Flute Editor — minimal web IDE for sandboxed Python execution.

Usage:
    pip install aiohttp
    python samples/editor/app.py [--port 8200] [--redis redis://localhost:6399/0]

Environments are configured in ``environments.json`` (gitignored) next to this file::

    {
      "local": {"name": "Local Docker", "redis": "redis://localhost:6399/0"},
      "azure": {"name": "Azure AKS",    "redis": "rediss://:KEY@host:port,"}
    }
"""

import argparse
import base64
import json
import os
from pathlib import Path

from aiohttp import web

from flute import SessionClient

STATIC_DIR = Path(__file__).resolve().parent
VENDOR_DIR = STATIC_DIR / "vendor"
ENVS_FILE = STATIC_DIR / "environments.json"
client: SessionClient
current_env_key: str | None = None


def load_environments() -> dict:
    """Load environment definitions from environments.json."""
    if ENVS_FILE.exists():
        with open(ENVS_FILE) as f:
            return json.load(f)
    # Fallback: single local environment
    return {"local": {"name": "Local", "redis": "redis://localhost:6399/0"}}


async def connect_env(key: str) -> None:
    """Connect (or reconnect) the global client to the given environment."""
    global client, current_env_key
    envs = load_environments()
    if key not in envs:
        key = next(iter(envs))  # fallback to first
    redis_url = envs[key]["redis"]
    os.environ["REDIS_URL"] = redis_url
    client = SessionClient(redis_url)
    await client._ensure_connected()
    current_env_key = key


async def init_client(app):
    # If --redis was passed, use that directly (legacy mode)
    redis_override = os.environ.get("REDIS_URL")
    if redis_override:
        # Try to find a matching env key
        envs = load_environments()
        matched = None
        for key, cfg in envs.items():
            if cfg.get("redis") == redis_override:
                matched = key
                break
        if matched:
            await connect_env(matched)
        else:
            global client, current_env_key
            client = SessionClient(redis_override)
            await client._ensure_connected()
            current_env_key = "__cli__"
    else:
        envs = load_environments()
        await connect_env(next(iter(envs)))


routes = web.RouteTableDef()


@routes.get("/")
async def index(request):
    return web.FileResponse(STATIC_DIR / "index.html")


@routes.post("/api/run")
async def run(request):
    data = await request.json()
    params = data.get("params")
    files = data.get("files", {})
    max_memory = data.get("max_memory_mb", 512)
    max_runtime = data.get("max_runtime_seconds", 30)
    worker_type = data.get("worker_type", "python")

    # Decode attached files
    files_bytes = {name: base64.b64decode(b64) for name, b64 in files.items()}

    if data.get("code") is not None:
        # Python runner mode
        input_files = {}
        if params is not None:
            input_files["input.json"] = json.dumps(params, indent=2).encode()
        input_files.update(files_bytes)
        sid = await client.submit(
            data["code"],
            input_files=input_files or None,
            worker_type=worker_type,
            max_memory_mb=max_memory,
            max_runtime_seconds=max_runtime,
        )
    else:
        # Service mode (no code, just files + params + steps)
        sid = await client.submit_service(
            worker_type=worker_type,
            input_files=files_bytes or None,
            params=params or {},
            steps=data.get("steps", []),
            max_memory_mb=max_memory,
            max_runtime_seconds=max_runtime,
        )
    return web.json_response({"session_id": sid})


@routes.post("/api/cancel/{sid}")
async def cancel(request):
    sid = request.match_info["sid"]
    await client.cancel(sid)
    return web.json_response({"ok": True})


@routes.get("/api/status/{sid}")
async def status(request):
    sid = request.match_info["sid"]
    await client._ensure_connected()
    st = await client.r.get(f"session:{sid}:status") or "unknown"
    lines = await client.log_lines(sid)
    progress = await client.progress(sid)
    return web.json_response({"status": st, "log_lines": lines, "progress": progress})


@routes.get("/api/result/{sid}")
async def result(request):
    sid = request.match_info["sid"]
    r = await client.result(sid)
    output_files = {
        name: base64.b64encode(data).decode()
        for name, data in r.get("output_files", {}).items()
    }
    lines = await client.log_lines(sid)
    return web.json_response({
        "session_id": r["session_id"],
        "status": r["status"],
        "exit_code": r["exit_code"],
        "logs": r["logs"],
        "error": r["error"],
        "output_files": output_files,
        "log_lines": lines,
    })


@routes.get("/api/services")
async def services(request):
    svcs = await client.services()
    return web.json_response(svcs)


@routes.get("/api/samples")
async def samples(request):
    """Return aggregated sample index from all workers that provide samples."""
    svcs = await client.services()
    result = {}
    for svc in svcs:
        si = svc.get("samples")
        if si and "categories" in si:
            result[svc["worker_type"]] = si
    return web.json_response(result)


@routes.get("/api/sample/{worker_type}/{filename}")
async def sample_source(request):
    """Return the source code of a sample script."""
    wt = request.match_info["worker_type"]
    fn = request.match_info["filename"]
    # Sanitize filename
    if "/" in fn or "\\" in fn or fn.startswith("."):
        return web.json_response({"error": "invalid filename"}, status=400)
    code = await client.sample_source(wt, fn)
    if code is None:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({"code": code, "filename": fn, "worker_type": wt})


@routes.get("/api/env")
async def env_info(request):
    info = {}
    # Redis target (credentials stripped)
    redis_url = os.environ.get("REDIS_URL", "")
    if "@" in redis_url:
        info["redis"] = redis_url.split("@", 1)[1]
    elif redis_url:
        info["redis"] = redis_url
    # Worker count
    try:
        info["workers"] = len(await client.workers())
    except Exception:
        info["workers"] = 0
    info["current_env"] = current_env_key
    return web.json_response(info)


@routes.get("/api/environments")
async def environments(request):
    envs = load_environments()
    result = []
    for key, cfg in envs.items():
        redis_url = cfg.get("redis", "")
        # Strip credentials for display
        safe = redis_url.split("@")[-1] if "@" in redis_url else redis_url
        safe = safe.rstrip(",").split(":")[0] if safe else ""
        result.append({"key": key, "name": cfg.get("name", key), "host": safe})
    return web.json_response({"environments": result, "current": current_env_key})


@routes.post("/api/environment")
async def switch_environment(request):
    data = await request.json()
    key = data.get("key")
    envs = load_environments()
    if key not in envs:
        return web.json_response({"error": "Unknown environment"}, status=400)
    await connect_env(key)
    return web.json_response({"ok": True, "key": key})


def main():
    parser = argparse.ArgumentParser(description="Flute Editor")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--redis", default=None)
    args = parser.parse_args()

    if args.redis:
        os.environ["REDIS_URL"] = args.redis

    app = web.Application(client_max_size=50 * 1024 * 1024)  # 50 MB
    app.on_startup.append(init_client)
    app.add_routes(routes)
    app.router.add_static("/vendor", VENDOR_DIR)

    print(f"Flute Editor: http://localhost:{args.port}")
    web.run_app(app, port=args.port, print=None)


if __name__ == "__main__":
    main()
