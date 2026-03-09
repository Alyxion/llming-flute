"""Flute Editor — minimal web IDE for sandboxed Python execution.

Usage:
    pip install aiohttp
    python samples/editor/app.py [--port 8200] [--redis redis://localhost:6399/0]
"""

import argparse
import base64
import json
import os
from pathlib import Path

from aiohttp import web

from flute import SessionClient

STATIC_DIR = Path(__file__).parent
client: SessionClient


async def init_client(app):
    global client
    client = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))
    await client._ensure_connected()


routes = web.RouteTableDef()


@routes.get("/")
async def index(request):
    return web.FileResponse(STATIC_DIR / "index.html")


@routes.post("/api/run")
async def run(request):
    data = await request.json()
    code = data.get("code", "")
    params = data.get("params")
    files = data.get("files", {})
    max_memory = data.get("max_memory_mb", 512)
    max_runtime = data.get("max_runtime_seconds", 30)

    input_files = {}
    if params is not None:
        input_files["input.json"] = json.dumps(params, indent=2).encode()
    for name, b64data in files.items():
        input_files[name] = base64.b64decode(b64data)

    sid = await client.submit(
        code,
        input_files=input_files or None,
        max_memory_mb=max_memory,
        max_runtime_seconds=max_runtime,
    )
    return web.json_response({"session_id": sid})


@routes.get("/api/status/{sid}")
async def status(request):
    sid = request.match_info["sid"]
    await client._ensure_connected()
    st = await client.r.get(f"session:{sid}:status") or "unknown"
    lines = await client.log_lines(sid)
    return web.json_response({"status": st, "log_lines": lines})


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
    return web.json_response(info)


def main():
    parser = argparse.ArgumentParser(description="Flute Editor")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--redis", default=None)
    args = parser.parse_args()

    if args.redis:
        os.environ["REDIS_URL"] = args.redis

    app = web.Application()
    app.on_startup.append(init_client)
    app.add_routes(routes)

    print(f"Flute Editor: http://localhost:{args.port}")
    web.run_app(app, port=args.port, print=None)


if __name__ == "__main__":
    main()
