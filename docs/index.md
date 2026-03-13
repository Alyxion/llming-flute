# Flute Documentation

Flute is a Redis-backed execution engine for running isolated Python workloads
with defined memory, disk, and runtime limits. It supports three execution
paradigms:

- **Python Runner** — arbitrary code in, logs + files out
- **Task Runner** — Pydantic-typed structured input/output
- **Service Runner** — long-lived subprocess for expensive model loading, files in/out with multi-step pipelines

## Guides

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | System design, component overview, data flow diagrams |
| [Workers](workers.md) | How to create Python, task, and service workers |
| [Redis Protocol](redis-protocol.md) | Complete key reference, payload formats, subprocess protocol |
| [Quotas](quotas.md) | Per-user per-type quota system |
| [Security](security.md) | Container hardening, process limits, code sandboxing |
| [Deployment](deployment.md) | Docker Compose, Azure AKS, environment variables |
| [Extending](extending.md) | Adding handlers, config fields, session keys, custom protocols |
| [Testing](testing.md) | Running tests, coverage requirements, test infrastructure |

## Quick Start

```bash
# Start Redis + workers
docker-compose up --build -d

# Start the web IDE
pip install aiohttp
python samples/editor/app.py --port 8200

# Open http://localhost:8200
```

## Using the Client

```python
from flute import SessionClient

client = SessionClient("redis://localhost:6399/0")

# Python runner
sid = await client.submit("print('hello')")
result = await client.wait(sid)
print(result["logs"])  # "hello\n"

# Service runner (image processing)
sid = await client.submit_service(
    worker_type="image-processor",
    input_files={"photo.jpg": open("photo.jpg", "rb").read()},
    steps=["grayscale", "resize"],
    params={"width": 256, "height": 256},
)
result = await client.wait(sid)
output_jpg = result["output_files"]["output.jpg"]

# Cancel a running session
await client.cancel(sid)

# Check quota
usage = await client.quota("alice", "python")

# Discover services
services = await client.services()
```

## Project Layout

```
llming_flute/flute/     Core framework (8 modules, 100% test coverage)
workers/                Worker definitions (pyproject.toml + handler code)
  python/               General Python execution
  image-processor/      Image processing service (Pillow)
samples/editor/         Web IDE (aiohttp + CodeMirror)
tests/                  Unit + integration tests
k8s/                    Kubernetes manifests
scripts/                Build helpers
docs/                   This documentation
```
