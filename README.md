# Flute

Remote-controlled isolated Python execution with resource limits.

Flute runs untrusted Python code in locked-down containers with strict memory, disk, and runtime limits. Jobs are submitted via a Redis queue, executed in isolated subprocesses, and results (stdout, files) are returned through Redis. Logs can be streamed in real time via Redis pub/sub.

Supports two execution paradigms:
- **Python Runner** — receives Python code + attachments, produces logs + output files
- **Task Runner** — receives a Pydantic-typed task, returns a Pydantic-typed response + output files

Built for scenarios where an LLM or web app needs a safe sandbox to execute user-provided Python code — think ChatGPT-style code execution — or structured task processing like image manipulation.

## Architecture

```
Client  ──submit──►  Redis Queue  ──pop──►  Worker Container(s)
        ◄──result──  (per type)            │
        ◄──stream──  Redis Pub/Sub  ◄───────┤ live stdout
                                            ├─ subprocess with rlimits
                                            ├─ psutil memory monitoring
                                            └─ disk usage polling

Client  ──services()──►  Redis "services" hash  ◄── workers register on startup
        (discover available workers, deps, Pydantic schemas)
```

**Workers** run inside hardened containers:
- Read-only root filesystem, `tmpfs` workdirs (512 MB cap)
- Drop all capabilities, no privilege escalation, non-root user
- Per-session memory, disk, and runtime limits
- Environment variables cleared before user code runs

**Per-session enforcement** (configurable per submission):
| Resource | Default | Mechanism |
|----------|---------|-----------|
| Runtime  | 30s     | Wall-clock timer + process tree kill |
| Memory   | 128 MB  | psutil RSS monitoring |
| Disk     | 50 MB   | `RLIMIT_FSIZE` + directory size polling |

## Defining Workers

Each worker lives in its own directory with a `pyproject.toml`:

### Python Runner

Receives arbitrary Python code and executes it in a subprocess.

```
workers/python/
└── pyproject.toml
```

```toml
[project]
name = "python"
description = "General-purpose Python execution"
dependencies = [
    "numpy",
    "pandas",
    "matplotlib",
    "scipy",
    "pillow",
]

[tool.flute.worker]
type = "python-runner"
description = "Python code execution with scientific libraries"
```

### Task Runner

Receives a Pydantic-typed task and returns a typed response. Define a handler class:

```
workers/image-processor/
├── pyproject.toml
└── handler.py
```

```toml
[project]
name = "image-processor"
dependencies = ["pillow>=10.0"]

[tool.flute.worker]
type = "task-runner"
handler = "handler:ImageProcessHandler"
description = "Process images: grayscale, resize, thumbnail"
```

```python
# handler.py
from pydantic import BaseModel
from flute.task_runner import TaskHandler

class ImageProcessTask(BaseModel):
    operation: str = "grayscale"
    width: int | None = None
    height: int | None = None

class ImageProcessResponse(BaseModel):
    operation: str
    output_filename: str
    output_width: int
    output_height: int

class ImageProcessHandler(TaskHandler):
    task_model = ImageProcessTask
    response_model = ImageProcessResponse

    async def execute(self, task, input_files):
        # process image...
        return ImageProcessResponse(...), {"output.png": output_bytes}
```

## Quick Start (Docker Compose)

### Prerequisites

- Docker & Docker Compose
- Python 3.14+ with [Poetry](https://python-poetry.org/) (for development)

### Start the Stack

```bash
docker compose up -d
```

This starts Redis, a Python worker (2 replicas), and an image processor worker.

### Run Code

```python
import asyncio
from flute import SessionClient

async def main():
    client = SessionClient("redis://localhost:6399/0")

    # Discover available services
    services = await client.services()
    for s in services:
        print(f"{s['worker_type']} ({s['type']})")
        print(f"  deps: {s['dependencies']}")
        print(f"  desc: {s['description']}")

    # Submit Python code
    sid = await client.submit("print('Hello from the sandbox!')")
    result = await client.wait(sid)
    print(result["logs"])  # "Hello from the sandbox!\n"

    # Submit with input/output files
    sid = await client.submit(
        code="open('out.txt', 'w').write(open('in.txt').read().upper())",
        input_files={"in.txt": b"hello world"},
    )
    result = await client.wait(sid)
    print(result["output_files"]["out.txt"])  # b"HELLO WORLD"

    # Submit a typed task
    import json
    sid = await client.submit_task(
        json.dumps({"operation": "grayscale"}),
        worker_type="image-processor",
        input_files={"photo.png": photo_bytes},
    )
    result = await client.wait(sid)
    response = json.loads(result["response"])
    print(response)  # {"operation": "grayscale", "output_filename": "output.png", ...}

asyncio.run(main())
```

### Live Log Streaming

```python
sid = await client.submit(
    "import time\nfor i in range(5):\n    print(f'Step {i}')\n    time.sleep(1)"
)
# Real-time via pub/sub
async for line in client.stream_logs(sid, timeout=30):
    print(line, end="")

# Or fetch buffered lines after the fact (last 500 lines kept per session)
result = await client.wait(sid)
lines = await client.log_lines(sid)
```

## Deployment

### Docker Compose (Local / Single Server)

```bash
docker compose up -d
```

The `docker-compose.yml` uses YAML anchors for shared worker config. Scale workers:

```bash
docker compose up -d --scale worker-python=4
```

### Azure Kubernetes Service (AKS)

For production deployments on Azure, Flute includes Kubernetes manifests in `k8s/`.

#### 1. Create the Infrastructure

```bash
# Create a resource group (or use an existing one)
az group create -n mygroup -l germanywestcentral

# Create a VNet with a subnet for AKS
az network vnet create -g mygroup -n myvnet --address-prefixes 10.0.0.0/16
az network vnet subnet create -g mygroup --vnet-name myvnet -n aks-subnet \
    --address-prefixes 10.0.4.0/22

# Create an ACR (or use an existing one)
az acr create -g mygroup -n myregistry --sku Basic

# Create the AKS cluster
AKS_SUBNET=$(az network vnet subnet show -g mygroup --vnet-name myvnet \
    -n aks-subnet --query id -o tsv)

az aks create -g mygroup -n flute-aks \
    --node-count 1 \
    --node-vm-size Standard_D4s_v3 \
    --vnet-subnet-id "$AKS_SUBNET" \
    --network-plugin azure \
    --generate-ssh-keys \
    --attach-acr myregistry
```

The VNet integration ensures the AKS pods can reach your Redis instance (whether Azure Cache for Redis, Redis Enterprise, or a VM-hosted Redis) over private networking.

#### 2. Build and Push the Image

```bash
az acr login -n myregistry

# Build for amd64 (AKS runs Linux amd64 nodes)
docker buildx build --platform linux/amd64 \
    -t myregistry.azurecr.io/flute-worker:latest --push .
```

The Dockerfile automatically installs dependencies from all `workers/*/pyproject.toml` files via `scripts/install_worker_deps.py`.

#### 3. Configure Kubernetes Manifests

Edit the manifests in `k8s/`:

**`k8s/secret.yaml`** — set your Redis connection URL:
```yaml
stringData:
  REDIS_URL: "rediss://:YOUR_KEY@your-redis.region.redis.azure.net:6380"
```

For Redis Cluster (e.g. Azure Redis Enterprise with OSS Cluster policy), append a trailing comma:
```yaml
  REDIS_URL: "rediss://:YOUR_KEY@your-redis.region.redis.azure.net:10000,"
```

**`k8s/worker-python.yaml`** and **`k8s/worker-image-processor.yaml`** — update the image reference:
```yaml
image: myregistry.azurecr.io/flute-worker:latest
```

#### 4. Deploy

```bash
az aks get-credentials -g mygroup -n flute-aks

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/worker-python.yaml
kubectl apply -f k8s/worker-image-processor.yaml
```

Verify:
```bash
kubectl get pods -n flute
kubectl logs -n flute deployment/worker-python
```

#### 5. Scale

```bash
# More replicas (each runs MAX_CONCURRENT=4 parallel sessions)
kubectl scale deployment/worker-python -n flute --replicas=3

# Or add more nodes to the cluster
az aks scale -g mygroup -n flute-aks --node-count 3
```

#### Node Sizing Guide

| VM Size | vCPU | RAM | Good for |
|---------|------|-----|----------|
| Standard_B2s | 2 | 4 GB | Light workloads, testing |
| Standard_D4s_v3 | 4 | 16 GB | Mixed workloads, ML inference |
| Standard_D8s_v3 | 8 | 32 GB | Heavy compute, multiple workers |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL (trailing comma = cluster) |
| `WORKER_DIR` | *(empty)* | Path to worker directory with pyproject.toml |
| `WORKER_TYPE` | `python` | Fallback worker type when WORKER_DIR is not set |
| `MAX_CONCURRENT` | `4` | Max concurrent sessions per worker process |
| `WORKDIR_BASE` | System temp dir | Base directory for session workdirs |
| `QUOTA_RULES` | `*:600/3600` | Per-type quota rules (format: `type:limit/interval`) |
| `SESSION_TTL` | `1800` | Auto-delete all session keys after this many seconds (30 min) |
| `MAX_LOG_LINES` | `500` | Max log lines buffered per session in Redis |

## Service Discovery

Workers register themselves in a Redis `services` hash on startup. Clients can discover available workers, their types, dependencies, and (for task runners) Pydantic schemas:

```python
services = await client.services()
# [
#   {
#     "worker_type": "python",
#     "type": "python-runner",
#     "description": "Python code execution with scientific libraries",
#     "dependencies": ["numpy", "pandas", "matplotlib", ...],
#   },
#   {
#     "worker_type": "image-processor",
#     "type": "task-runner",
#     "description": "Process images: grayscale, resize, thumbnail",
#     "dependencies": ["pillow>=10.0"],
#     "task_schema": { ... },       # JSON Schema for input
#     "response_schema": { ... },   # JSON Schema for output
#   }
# ]
```

## API Reference

### `SessionClient(redis_url=None)`

Connects to Redis. Falls back to `REDIS_URL` env var, then `redis://localhost:6379/0`.

### `.submit(code, *, session_id=None, user_id=None, worker_type="python", max_runtime_seconds=30, max_memory_mb=128, max_disk_mb=50, input_files=None) -> str`

Enqueue Python code for execution. Returns `session_id`.

### `.submit_task(task_json, *, session_id=None, user_id=None, worker_type, max_runtime_seconds=30, input_files=None) -> str`

Submit a Pydantic-typed task to a task runner worker. `task_json` is a JSON string matching the worker's `task_schema`. Returns `session_id`.

### `.stream_logs(session_id, timeout=60) -> AsyncGenerator[str]`

Yield log lines as they arrive via Redis pub/sub. Subscribe right after `submit()` for complete output.

### `.log_lines(session_id, start=0, stop=-1) -> list[str]`

Fetch buffered log lines from Redis. Up to `MAX_LOG_LINES` (default 500) most recent lines are kept per session. Unlike `stream_logs()`, this works even if you weren't subscribed during execution — useful for retrieving output after the fact.

### `.wait(session_id, timeout=60, poll=0.3) -> dict`

Block until the session completes, errors, is killed, or times out.

### `.result(session_id) -> dict`

Fetch session results:

```python
{
    "session_id": "abc123",
    "status": "completed",   # completed | error | killed | quota_exceeded | unknown
    "exit_code": 0,
    "logs": "stdout and stderr output...",
    "error": "",
    "output_files": {"result.csv": b"..."},
    "response": '{"key": "value"}',  # JSON string (task runners only, None for python runners)
}
```

### `.services() -> list[dict]`

Discover available worker services and their capabilities.

### `.workers() -> list[dict]`

List active worker instances with heartbeat status.

### `.quota(user_id, worker_type="python") -> dict`

Query per-user quota usage for a worker type.

## Development

```bash
# Install dependencies
poetry install

# Run unit tests (173 tests, 100% coverage)
poetry run pytest tests/ --ignore=tests/test_integration.py -v \
    --cov=flute --cov-report=term-missing

# Lint
poetry run ruff check llming_flute/ tests/ workers/ scripts/

# Integration tests (requires running Docker stack)
REDIS_URL=redis://localhost:6399/0 poetry run pytest tests/test_integration.py -v
```

## Project Structure

```
llming_flute/flute/
├── __init__.py          # Package exports
├── client.py            # SessionClient — submit, submit_task, wait, stream_logs, result, services
├── server.py            # Worker server — create_handler, run_session, serve, _register_service
├── handlers.py          # WorkerHandler, PythonHandler, TaskRunnerHandler, HANDLER_REGISTRY
├── task_runner.py       # TaskHandler base class for Pydantic-typed workers
├── worker_config.py     # Load worker config from pyproject.toml
├── quota_rules.py       # Per-type quota rule parsing
└── redis_conn.py        # Redis/RedisCluster connection factory

workers/
├── python/
│   └── pyproject.toml   # Python runner: numpy, pandas, matplotlib, ...
└── image-processor/
    ├── pyproject.toml   # Task runner: pillow
    └── handler.py       # ImageProcessHandler

k8s/
├── namespace.yaml       # flute namespace
├── secret.yaml          # Redis connection URL
├── worker-python.yaml   # Python runner deployment
└── worker-image-processor.yaml  # Image processor deployment

scripts/
└── install_worker_deps.py  # Installs deps from all workers/ at Docker build time

Dockerfile               # Multi-worker image with dynamic dependency installation
docker-compose.yml       # Local dev: Redis + workers with security hardening
```

## License

MIT — Copyright (c) 2026 Michael Ikemann
