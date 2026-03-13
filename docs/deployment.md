# Deployment

**Workers MUST always run inside Docker** — both locally and in production.
Running `python -m flute.server` directly on the host is only acceptable for
framework development and unit testing, never for executing untrusted code.
Without container isolation, user code runs as your OS user with full
filesystem and network access.

## Local Development (Docker Compose)

```bash
docker-compose up --build
```

This starts:
- **Redis** on `localhost:6399` (mapped from container port 6379)
- **2x Python workers** (general code execution)
- **1x Image processor** (service-runner with Pillow)

Workers are on an internal network with no internet access. Redis is bridged
to the host via the `control` network.

### Editor

The sample web IDE connects to the local Redis:

```bash
pip install aiohttp
python samples/editor/app.py --port 8200
```

Open `http://localhost:8200`. The editor auto-discovers workers from Redis.

### Environment Switching

The editor supports multiple Redis targets via `samples/editor/environments.json`
(gitignored):

```json
{
  "local": {"name": "Local Docker", "redis": "redis://localhost:6399/0"},
  "azure": {"name": "Azure AKS",    "redis": "rediss://:KEY@host:port,"}
}
```

Switch environments in the UI dropdown. The last-used environment is persisted
in IndexedDB.

## Docker Image

The `Dockerfile` builds a single image used by all worker types. The `WORKER_DIR`
env var determines which worker config to load.

```dockerfile
FROM python:3.14-slim-bookworm

# Core deps
RUN pip install --no-cache-dir psutil redis pydantic

# Worker-specific deps (read from each worker's pyproject.toml)
COPY workers/ /workers/
COPY scripts/install_worker_deps.py /tmp/install_worker_deps.py
RUN python /tmp/install_worker_deps.py

# Framework code + workers
COPY llming_flute/flute/ /app/flute/
COPY workers/ /app/workers/

USER flute
CMD ["python", "-u", "-m", "flute.server"]
```

`scripts/install_worker_deps.py` reads each worker's `pyproject.toml` and
installs the declared dependencies via pip.

### Building for Azure ACR

```bash
docker buildx build --platform linux/amd64 \
  -t YOUR_REGISTRY.azurecr.io/flute-worker:latest \
  --push .
```

## Azure AKS

### Prerequisites

- AKS cluster with ACR integration
- Azure Redis Enterprise (OSS Cluster mode, TLS)
- K8s namespace `flute`

### Manifests

```
k8s/
  namespace.yaml          # Namespace creation
  secret.yaml             # Redis connection string
  worker-python.yaml      # Python worker deployment
  worker-image-processor.yaml  # Image processor deployment
```

### Deploy

```bash
kubectl apply -f k8s/
```

### Azure Redis Enterprise Notes

- Use `rediss://` scheme (TLS) with port 10000
- **Trailing comma** in the URL signals cluster mode to `redis_conn.py`:
  `rediss://:KEY@host:10000,`
- Cluster mode returns internal IPs for slot redirects — `ssl_cert_reqs=NONE`
  is set automatically when a cluster URL with `rediss://` is detected

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `WORKER_TYPE` | `python` | Queue name to listen on (overridden by WORKER_DIR config) |
| `WORKER_DIR` | (empty) | Path to worker directory with `pyproject.toml` |
| `MAX_CONCURRENT` | `4` | Max parallel sessions per server process |
| `WORKDIR_BASE` | system temp | Base directory for session workdirs |
| `QUOTA_RULES` | `*:600/3600` | Per-type quota rules |
| `QUOTA_SECONDS` | `600` | Default quota limit (used if no rule matches) |
| `QUOTA_INTERVAL` | `3600` | Default quota window (used if no rule matches) |
| `SESSION_TTL` | `1800` | Redis key expiry for session data (seconds) |
| `MAX_LOG_LINES` | `500` | Max buffered log lines per session |

## Scaling

- **Horizontal scaling**: run multiple replicas of the same worker type.
  Each picks from the same Redis queue (FIFO, at-most-once via `BLPOP`).
- **Per-type scaling**: each worker type is an independent deployment.
  Scale image workers separately from Python workers.
- **Concurrency**: each server process handles `MAX_CONCURRENT` sessions
  simultaneously via asyncio + semaphore.
