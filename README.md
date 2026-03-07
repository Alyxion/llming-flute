# Flute

Remote-controlled isolated Python execution with resource limits.

Flute runs untrusted Python code in locked-down Docker containers with strict memory, disk, and runtime limits. Jobs are submitted via a Redis queue, executed in isolated subprocesses, and results (stdout, files) are returned through Redis.

Built for scenarios where an LLM or web app needs a safe sandbox to execute user-provided Python code — think ChatGPT-style code execution.

## Architecture

```
Client  ──submit──►  Redis Queue  ──pop──►  Worker Container(s)
        ◄──result──                         │
                                            ├─ subprocess with rlimits
                                            ├─ psutil memory monitoring
                                            └─ disk usage polling
```

**Workers** run inside hardened Docker containers:
- `internal: true` network — no internet, no LAN egress
- `read_only: true` root filesystem, `tmpfs` workdirs (512 MB cap)
- `cap_drop: ALL`, `no-new-privileges`, non-root user
- `pids_limit: 128`, `mem_limit: 512m`, `cpus: 2.0`

**Per-session enforcement** (configurable per submission):
| Resource | Default | Mechanism |
|----------|---------|-----------|
| Runtime  | 30s     | Wall-clock timer + process tree kill |
| Memory   | 128 MB  | psutil RSS monitoring |
| Disk     | 50 MB   | `RLIMIT_FSIZE` + directory size polling |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.14+ with [Poetry](https://python-poetry.org/) (for development)

### Start the Stack

```bash
docker compose up -d
```

This starts Redis and 2 worker replicas (4 concurrent sessions each = 8 total slots).

Scale workers as needed:

```bash
docker compose up -d --scale worker=5
```

### Run Code

```python
from flute import SessionClient

c = SessionClient("redis://localhost:6399/0")

# Simple execution
sid = c.submit("print('Hello from the sandbox!')")
result = c.wait(sid)
print(result["logs"])  # "Hello from the sandbox!\n"

# With input/output files
sid = c.submit(
    code="open('out.txt', 'w').write(open('in.txt').read().upper())",
    input_files={"in.txt": b"hello world"},
)
result = c.wait(sid)
print(result["output_files"]["out.txt"])  # b"HELLO WORLD"

# Custom resource limits
sid = c.submit(
    code="import numpy as np; np.save('matrix.npy', np.random.randn(1000, 1000))",
    max_runtime_seconds=60,
    max_memory_mb=256,
    max_disk_mb=100,
)
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `MAX_CONCURRENT` | `4` | Max concurrent sessions per worker |
| `WORKDIR_BASE` | System temp dir | Base directory for session workdirs |

## Pre-installed Libraries

The worker image ships with a scientific Python stack:

`numpy` `pandas` `matplotlib` `scipy` `sympy` `pillow` `openpyxl` `pdfplumber`

## Development

```bash
# Install dependencies
poetry install

# Run unit tests (51 tests, 100% coverage)
poetry run pytest tests/test_client.py tests/test_server.py -v \
    --cov=flute --cov-report=term-missing

# Lint
poetry run ruff check llming_flute/ tests/

# Integration tests (requires running Docker stack)
REDIS_URL=redis://localhost:6399/0 poetry run pytest tests/test_integration.py -v
```

## Project Structure

```
llming_flute/flute/
├── __init__.py        # Package exports (SessionClient, __version__)
├── client.py          # SessionClient — submit, wait, result
└── server.py          # Worker server — run_session, serve, main

tests/
├── conftest.py        # FakeRedis mock & fixtures
├── test_client.py     # 18 client unit tests
├── test_server.py     # 33 server unit tests
└── test_integration.py # 22 end-to-end tests (Docker required)

Dockerfile             # Worker image (Python 3.14 + scientific stack)
docker-compose.yml     # Redis + worker orchestration
pyproject.toml         # Poetry config, ruff, pytest, coverage
```

## API Reference

### `SessionClient(redis_url=None)`

Connects to Redis. Falls back to `REDIS_URL` env var, then `redis://localhost:6379/0`.

### `.submit(code, *, session_id=None, max_runtime_seconds=30, max_memory_mb=128, max_disk_mb=50, input_files=None) -> str`

Enqueue a Python code string for execution. Returns `session_id`.

- **`input_files`**: `dict[str, bytes]` — files written into the working directory before execution.

### `.wait(session_id, timeout=60, poll=0.3) -> dict`

Block until the session completes, errors, is killed, or times out.

### `.result(session_id) -> dict`

Fetch session results:

```python
{
    "session_id": "abc123",
    "status": "completed",  # completed | error | killed | unknown
    "exit_code": 0,
    "logs": "stdout and stderr output...",
    "error": "",
    "output_files": {"result.csv": b"..."},  # files created during execution
}
```

## License

MIT — Copyright (c) 2026 Michael Ikemann
