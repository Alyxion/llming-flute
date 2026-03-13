# Architecture

Flute is a Redis-backed execution engine for running isolated Python workloads.
A client pushes a job to a per-type Redis queue; a server process picks it up,
runs it inside an ephemeral workdir with resource limits, and stores the result
back in Redis.

## Design Principles

- **One queue per worker type.** Each worker type (`python`, `image-processor`, ...)
  listens on its own FIFO queue `session:queue:{worker_type}`. This keeps routing
  explicit and makes it trivial to scale a single type independently.

- **Handlers own execution strategy.** The server loop is generic (dequeue, create
  workdir, call handler, store result). All execution logic lives in a
  `WorkerHandler` subclass.

- **Redis is the only shared state.** No filesystem, database, or message broker
  beyond Redis. This makes the system easy to operate and reason about.

- **Everything expires.** Session keys expire after `SESSION_TTL` (default 30 min),
  infrastructure keys after `INFRA_TTL` (120s, refreshed by heartbeat). Nothing
  accumulates.

## Component Overview

```
                  +-----------+
                  |  Client   |  SessionClient
                  +-----+-----+
                        |
                        | rpush / get / publish
                        v
                  +-----------+
                  |   Redis   |  queues, session keys, services hash
                  +-----+-----+
                        |
                        | blpop
                        v
                  +-----------+
                  |  Server   |  serve() loop
                  +-----+-----+
                        |
            +-----------+-----------+
            |           |           |
       Python      TaskRunner   Service
       Handler     Handler      Handler
            |           |           |
        subprocess  in-process  long-lived
        per session per session subprocess
```

### Client (`client.py`)

Async API for submitting jobs and retrieving results.

| Method | Purpose |
|--------|---------|
| `submit()` | Queue a Python code snippet |
| `submit_task()` | Queue a Pydantic-typed task |
| `submit_service()` | Queue a service job (files + params + steps) |
| `pipeline()` | Chain multiple service steps (output N -> input N+1) |
| `cancel()` | Request cancellation of a running session |
| `wait()` | Block until session finishes |
| `stream_logs()` | Async generator for live log lines (pub/sub) |
| `log_lines()` | Fetch buffered log lines (works after completion) |
| `result()` | Fetch complete result dict |
| `quota()` | Check quota usage for a user + worker type |
| `workers()` | List active workers |
| `services()` | Discover available services and their schemas |

### Server (`server.py`)

The main async loop. One server process handles one worker type.

1. `create_handler()` reads `WORKER_DIR/pyproject.toml` and instantiates the
   right handler.
2. `serve()` starts a heartbeat task, then loops: acquire semaphore slot,
   `BLPOP` from queue, spawn `run_session()` as asyncio task.
3. `run_session()` creates a workdir, writes input files, calls
   `handler.execute()`, collects output files, stores everything in Redis,
   charges quota.

### Handlers (`handlers.py`)

Three handler types, one base class:

| Handler | Worker Type | How It Runs |
|---------|------------|-------------|
| `PythonHandler` | `python-runner` | Spawns a subprocess per session. Code is sandboxed via a wrapper that clears env vars. |
| `TaskRunnerHandler` | `task-runner` | In-process. Deserializes a Pydantic model, calls `TaskHandler.execute()`, serializes response. |
| `ServiceHandler` | `service-runner` | Starts a subprocess once, sends jobs via stdin JSON, reads responses from stdout. |

### Service Base Class (`service.py`)

For workers that need to load expensive resources (ML models, large datasets) once
and reuse them across requests. Subclass `Service`, override `setup()` and `process()`.

The `Job` object passed to `process()` provides:
- `read_file()` / `write_file()` / `input_files()` — file I/O
- `progress(value, step)` — progress reporting with automatic ETA
- `check_quota()` — remaining budget in seconds
- `elapsed` — wall-clock time since job start

### Worker Config (`worker_config.py`)

Each worker is a directory with a `pyproject.toml`:

```toml
[project]
name = "image-processor"
dependencies = ["pillow>=10.0"]

[tool.flute.worker]
type = "service-runner"           # python-runner | task-runner | service-runner
handler = "service:ImageService"  # module:ClassName
description = "Image processing service"
operations = ["grayscale", "resize"]
accept_files = ["image/*"]        # MIME patterns for the editor UI
```

## Data Flow

### Python Runner Session

```
Client                     Redis                      Server
  |                          |                          |
  |-- rpush(queue, spec) --> |                          |
  |                          | <-- blpop(queue) --------|
  |                          |                          |-- mkdtemp
  |                          |                          |-- write code + inputs
  |                          |                          |-- subprocess(python -u runner.py)
  |                          | <-- publish(logs) -------|    |-- monitor RSS, disk, time
  |-- subscribe(logs) -----> |                          |    |-- check cancel key
  |<-- log lines ----------- |                          |
  |                          | <-- set(status) ---------|-- collect output files
  |-- get(result) ---------->|                          |-- charge quota
  |<-- result --------------|                          |-- rmtree(workdir)
```

### Service Runner Session

```
Server                   Service Process
  |                          |
  |-- start subprocess ----->|
  |                          |-- setup() (load models)
  |<-- {"type":"ready"} -----|
  |                          |
  |-- {"type":"job",...} --->|
  |                          |-- process(job)
  |<-- {"type":"log",...} ---|    |-- read/write files
  |<-- {"type":"progress"} -|    |-- report progress
  |<-- {"type":"done",...} --|
  |                          |   (stays alive for next job)
```

## Redis Key Layout

See [redis-protocol.md](redis-protocol.md) for the complete key reference.

## Security Model

See [security.md](security.md) for container hardening and execution sandboxing.
