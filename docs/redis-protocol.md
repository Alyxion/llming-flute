# Redis Protocol

All communication between client, server, and workers goes through Redis.
This document describes every key, its type, semantics, and lifetime.

## Key Namespaces

### Session Keys

Created per session, auto-expire after `SESSION_TTL` (default 1800s / 30 min).

| Key | Type | Description |
|-----|------|-------------|
| `session:{sid}:status` | string | `"queued"`, `"running"`, `"completed"`, `"error"`, `"killed"`, `"cancelled"`, `"quota_exceeded"` |
| `session:{sid}:logs` | string | Complete stdout/stderr buffer (set on completion) |
| `session:{sid}:logs:lines` | list | Buffered log lines (capped to `MAX_LOG_LINES`, default 500). Survives session completion for post-hoc retrieval. |
| `session:{sid}:logs:stream` | pub/sub channel | Live log line streaming. Empty string `""` signals end-of-stream. |
| `session:{sid}:exit_code` | string | Process exit code (absent for service/task runners that don't spawn a subprocess) |
| `session:{sid}:error` | string | Error description (resource limit, quota, signal, cancellation) |
| `session:{sid}:workdir` | string | Path to ephemeral workdir (informational; cleaned up on completion) |
| `session:{sid}:output_files` | hash | `{filename: base64_encoded_bytes}` — files produced by the session |
| `session:{sid}:response` | string | JSON response from task/service runners |
| `session:{sid}:cancel` | string | Set to `"1"` by `client.cancel()`. Checked every ~2s by handlers. Expires after 120s. |
| `session:{sid}:progress` | string | JSON progress object `{"progress": 0.5, "step": "resize", "eta_seconds": 2.3}`. Overwritten per update. |

### Queue Keys

| Key | Type | Description |
|-----|------|-------------|
| `session:queue:{worker_type}` | list | FIFO queue of JSON payloads. One per worker type. |

### Infrastructure Keys

Expire after `INFRA_TTL` (120s), refreshed every ~10s by the worker heartbeat.

| Key | Type | Description |
|-----|------|-------------|
| `workers` | hash | `{"{worker_type}:{worker_id}": JSON}` — active worker processes |
| `services` | hash | `{worker_type: JSON}` — service capabilities, schemas, operations |
| `quota:__config:rules` | string | Server's `QUOTA_RULES` string (published so clients can read limits) |

Worker heartbeat JSON:
```json
{"worker_type": "python", "last_heartbeat": 1710000000.0, "pid": 12345}
```

Service registration JSON:
```json
{
  "worker_type": "image-processor",
  "type": "service-runner",
  "description": "Image processing service",
  "dependencies": ["pillow>=10.0"],
  "operations": ["grayscale", "resize"],
  "accept_files": ["image/*"]
}
```

For task runners, the registration includes Pydantic JSON schemas:
```json
{
  "worker_type": "my-task-worker",
  "type": "task-runner",
  "description": "...",
  "task_schema": { ... },
  "response_schema": { ... }
}
```

### Quota Keys

| Key | Type | Description |
|-----|------|-------------|
| `quota:{user_id}:{worker_type}` | string (float) | Accumulated seconds in the current window. Incremented atomically via `INCRBYFLOAT`. Expires after `quota_interval`. |

### Service Performance Keys

| Key | Type | Description |
|-----|------|-------------|
| `service:{worker_type}:perf` | string | JSON `{"step_name": avg_seconds, ...}`. Updated after each completed service job. |

## Payload Formats

### Python Runner Submission

```json
{
  "session_id": "abc123def456",
  "code": "print('hello')",
  "worker_type": "python",
  "max_runtime_seconds": 30,
  "max_memory_mb": 128,
  "max_disk_mb": 50,
  "user_id": "alice",
  "input_files": {"data.csv": "<base64>"}
}
```

### Task Runner Submission

```json
{
  "session_id": "xyz789",
  "task": "{\"text\": \"hello\"}",
  "worker_type": "my-task-worker",
  "max_runtime_seconds": 30,
  "user_id": "alice",
  "input_files": {"photo.jpg": "<base64>"}
}
```

### Service Runner Submission

```json
{
  "session_id": "srv001",
  "worker_type": "image-processor",
  "params": {"width": 256, "height": 256},
  "steps": ["grayscale", "resize"],
  "max_runtime_seconds": 30,
  "max_memory_mb": 512,
  "max_disk_mb": 50,
  "user_id": "alice",
  "input_files": {"photo.jpg": "<base64>"}
}
```

## Service Subprocess Protocol

Communication between `ServiceHandler` and the service subprocess uses
newline-delimited JSON on stdin (handler -> service) and stdout (service -> handler).

### Messages: Handler -> Service (stdin)

```json
{"type": "job", "job_id": "sid123", "workdir": "/tmp/session_sid123_xyz",
 "params": {"width": 256}, "steps": ["grayscale", "resize"],
 "quota_budget_seconds": 3600, "max_runtime_seconds": 30}
```

### Messages: Service -> Handler (stdout)

**Ready** (sent once after `setup()` completes):
```json
{"type": "ready"}
```

**Log line** (from `print()` or `job.progress()`):
```json
{"type": "log", "job_id": "sid123", "text": "Processing step 1...\n"}
```

**Progress** (from `job.progress()`):
```json
{"type": "progress", "job_id": "sid123", "value": 0.5,
 "step": "resize", "eta_seconds": 2.3}
```

**Done** (job completed successfully):
```json
{"type": "done", "job_id": "sid123", "elapsed": 5.123,
 "perf": {"grayscale": 1.2, "resize": 3.9}}
```

**Error** (job failed):
```json
{"type": "error", "job_id": "sid123",
 "error": "No image file found", "elapsed": 0.05}
```

## Terminal Status Values

A session is considered finished when its status is one of:

| Status | Meaning |
|--------|---------|
| `completed` | Finished successfully (exit code 0 or service "done") |
| `error` | Failed (non-zero exit, exception, service "error") |
| `killed` | Killed by resource limit (time, memory, disk) or signal |
| `cancelled` | Cancelled by user via `cancel()` |
| `quota_exceeded` | Rejected before execution because quota was exhausted |
