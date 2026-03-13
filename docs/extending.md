# Extending Flute

Flute is designed to stay flexible. Each layer has a clear extension point.

## Adding a New Worker Type

The most common extension. See [workers.md](workers.md) for the full guide.
In short:

1. Create `workers/my-worker/pyproject.toml` with a `[tool.flute.worker]` section
2. Implement a handler class (Service subclass or TaskHandler subclass)
3. Add to docker-compose / k8s
4. The worker self-registers in Redis on startup — no central config to update

## Adding a New Handler Type

If the three built-in handler types (Python runner, task runner, service runner)
don't fit your needs, you can create a new one.

### Option A: Register via Decorator

```python
from flute.handlers import WorkerHandler, HandlerResult, register_handler

@register_handler
class MyCustomHandler(WorkerHandler):
    worker_type = "my-custom"

    async def execute(self, spec, workdir, rconn, sid, channel):
        # Your execution logic here
        return HandlerResult(status="completed", exit_code=0, logs="done\n")
```

The `@register_handler` decorator adds it to `HANDLER_REGISTRY`, so
`create_handler()` can find it by type name.

### Option B: Direct Instantiation

For workers loaded from `WORKER_DIR`, `create_handler()` in `server.py` decides
which handler to use based on the `type` field in pyproject.toml. Add a new
branch there:

```python
# server.py — create_handler()
elif wtype == "my-custom" and handler_ref:
    handler = MyCustomHandler(...)
```

### Handler Contract

Every handler must implement:

```python
async def execute(
    self, spec: dict, workdir: str, rconn, sid: str, channel: str,
) -> HandlerResult
```

Where:
- `spec` — the full session payload from Redis
- `workdir` — path to the ephemeral workdir (input files already written)
- `rconn` — async Redis connection
- `sid` — session ID
- `channel` — pub/sub channel for live log streaming

Return a `HandlerResult(status, exit_code, logs, error, response)`.

### Cancellation Support

Handlers should check `session:{sid}:cancel` in Redis periodically (every 2-5s)
and return `HandlerResult(status="cancelled")` when found. This is the
convention used by both PythonHandler and ServiceHandler.

## Adding New Config Fields

Worker config is loaded from `pyproject.toml` by `load_worker_config()` in
`worker_config.py`. To add a new field:

1. Add it to the returned dict in `load_worker_config()`
2. Use it in `create_handler()` or `_register_service()` as needed
3. Add a test to `tests/test_worker_config.py`

The config dict is attached to every handler as `handler.worker_config`, so
any new field is accessible throughout the execution pipeline.

## Adding New Session Keys

To store additional data per session:

1. Set keys in `run_session()` (server.py) using the `_set()` helper — this
   ensures consistent TTL.
2. Read them in `client.result()` or add a new client method.
3. Clear them in `client._clear_session_keys()` to avoid stale data on resubmit.

## Adding New Client Methods

The `SessionClient` class is straightforward to extend. All methods follow the
same pattern:

1. `await self._ensure_connected()` — lazy Redis connection
2. Do Redis operations via `self.r`
3. Return structured data

## Custom Service Protocols

The service subprocess protocol (stdin/stdout JSON lines) is extensible. The
`Service.run()` loop ignores messages with unknown `type` values. To add new
message types:

1. Send them from your Service subclass via `self._send({"type": "my-type", ...})`
2. Handle them in `ServiceHandler._read_responses()` with a new `elif msg_type == "my-type"` branch

## Middleware / Hooks

There are no built-in hooks, but several natural extension points:

- **Pre-execution**: add checks in `run_session()` before `handler.execute()`
  (e.g., content filtering, custom auth)
- **Post-execution**: add logic after `handler.execute()` in `run_session()`
  (e.g., result caching, webhook notifications)
- **Custom quotas**: replace `_charge_quota()` with a custom implementation
  (e.g., per-org billing, token-based quotas)

## Editor Customization

The sample editor (`samples/editor/`) is a standalone aiohttp app. It talks
to Flute entirely through the `SessionClient` API and Redis. You can:

- Replace it with any frontend (React, Vue, etc.) that speaks the same REST API
- Add new endpoints in `app.py` (e.g., `/api/history`, `/api/admin`)
- Customize the UI for specific worker types (the editor already adapts its
  layout based on whether the selected endpoint is a Python runner or service)
