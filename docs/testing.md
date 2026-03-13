# Testing

## Running Tests

```bash
# All unit tests (excludes integration tests that need live Redis)
pytest tests/ --ignore=tests/test_integration.py -q

# With coverage (must stay at 100%)
pytest tests/ --ignore=tests/test_integration.py --cov=flute --cov-report=term-missing

# Integration tests (requires running Redis + workers)
docker-compose up -d
pytest tests/test_integration.py -q

# Lint
ruff check llming_flute/ tests/ workers/ scripts/
```

## Coverage Requirement

The project enforces **100% line coverage** via `pyproject.toml`:

```toml
[tool.coverage.report]
fail_under = 100.0
```

Every line in `llming_flute/flute/` must be exercised by tests. This is
non-negotiable — the CI will fail if coverage drops.

## Test Infrastructure

### FakeRedis (`tests/conftest.py`)

All unit tests use an in-memory Redis mock (`FakeRedis`) that implements the
async Redis interface. This keeps tests fast (no I/O) and deterministic.

Supported operations: strings, hashes, lists, pub/sub, TTL/expire, INCRBYFLOAT.

### Fixtures

| Fixture | Scope | Description |
|---------|-------|-------------|
| `fake_redis` | function | Fresh FakeRedis instance |
| `client` | function | SessionClient backed by FakeRedis (no network) |

### Test Organization

| File | Tests | Covers |
|------|-------|--------|
| `test_client.py` | SessionClient (submit, wait, result, quota, cancel, ...) | `client.py` |
| `test_server.py` | Server loop, handler creation, session execution, heartbeat, service registration | `server.py` |
| `test_handlers.py` | PythonHandler, TaskRunnerHandler, ServiceHandler, utilities | `handlers.py` |
| `test_service.py` | Service base class, Job, _Capture, in-process run() | `service.py` |
| `test_worker_config.py` | Config loading, task handler loading | `worker_config.py` |
| `test_quota_rules.py` | Rule parsing, type resolution | `quota_rules.py` |
| `test_redis_conn.py` | Connection factory, cluster detection | `redis_conn.py` |
| `test_integration.py` | End-to-end tests against live Redis + workers | All |

### Testing Service Handlers

ServiceHandler tests use two approaches:

1. **Real subprocess tests**: Create a temporary Service subclass, start it as
   a real subprocess, and exercise the full protocol.

2. **Mock subprocess tests**: For edge cases (process death, bad JSON, timeouts),
   mock `self._proc` with a `MagicMock` that simulates the failure.

### Testing the Service Base Class

`Service.run()` is normally called inside a subprocess. For in-process testing,
`test_service.py` uses a `_FakeStdin` helper that feeds JSON lines to
`sys.stdin` and captures protocol output from `sys.stdout`.

## Writing New Tests

When adding features:

1. Write tests for all new code paths
2. Run `pytest --cov=flute --cov-report=term-missing` and check for `Missing` lines
3. Use `monkeypatch` to speed up timing-dependent tests (e.g., `CANCEL_CHECK_INTERVAL`)
4. Use `FakeRedis` for unit tests, real Redis only for integration tests
5. Keep integration test fixtures function-scoped (async event loop per test)
