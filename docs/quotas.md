# Quota System

Quotas limit how much CPU time each user can consume per worker type within a
rolling time window.

## How It Works

1. **Before execution**: server checks `quota:{user_id}:{worker_type}`. If the
   accumulated value exceeds the limit, the session is rejected with status
   `quota_exceeded`.

2. **After execution**: the elapsed wall-clock seconds are atomically added to
   the quota key. This happens even if the session was killed or errored, so
   crashes cannot be exploited for free compute.

3. **Window expiry**: the quota key's TTL equals the window interval. Once it
   expires, the user gets a fresh budget.

## Configuration

Quota rules are set via the `QUOTA_RULES` environment variable:

```
QUOTA_RULES="*:600/3600 image-processor:120/3600"
```

Format: `type:limit_seconds/interval_seconds`, space-separated.

| Token | Meaning |
|-------|---------|
| `*` | Wildcard — applies to any type not explicitly listed |
| `type` | Exact worker type name |
| `limit_seconds` | Max CPU seconds allowed per window |
| `interval_seconds` | Window duration in seconds |

### Resolution Order

1. Exact match on worker type name
2. Wildcard `*` match
3. Fallback defaults: `QUOTA_SECONDS` / `QUOTA_INTERVAL` env vars (600s / 3600s)

## Redis Keys

| Key | Type | Lifetime |
|-----|------|----------|
| `quota:{user_id}:{worker_type}` | float string | Expires after `interval_seconds` |
| `quota:__config:rules` | string | Refreshed by heartbeat every ~10s, expires after `INFRA_TTL` (120s) |

## Client API

```python
usage = await client.quota("alice", "image-processor")
# {
#   "user_id": "alice",
#   "worker_type": "image-processor",
#   "used_seconds": 45.23,
#   "limit_seconds": 120,
#   "interval_seconds": 3600,
#   "remaining_seconds": 74.77,
#   "resets_in_seconds": 2847
# }
```

## Notes

- Quotas are only enforced when `user_id` is provided in the submission.
  Anonymous sessions bypass quota checks.
- The quota is per-user per-worker-type. A user's Python quota is independent
  of their image-processor quota.
- The server publishes its `QUOTA_RULES` to Redis so clients can read limits
  without needing local config.
