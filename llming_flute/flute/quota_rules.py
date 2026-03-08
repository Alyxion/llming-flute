"""Quota rules parser for per-worker-type rate limiting.

Format: "worker_type:limit/interval [worker_type:limit/interval ...]"
Special type "*" is the global default.

Examples:
    "*:600/3600"                    -- 600s per 3600s for all types
    "*:600/3600 image:120/3600"     -- 120s/3600s for image, 600s/3600s for others
    "python:300/1800 image:60/1800" -- per-type only, no wildcard default
"""


def parse_quota_rules(rules_str: str) -> dict[str, tuple[int, int]]:
    """Parse a quota rules string into {worker_type: (limit_seconds, interval_seconds)}."""
    rules = {}
    for part in rules_str.strip().split():
        if ":" not in part:
            continue
        type_part, quota_part = part.split(":", 1)
        if "/" not in quota_part:
            continue
        limit_str, interval_str = quota_part.split("/", 1)
        rules[type_part] = (int(limit_str), int(interval_str))
    return rules


def get_quota_for_type(
    rules: dict[str, tuple[int, int]],
    worker_type: str,
    default_limit: int = 600,
    default_interval: int = 3600,
) -> tuple[int, int]:
    """Look up (limit, interval) for a worker type.

    Resolution: exact match -> "*" wildcard -> defaults.
    """
    if worker_type in rules:
        return rules[worker_type]
    if "*" in rules:
        return rules["*"]
    return (default_limit, default_interval)
