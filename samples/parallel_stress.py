"""Stress test: submit 30 sessions and verify all complete correctly."""

import asyncio
import os
import sys
import time

from flute import SessionClient

NUM_TASKS = 30

CODE_TEMPLATE = """\
import math
task_id = {task_id}
result = sum(math.factorial(i) for i in range(50))
print(f"Task {{task_id}} done: {{result}}")
"""


async def main():
    c = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))

    # Submit all at once
    print(f"Submitting {NUM_TASKS} tasks...")
    t0 = time.monotonic()
    session_ids = {}
    for i in range(NUM_TASKS):
        sid = await c.submit(CODE_TEMPLATE.format(task_id=i))
        session_ids[i] = sid

    print(f"All submitted in {time.monotonic() - t0:.2f}s")

    # Wait for all in parallel
    results = dict(
        zip(
            session_ids.keys(),
            await asyncio.gather(*(c.wait(sid, timeout=120) for sid in session_ids.values())),
            strict=True,
        )
    )

    elapsed = time.monotonic() - t0

    # Report
    completed = sum(1 for r in results.values() if r["status"] == "completed")
    failed = {i: r for i, r in results.items() if r["status"] != "completed"}

    print(f"\nCompleted: {completed}/{NUM_TASKS} in {elapsed:.1f}s")

    if failed:
        for i, r in sorted(failed.items()):
            print(f"  FAIL task {i}: status={r['status']} error={r.get('error', '')[:100]}")
            if r.get("logs"):
                print(f"    logs: {r['logs'][:200]}")
        sys.exit(1)
    else:
        print("All tasks passed!")


asyncio.run(main())
