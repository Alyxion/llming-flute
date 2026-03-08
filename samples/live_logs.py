"""Example: stream stdout from a running session in real time."""

import asyncio
import os
import sys

from flute import SessionClient


async def main():
    c = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))

    code = """\
import time

print("Starting computation...")
for i in range(1, 11):
    time.sleep(0.5)
    progress = "#" * i + "." * (10 - i)
    print(f"  Step {i:2d}/10  [{progress}]  ({i * 10}%)")

print("Done!")
"""

    sid = await c.submit(code, max_runtime_seconds=30)
    print(f"Session {sid} submitted. Streaming logs:\n")

    async for line in c.stream_logs(sid, timeout=30):
        sys.stdout.write(line)
        sys.stdout.flush()

    result = await c.result(sid)
    print(f"\nFinal status: {result['status']} (exit code {result['exit_code']})")


asyncio.run(main())
