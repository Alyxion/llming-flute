"""Basic example: submit code and get the result."""

import asyncio
import os

from flute import SessionClient


async def main():
    c = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))

    sid = await c.submit("print('Hello from the sandbox!')")
    result = await c.wait(sid)

    print(f"Status:    {result['status']}")
    print(f"Exit code: {result['exit_code']}")
    print(f"Output:    {result['logs']}")


asyncio.run(main())
