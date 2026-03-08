"""Example: send input files, run code, retrieve output files."""

import asyncio
import os

from flute import SessionClient


async def main():
    c = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))

    csv_data = b"name,score\nAlice,95\nBob,87\nCharlie,92\n"

    code = """\
import pandas as pd

df = pd.read_csv("grades.csv")
print(f"Loaded {len(df)} rows")
print(df.to_string(index=False))

summary = df.describe()
summary.to_csv("summary.csv")
print("\\nSummary saved to summary.csv")
"""

    sid = await c.submit(
        code,
        input_files={"grades.csv": csv_data},
        max_memory_mb=256,
    )
    result = await c.wait(sid)

    print(f"Status: {result['status']}")
    print(f"\n--- stdout ---\n{result['logs']}")

    if result["output_files"]:
        print("--- output files ---")
        for name, data in result["output_files"].items():
            print(f"\n{name} ({len(data)} bytes):")
            print(data.decode())


asyncio.run(main())
