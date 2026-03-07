"""Example: stream stdout from a running session in real time."""

import sys

from flute import SessionClient

c = SessionClient("redis://localhost:6399/0")

code = """\
import time

print("Starting computation...")
for i in range(1, 11):
    time.sleep(0.5)
    progress = "#" * i + "." * (10 - i)
    print(f"  Step {i:2d}/10  [{progress}]  ({i * 10}%)")

print("Done!")
"""

sid = c.submit(code, max_runtime_seconds=30)
print(f"Session {sid} submitted. Streaming logs:\n")

for line in c.stream_logs(sid, timeout=30):
    sys.stdout.write(line)
    sys.stdout.flush()

# Fetch final result
result = c.result(sid)
print(f"\nFinal status: {result['status']} (exit code {result['exit_code']})")
