"""Basic example: submit code and get the result."""

from flute import SessionClient

c = SessionClient("redis://localhost:6399/0")

sid = c.submit("print('Hello from the sandbox!')")
result = c.wait(sid)

print(f"Status:    {result['status']}")
print(f"Exit code: {result['exit_code']}")
print(f"Output:    {result['logs']}")
