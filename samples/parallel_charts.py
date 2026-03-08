"""Example: submit multiple matplotlib chart sessions in parallel."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

from flute import SessionClient

CHARTS = {
    "sine": {
        "func": "np.sin(x)",
        "color": "steelblue",
        "title": "sin(x)",
    },
    "cosine": {
        "func": "np.cos(x)",
        "color": "coral",
        "title": "cos(x)",
    },
    "tangent": {
        "func": "np.clip(np.tan(x), -5, 5)",
        "color": "seagreen",
        "title": "tan(x) clipped",
    },
    "exponential": {
        "func": "np.exp(-x / 5) * np.sin(x)",
        "color": "orchid",
        "title": "exp(-x/5) · sin(x)",
    },
}

CODE_TEMPLATE = """\
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

x = np.linspace(0, 4 * np.pi, 500)
y = {func}

fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(x, y, color="{color}", linewidth=2)
ax.fill_between(x, y, alpha=0.2, color="{color}")
ax.set_title("{title}", fontsize=14, fontweight="bold")
ax.set_xlabel("x")
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig("{name}.png", dpi=150)
print(f"Chart '{name}' saved")
"""


async def main():
    c = SessionClient(os.environ.get("REDIS_URL", "redis://localhost:6399/0"))

    # Submit all sessions at once
    session_ids = {}
    for name, cfg in CHARTS.items():
        code = CODE_TEMPLATE.format(name=name, **cfg)
        sid = await c.submit(code, max_memory_mb=256)
        session_ids[name] = sid
        print(f"Submitted '{name}' -> session {sid}")

    # Wait for all in parallel
    results = dict(
        zip(
            session_ids.keys(),
            await asyncio.gather(*(c.wait(sid, timeout=60) for sid in session_ids.values())),
            strict=True,
        )
    )

    # Report results
    print(f"\n{'='*50}")
    print(f"Results ({len(results)}/{len(CHARTS)} completed)")
    print(f"{'='*50}")

    out_dir = Path(tempfile.gettempdir()) / "flute_parallel"
    out_dir.mkdir(exist_ok=True)

    all_ok = True
    for name, res in results.items():
        status = res["status"]
        png_key = f"{name}.png"
        has_file = png_key in res["output_files"]
        size = len(res["output_files"][png_key]) if has_file else 0

        if status == "completed" and has_file:
            out_path = out_dir / png_key
            out_path.write_bytes(res["output_files"][png_key])
            print(f"  OK {name:15s}  status={status}  png={size:,} bytes  -> {out_path}")
        else:
            all_ok = False
            print(f"  FAIL {name:15s}  status={status}  file={'yes' if has_file else 'NO'}")
            if res.get("error"):
                print(f"    error: {res['error']}")
            if res.get("logs"):
                print(f"    logs: {res['logs'][:200]}")

    print(f"\n{'All sessions passed!' if all_ok else 'Some sessions FAILED.'}")
    sys.exit(0 if all_ok else 1)


asyncio.run(main())
