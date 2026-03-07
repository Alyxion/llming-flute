"""Example: generate a matplotlib chart and display the result."""

import subprocess
import sys
import tempfile
from pathlib import Path

from flute import SessionClient

c = SessionClient("redis://localhost:6399/0")

code = """\
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

x = np.linspace(0, 4 * np.pi, 500)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

ax1.plot(x, np.sin(x), color="steelblue", linewidth=2)
ax1.fill_between(x, np.sin(x), alpha=0.2, color="steelblue")
ax1.set_title("sin(x)")
ax1.set_xlabel("x")
ax1.grid(True, alpha=0.3)

ax2.plot(x, np.cos(x), color="coral", linewidth=2)
ax2.fill_between(x, np.cos(x), alpha=0.2, color="coral")
ax2.set_title("cos(x)")
ax2.set_xlabel("x")
ax2.grid(True, alpha=0.3)

fig.suptitle("Trigonometric Functions", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig("chart.png", dpi=150)
print(f"Chart saved ({fig.get_size_inches()} inches @ 150 dpi)")
"""

sid = c.submit(code, max_memory_mb=256)
result = c.wait(sid, timeout=30)

print(f"Status: {result['status']}")
print(f"Output: {result['logs']}")

if "chart.png" in result["output_files"]:
    png = result["output_files"]["chart.png"]
    print(f"PNG size: {len(png):,} bytes")

    # Save and open the chart
    out = Path(tempfile.gettempdir()) / "flute_chart.png"
    out.write_bytes(png)
    print(f"Saved to: {out}")

    if sys.platform == "darwin":
        subprocess.Popen(["open", str(out)])
    elif sys.platform == "linux":
        subprocess.Popen(["xdg-open", str(out)])
    else:
        print("Open the file manually to view the chart.")
