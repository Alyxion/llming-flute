#!/usr/bin/env python3
"""Install dependencies from all worker pyproject.toml files.

Scans /workers/*/pyproject.toml and installs each worker's
[project.dependencies] via pip. Runs at Docker build time.
"""

import subprocess
import sys
import tomllib
from pathlib import Path

WORKERS_DIR = Path("/workers")


def main():
    if not WORKERS_DIR.exists():
        print("[install_worker_deps] No /workers directory found, skipping")
        return

    all_deps: set[str] = set()
    for pyproject in sorted(WORKERS_DIR.glob("*/pyproject.toml")):
        worker_name = pyproject.parent.name
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        deps = data.get("project", {}).get("dependencies", [])
        if deps:
            print(f"[install_worker_deps] {worker_name}: {deps}")
            all_deps.update(deps)

    if not all_deps:
        print("[install_worker_deps] No worker dependencies to install")
        return

    print(f"[install_worker_deps] Installing: {sorted(all_deps)}")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--no-cache-dir", *sorted(all_deps)]
    )


if __name__ == "__main__":
    main()
