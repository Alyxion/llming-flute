"""Worker configuration loading from pyproject.toml manifests."""

import importlib
import json
import os
import sys
import tomllib


def load_worker_config(worker_dir: str) -> dict:
    """Load worker configuration from pyproject.toml in the given directory.

    Returns a dict with keys: name, type, handler, description, worker_dir, samples.
    """
    pyproject_path = os.path.join(worker_dir, "pyproject.toml")
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    project = data.get("project", {})
    flute = data.get("tool", {}).get("flute", {}).get("worker", {})

    # Load samples index if present
    samples = _load_samples_index(worker_dir)

    return {
        "name": project.get("name", os.path.basename(worker_dir)),
        "type": flute.get("type", "python-runner"),
        "handler": flute.get("handler"),
        "description": flute.get("description", project.get("description", "")),
        "dependencies": project.get("dependencies", []),
        "operations": flute.get("operations", []),
        "accept_files": flute.get("accept_files", []),
        "worker_dir": worker_dir,
        "samples": samples,
    }


def _load_samples_index(worker_dir: str) -> dict | None:
    """Load samples/index.json from the worker directory if it exists."""
    index_path = os.path.join(worker_dir, "samples", "index.json")
    if not os.path.isfile(index_path):
        return None
    with open(index_path) as f:
        return json.load(f)


def load_task_handler(worker_dir: str, handler_ref: str):
    """Import and instantiate a TaskHandler from a worker directory.

    handler_ref format: "module_name:ClassName"
    """
    module_name, class_name = handler_ref.split(":")
    sys.path.insert(0, worker_dir)
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls()
    finally:
        if worker_dir in sys.path:
            sys.path.remove(worker_dir)
