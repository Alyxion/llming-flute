"""Unit tests for flute.worker_config."""

import os
import sys

import pytest
from flute.worker_config import load_task_handler, load_worker_config


class TestLoadWorkerConfig:
    def test_python_runner(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "my-python-worker"
dependencies = ["numpy", "pandas>=2.0"]

[tool.flute.worker]
type = "python-runner"
description = "General Python execution"
""")
        config = load_worker_config(str(tmp_path))
        assert config["name"] == "my-python-worker"
        assert config["type"] == "python-runner"
        assert config["handler"] is None
        assert config["description"] == "General Python execution"
        assert config["dependencies"] == ["numpy", "pandas>=2.0"]

    def test_task_runner(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "image-processor"
dependencies = ["pillow>=10.0"]

[tool.flute.worker]
type = "task-runner"
handler = "handler:ImageHandler"
description = "Process images"
""")
        config = load_worker_config(str(tmp_path))
        assert config["name"] == "image-processor"
        assert config["type"] == "task-runner"
        assert config["handler"] == "handler:ImageHandler"
        assert config["description"] == "Process images"
        assert config["dependencies"] == ["pillow>=10.0"]

    def test_defaults(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("[project]\n")
        config = load_worker_config(str(tmp_path))
        assert config["name"] == os.path.basename(str(tmp_path))
        assert config["type"] == "python-runner"
        assert config["handler"] is None
        assert config["description"] == ""
        assert config["dependencies"] == []

    def test_no_flute_section(self, tmp_path):
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "bare-worker"
""")
        config = load_worker_config(str(tmp_path))
        assert config["name"] == "bare-worker"
        assert config["type"] == "python-runner"
        assert config["dependencies"] == []

    def test_description_fallback_to_project(self, tmp_path):
        """If no flute description, falls back to project.description."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "fallback-worker"
description = "From project section"
""")
        config = load_worker_config(str(tmp_path))
        assert config["description"] == "From project section"

    def test_operations(self, tmp_path):
        """Loads operations list from pyproject.toml."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "image-service"
dependencies = ["pillow"]

[tool.flute.worker]
type = "service-runner"
handler = "svc:ImageService"
description = "Image processing"
operations = ["resize", "crop", "watermark"]
""")
        config = load_worker_config(str(tmp_path))
        assert config["operations"] == ["resize", "crop", "watermark"]
        assert config["type"] == "service-runner"
        assert config["handler"] == "svc:ImageService"

    def test_operations_default_empty(self, tmp_path):
        """Operations defaults to empty list when not specified."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "basic-worker"

[tool.flute.worker]
type = "python-runner"
""")
        config = load_worker_config(str(tmp_path))
        assert config["operations"] == []

    def test_accept_files(self, tmp_path):
        """Loads accept_files list from pyproject.toml."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "image-service"

[tool.flute.worker]
type = "service-runner"
handler = "svc:ImageService"
accept_files = ["image/*"]
""")
        config = load_worker_config(str(tmp_path))
        assert config["accept_files"] == ["image/*"]

    def test_accept_files_default_empty(self, tmp_path):
        """accept_files defaults to empty list when not specified."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""\
[project]
name = "basic-worker"

[tool.flute.worker]
type = "python-runner"
""")
        config = load_worker_config(str(tmp_path))
        assert config["accept_files"] == []

    def test_missing_pyproject_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_worker_config(str(tmp_path))


class TestLoadTaskHandler:
    def test_loads_handler_class(self, tmp_path):
        handler_py = tmp_path / "my_handler.py"
        handler_py.write_text("""\
class MyHandler:
    task_model = None
    response_model = None
""")
        instance = load_task_handler(str(tmp_path), "my_handler:MyHandler")
        assert instance.__class__.__name__ == "MyHandler"
        # Verify sys.path cleanup
        assert str(tmp_path) not in sys.path

    def test_sys_path_cleaned_on_success(self, tmp_path):
        handler_py = tmp_path / "clean_handler.py"
        handler_py.write_text("class H: pass\n")
        load_task_handler(str(tmp_path), "clean_handler:H")
        assert str(tmp_path) not in sys.path

    def test_sys_path_cleaned_on_error(self, tmp_path):
        with pytest.raises(ModuleNotFoundError):
            load_task_handler(str(tmp_path), "nonexistent:Cls")
        assert str(tmp_path) not in sys.path

    def test_invalid_handler_ref(self, tmp_path):
        with pytest.raises(ValueError):
            load_task_handler(str(tmp_path), "no_colon_here")
