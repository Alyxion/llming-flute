# Creating Workers

A worker is a directory with a `pyproject.toml` that tells Flute how to run it.
There are three worker types, each suited to different use cases.

## Worker Types at a Glance

| Type | Best For | Execution Model | I/O |
|------|----------|----------------|-----|
| `python-runner` | Ad-hoc scripts, notebooks, data exploration | New subprocess per session | Code in, logs + files out |
| `task-runner` | Structured APIs with typed input/output | In-process, Pydantic-validated | JSON in, JSON + files out |
| `service-runner` | Expensive model loading, multi-step pipelines | Long-lived subprocess, reused across sessions | Files + params in, files out |

## Python Runner

The simplest type. Accepts arbitrary Python code, runs it in a subprocess.

```
workers/python/
  pyproject.toml
```

```toml
[project]
name = "python"
dependencies = ["numpy", "pandas", "matplotlib"]

[tool.flute.worker]
type = "python-runner"
description = "General Python execution"
```

No handler code needed. The client submits code as a string:

```python
sid = await client.submit("print('hello world')")
```

## Task Runner

For workers that need structured input/output with validation.

```
workers/my-task/
  pyproject.toml
  handler.py
```

```toml
[project]
name = "my-task"
dependencies = ["pillow"]

[tool.flute.worker]
type = "task-runner"
handler = "handler:MyHandler"
description = "Does something specific"
```

```python
# handler.py
from pydantic import BaseModel
from flute.task_runner import TaskHandler

class MyInput(BaseModel):
    text: str
    uppercase: bool = False

class MyOutput(BaseModel):
    result: str
    length: int

class MyHandler(TaskHandler):
    task_model = MyInput
    response_model = MyOutput

    async def execute(self, task, input_files):
        text = task.text.upper() if task.uppercase else task.text
        return MyOutput(result=text, length=len(text)), {}
```

The Pydantic schemas are automatically published to Redis for service discovery.
Clients can introspect them via `client.services()`.

```python
sid = await client.submit_task(
    '{"text": "hello", "uppercase": true}',
    worker_type="my-task",
)
```

## Service Runner

For workers that load expensive resources once and handle many requests.
The subprocess stays alive between jobs.

```
workers/image-processor/
  pyproject.toml
  service.py
```

```toml
[project]
name = "image-processor"
dependencies = ["pillow>=10.0"]

[tool.flute.worker]
type = "service-runner"
handler = "service:ImageService"
description = "Image processing with multi-step pipelines"
operations = ["grayscale", "resize"]
accept_files = ["image/*"]
```

```python
# service.py
import io
from flute.service import Service
from PIL import Image

class ImageService(Service):
    operations = ["grayscale", "resize"]
    max_input_bytes = 20 * 1024 * 1024

    def setup(self):
        # Called once on startup. Load models, warm caches, etc.
        print("Loading model...")
        self.model_ready = True

    def process(self, job):
        img_data = job.read_file("input.png")
        img = Image.open(io.BytesIO(img_data))

        for i, step in enumerate(job.steps):
            job.progress(i / len(job.steps), step)

            if step == "grayscale":
                img = img.convert("L")
            elif step == "resize":
                w = job.params.get("width", img.width // 2)
                h = job.params.get("height", img.height // 2)
                img = img.resize((w, h))

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        job.write_file("output.png", buf.getvalue())
        job.progress(1.0, "Done")
```

### Service API: `Job` Object

| Method | Description |
|--------|-------------|
| `job.read_file(name, max_bytes=100MB)` | Read an input file from the workdir |
| `job.write_file(name, data)` | Write an output file |
| `job.input_files()` | List files in the workdir |
| `job.progress(value, step, eta_seconds=None)` | Report progress (0.0-1.0). ETA is auto-calculated from performance history if not provided. |
| `job.check_quota()` | Remaining budget in seconds (0 when exceeded) |
| `job.elapsed` | Wall-clock seconds since job start |
| `job.params` | Dict of parameters from the client |
| `job.steps` | List of pipeline step names |

### Performance Tracking

Services automatically track step durations (last 20 samples per step).
Call `self._record_perf(step, duration)` after each step. The `job.progress()`
method uses this history to calculate accurate ETAs.

## pyproject.toml Reference

```toml
[project]
name = "worker-name"           # Used as the worker type / queue name
description = "..."            # Shown in service discovery (fallback)
dependencies = ["dep>=1.0"]    # Installed in the Docker image

[tool.flute.worker]
type = "python-runner"         # python-runner | task-runner | service-runner
handler = "module:ClassName"   # Required for task-runner and service-runner
description = "..."            # Overrides [project] description
operations = ["op1", "op2"]    # Service runner: available pipeline steps
accept_files = ["image/*"]     # MIME patterns for UI file filtering
```

## Adding a New Worker

1. Create a directory under `workers/`:
   ```
   workers/my-worker/
     pyproject.toml
     service.py  (or handler.py)
   ```

2. Define the `pyproject.toml` with `[tool.flute.worker]` config.

3. Implement the handler (TaskHandler subclass or Service subclass).

4. Add to `docker-compose.yml`:
   ```yaml
   worker-my-worker:
     <<: *worker-base
     environment:
       REDIS_URL: redis://redis:6379/0
       WORKER_DIR: /app/workers/my-worker
       MAX_CONCURRENT: "4"
     mem_limit: 512m
   ```

5. Add to `k8s/` if deploying to Kubernetes.

6. `docker-compose up --build` — the worker auto-registers in Redis and
   appears in `client.services()`.
