"""Image processing service -- long-lived worker with simulated model loading.

Supports multi-step pipelines: ``["grayscale", "resize"]``.
Each step checks quota before proceeding, and performance is tracked
so the IDE can show accurate ETAs.
"""

from __future__ import annotations

import io
import time

from flute.service import Service
from PIL import Image


class ImageService(Service):
    operations = ["grayscale", "resize"]
    max_input_bytes = 20 * 1024 * 1024  # 20 MB

    def setup(self):
        print("Loading image processing model...")
        time.sleep(5)  # simulate heavy model loading
        self.model_ready = True
        print("Model loaded, ready for requests.")

    def process(self, job):
        # Find the first image file
        img_data = None
        img_name = None
        for name in job.input_files():
            if name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp")):
                img_data = job.read_file(name, max_bytes=self.max_input_bytes)
                img_name = name
                break

        if img_data is None:
            raise ValueError("No image file found in inputs")

        img = Image.open(io.BytesIO(img_data))
        steps = job.steps or ["grayscale"]
        total = len(steps)

        for i, step in enumerate(steps):
            # Quota gate between steps
            remaining = job.check_quota()
            if remaining <= 0:
                raise RuntimeError(
                    f"Quota exceeded before step '{step}' "
                    f"(elapsed {job.elapsed:.1f}s, budget {job.quota_budget:.0f}s)"
                )

            job.progress(i / total, step)
            step_start = time.monotonic()

            if step == "grayscale":
                img = img.convert("L")
            elif step == "resize":
                w = job.params.get("width", img.width // 2)
                h = job.params.get("height", img.height // 2)
                img = img.resize((w, h))
            else:
                raise ValueError(f"Unknown operation: {step}")

            self._record_perf(step, time.monotonic() - step_start)

        # Save output
        ext = img_name.rsplit(".", 1)[-1].lower()
        out_name = f"output.{ext}"
        buf = io.BytesIO()
        fmt = "JPEG" if ext in ("jpg", "jpeg") else ext.upper()
        img.save(buf, format=fmt)
        job.write_file(out_name, buf.getvalue())

        job.progress(1.0, "Complete")
        print(f"Processed {img_name}: {' -> '.join(steps)}")
