"""Image processing task handler for Flute."""

from __future__ import annotations

import io
from typing import Literal

from flute.task_runner import TaskHandler
from PIL import Image
from pydantic import BaseModel


class ImageProcessTask(BaseModel):
    """Input: image processing request."""

    operation: Literal["grayscale", "resize", "thumbnail"] = "grayscale"
    width: int | None = None
    height: int | None = None
    max_size: int = 256


class ImageProcessResponse(BaseModel):
    """Output: processing result metadata."""

    operation: str
    output_filename: str
    output_width: int
    output_height: int


class ImageProcessHandler(TaskHandler):
    """Processes images via Pydantic-typed tasks."""

    task_model = ImageProcessTask
    response_model = ImageProcessResponse
    description = "Process images: grayscale, resize, thumbnail"

    async def execute(
        self,
        task: ImageProcessTask,
        input_files: dict[str, bytes],
    ) -> tuple[ImageProcessResponse, dict[str, bytes]]:
        # Find the first image file
        img_data = None
        img_name = None
        for name, data in input_files.items():
            if name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp")):
                img_data = data
                img_name = name
                break

        if img_data is None:
            raise ValueError("No image file found in input_files")

        img = Image.open(io.BytesIO(img_data))
        ext = img_name.rsplit(".", 1)[-1].lower()
        out_name = f"output.{ext}"

        if task.operation == "grayscale":
            img = img.convert("L")
        elif task.operation == "resize":
            w = task.width or img.width
            h = task.height or img.height
            img = img.resize((w, h))
        elif task.operation == "thumbnail":
            img.thumbnail((task.max_size, task.max_size))

        buf = io.BytesIO()
        img.save(buf, format=img.format or "PNG")
        output_bytes = buf.getvalue()

        return ImageProcessResponse(
            operation=task.operation,
            output_filename=out_name,
            output_width=img.width,
            output_height=img.height,
        ), {out_name: output_bytes}
