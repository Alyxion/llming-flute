"""Unit tests for flute.task_runner."""

import pytest
from flute.task_runner import TaskHandler
from pydantic import BaseModel


class TestTaskHandler:
    def test_base_class_annotations(self):
        """TaskHandler has task_model and response_model as type annotations."""
        annotations = TaskHandler.__annotations__
        assert "task_model" in annotations
        assert "response_model" in annotations

    @pytest.mark.asyncio
    async def test_execute_raises_not_implemented(self):
        h = TaskHandler()
        with pytest.raises(NotImplementedError):
            await h.execute(None, {})

    def test_subclass(self):
        class MyInput(BaseModel):
            x: int

        class MyOutput(BaseModel):
            y: int

        class MyHandler(TaskHandler):
            task_model = MyInput
            response_model = MyOutput

            async def execute(self, task, input_files):
                return MyOutput(y=task.x * 2), {}

        h = MyHandler()
        assert h.task_model is MyInput
        assert h.response_model is MyOutput

    @pytest.mark.asyncio
    async def test_subclass_execute(self):
        class InModel(BaseModel):
            value: str

        class OutModel(BaseModel):
            upper: str

        class UpperHandler(TaskHandler):
            task_model = InModel
            response_model = OutModel

            async def execute(self, task, input_files):
                return OutModel(upper=task.value.upper()), {}

        h = UpperHandler()
        result, files = await h.execute(InModel(value="hello"), {})
        assert result.upper == "HELLO"
        assert files == {}
