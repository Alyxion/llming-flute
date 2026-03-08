"""Base class for Pydantic-typed task handlers.

Users subclass TaskHandler to define typed workers. Example:

    from pydantic import BaseModel
    from flute.task_runner import TaskHandler

    class MyInput(BaseModel):
        text: str

    class MyOutput(BaseModel):
        result: str

    class MyHandler(TaskHandler):
        task_model = MyInput
        response_model = MyOutput

        async def execute(self, task, input_files):
            return MyOutput(result=task.text.upper()), {}
"""

from __future__ import annotations

from pydantic import BaseModel


class TaskHandler:
    """Base class for Pydantic-typed task handlers.

    Subclass and set ``task_model`` and ``response_model`` to Pydantic models.
    Override ``execute()`` to implement the task logic.

    Returns:
        A tuple of (response_model_instance, output_files_dict).
    """

    task_model: type[BaseModel]
    response_model: type[BaseModel]

    async def execute(
        self,
        task: BaseModel,
        input_files: dict[str, bytes],
    ) -> tuple[BaseModel, dict[str, bytes]]:
        """Execute a task. Override this in your subclass."""
        raise NotImplementedError
