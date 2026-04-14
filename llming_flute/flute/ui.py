"""Flute UI — declarative UI descriptors for visualization from sandboxed scripts.

Scripts write a ``ui.json`` output file containing a UI descriptor. The editor
renders it client-side using the bundled vendor libraries (Plotly, Three.js,
Quasar/Vue).

Usage inside a worker script::

    from flute.ui import UI

    ui = UI()
    ui.plotly(data=[{"x": [1,2,3], "y": [4,5,6], "type": "scatter"}])
    ui.render()          # writes ui.json to the output directory
"""

from __future__ import annotations

import json
import os
from typing import Any

from pydantic import BaseModel

_INPUT_FILE = "input.json"


def load_params[T: BaseModel](model: type[T]) -> T:
    """Load parameters from ``input.json`` into a Pydantic model.

    Returns default values when the file doesn't exist::

        class Params(BaseModel):
            amplitude: float = 5
            frequency: float = 2

        p = load_params(Params)
    """
    if os.path.exists(_INPUT_FILE):
        with open(_INPUT_FILE) as f:
            return model.model_validate_json(f.read())
    return model()


class UI:
    """Builder for a UI descriptor that gets serialised to ``ui.json``."""

    def __init__(self) -> None:
        self._components: list[dict[str, Any]] = []
        self._layout: dict[str, Any] | None = None
        self._controls: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Components
    # ------------------------------------------------------------------

    def plotly(
        self,
        data: list[dict[str, Any]],
        layout: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        *,
        id: str | None = None,
    ) -> UI:
        """Add a Plotly chart."""
        comp: dict[str, Any] = {"type": "plotly", "data": data}
        if layout:
            comp["layout"] = layout
        if config:
            comp["config"] = config
        if id:
            comp["id"] = id
        self._components.append(comp)
        return self

    def table(
        self,
        columns: list[dict[str, str]],
        rows: list[dict[str, Any]],
        *,
        title: str | None = None,
        id: str | None = None,
    ) -> UI:
        """Add a data table (rendered via Quasar QTable)."""
        comp: dict[str, Any] = {"type": "table", "columns": columns, "rows": rows}
        if title:
            comp["title"] = title
        if id:
            comp["id"] = id
        self._components.append(comp)
        return self

    def three(
        self,
        scene: dict[str, Any],
        *,
        camera: dict[str, Any] | None = None,
        controls: bool = True,
        id: str | None = None,
    ) -> UI:
        """Add a Three.js 3D scene.

        The *scene* dict describes objects declaratively::

            {
                "objects": [
                    {"type": "box", "size": [1,1,1], "position": [0,0,0], "color": "#ff0000"},
                    {"type": "sphere", "radius": 0.5, "position": [2,0,0]},
                    {"type": "line", "points": [[0,0,0],[1,1,1]]},
                    {"type": "bezier", "points": [[0,0,0],[1,2,0],[2,0,0],[3,1,0]]},
                    {"type": "catmull", "points": [[0,0,0],[1,1,0],[2,0,0]], "closed": false},
                    {"type": "surface", "func": "sin(x)*cos(y)",
                     "range": [[-3,3],[-3,3]], "resolution": 50},
                    {"type": "mesh", "vertices": [...], "faces": [...]},
                ],
                "background": "#1a1a2e",
                "ambient_light": 0.4,
                "directional_light": {"intensity": 0.8, "position": [5,10,7]}
            }
        """
        comp: dict[str, Any] = {"type": "three", "scene": scene}
        if camera:
            comp["camera"] = camera
        comp["controls"] = controls
        if id:
            comp["id"] = id
        self._components.append(comp)
        return self

    def text(self, content: str, *, format: str = "plain", id: str | None = None) -> UI:
        """Add a text block (plain or markdown)."""
        comp: dict[str, Any] = {"type": "text", "content": content, "format": format}
        if id:
            comp["id"] = id
        self._components.append(comp)
        return self

    def image(self, filename: str, *, alt: str = "", id: str | None = None) -> UI:
        """Reference an output image file by name."""
        comp: dict[str, Any] = {"type": "image", "filename": filename}
        if alt:
            comp["alt"] = alt
        if id:
            comp["id"] = id
        self._components.append(comp)
        return self

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------

    def split_h(self, *ratios: float) -> UI:
        """Horizontal split layout. Ratios map to components in order."""
        self._layout = {"type": "split-h", "ratios": list(ratios) or None}
        return self

    def split_v(self, *ratios: float) -> UI:
        """Vertical split layout."""
        self._layout = {"type": "split-v", "ratios": list(ratios) or None}
        return self

    def tabs(self, labels: list[str] | None = None) -> UI:
        """Tabbed layout. Labels map to components in order."""
        self._layout = {"type": "tabs", "labels": labels}
        return self

    def grid(self, columns: int = 2) -> UI:
        """Grid layout with the given number of columns."""
        self._layout = {"type": "grid", "columns": columns}
        return self

    # ------------------------------------------------------------------
    # Controls (re-run triggers)
    # ------------------------------------------------------------------

    def slider(
        self,
        name: str,
        *,
        min: float = 0,
        max: float = 100,
        step: float = 1,
        value: float = 50,
        label: str | None = None,
    ) -> UI:
        """Add a slider control that feeds back into params on re-run."""
        ctrl: dict[str, Any] = {
            "type": "slider",
            "name": name,
            "min": min,
            "max": max,
            "step": step,
            "value": value,
        }
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    def number_input(
        self, name: str, *, value: float = 0, label: str | None = None
    ) -> UI:
        """Add a numeric input control."""
        ctrl: dict[str, Any] = {"type": "number", "name": name, "value": value}
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    def text_input(
        self, name: str, *, value: str = "", label: str | None = None
    ) -> UI:
        """Add a text input control."""
        ctrl: dict[str, Any] = {"type": "text", "name": name, "value": value}
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    def select(
        self,
        name: str,
        options: list[str],
        *,
        value: str | None = None,
        label: str | None = None,
    ) -> UI:
        """Add a dropdown select control."""
        ctrl: dict[str, Any] = {
            "type": "select",
            "name": name,
            "options": options,
            "value": value or options[0],
        }
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    def checkbox(
        self, name: str, *, value: bool = False, label: str | None = None
    ) -> UI:
        """Add a checkbox control."""
        ctrl: dict[str, Any] = {"type": "checkbox", "name": name, "value": value}
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    def file_drop(
        self,
        name: str,
        *,
        accept: str = "*",
        max_size_mb: int = 10,
        auto_submit: bool = True,
        label: str | None = None,
    ) -> UI:
        """Add a file drop zone that optionally auto-submits on drop."""
        ctrl: dict[str, Any] = {
            "type": "file-drop",
            "name": name,
            "accept": accept,
            "max_size_mb": max_size_mb,
            "auto_submit": auto_submit,
        }
        if label:
            ctrl["label"] = label
        self._controls.append(ctrl)
        return self

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return the full UI descriptor as a dict."""
        desc: dict[str, Any] = {"version": 1, "components": self._components}
        if self._layout:
            desc["layout"] = self._layout
        if self._controls:
            desc["controls"] = self._controls
        return desc

    def to_json(self) -> str:
        """Return the UI descriptor as a JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def render(self, output_dir: str | None = None) -> None:
        """Write ``ui.json`` to the output directory.

        If *output_dir* is not given, uses the current working directory
        (which inside a worker is the sandboxed workdir).
        """
        target = os.path.join(output_dir or os.getcwd(), "ui.json")
        with open(target, "w") as f:
            f.write(self.to_json())
