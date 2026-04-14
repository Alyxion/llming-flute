"""Tests for flute.ui — declarative UI descriptor builder."""

import json
import os
import tempfile

from flute.ui import UI, load_params
from pydantic import BaseModel


class TestUIComponents:
    def test_plotly(self):
        ui = UI()
        ui.plotly(data=[{"x": [1, 2], "y": [3, 4], "type": "scatter"}])
        d = ui.to_dict()
        assert d["version"] == 1
        assert len(d["components"]) == 1
        assert d["components"][0]["type"] == "plotly"
        assert d["components"][0]["data"][0]["type"] == "scatter"

    def test_plotly_with_layout_and_config(self):
        ui = UI()
        ui.plotly(
            data=[{"x": [1], "y": [1]}],
            layout={"title": "Test"},
            config={"displaylogo": False},
            id="chart1",
        )
        comp = ui.to_dict()["components"][0]
        assert comp["layout"]["title"] == "Test"
        assert comp["config"]["displaylogo"] is False
        assert comp["id"] == "chart1"

    def test_table(self):
        ui = UI()
        cols = [{"name": "a", "label": "A"}, {"name": "b", "label": "B"}]
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        ui.table(cols, rows, title="My Table", id="t1")
        comp = ui.to_dict()["components"][0]
        assert comp["type"] == "table"
        assert comp["title"] == "My Table"
        assert len(comp["columns"]) == 2
        assert len(comp["rows"]) == 2

    def test_three(self):
        ui = UI()
        scene = {
            "objects": [{"type": "box", "size": [1, 1, 1]}],
            "background": "#000000",
        }
        ui.three(scene, camera={"fov": 75}, controls=False, id="scene1")
        comp = ui.to_dict()["components"][0]
        assert comp["type"] == "three"
        assert comp["scene"]["background"] == "#000000"
        assert comp["camera"]["fov"] == 75
        assert comp["controls"] is False
        assert comp["id"] == "scene1"

    def test_three_defaults(self):
        ui = UI()
        ui.three({"objects": []})
        comp = ui.to_dict()["components"][0]
        assert comp["controls"] is True
        assert "camera" not in comp

    def test_text(self):
        ui = UI()
        ui.text("Hello world", format="markdown", id="txt1")
        comp = ui.to_dict()["components"][0]
        assert comp["type"] == "text"
        assert comp["content"] == "Hello world"
        assert comp["format"] == "markdown"

    def test_text_defaults(self):
        ui = UI()
        ui.text("Plain text")
        comp = ui.to_dict()["components"][0]
        assert comp["format"] == "plain"

    def test_image(self):
        ui = UI()
        ui.image("chart.png", alt="A chart", id="img1")
        comp = ui.to_dict()["components"][0]
        assert comp["type"] == "image"
        assert comp["filename"] == "chart.png"
        assert comp["alt"] == "A chart"

    def test_image_no_alt(self):
        ui = UI()
        ui.image("photo.jpg")
        comp = ui.to_dict()["components"][0]
        assert "alt" not in comp


class TestUILayout:
    def test_split_h(self):
        ui = UI()
        ui.plotly(data=[]).plotly(data=[])
        ui.split_h(1, 2)
        d = ui.to_dict()
        assert d["layout"]["type"] == "split-h"
        assert d["layout"]["ratios"] == [1, 2]

    def test_split_v(self):
        ui = UI()
        ui.split_v(1, 1, 1)
        d = ui.to_dict()
        assert d["layout"]["type"] == "split-v"
        assert d["layout"]["ratios"] == [1, 1, 1]

    def test_split_h_no_ratios(self):
        ui = UI()
        ui.split_h()
        d = ui.to_dict()
        assert d["layout"]["ratios"] is None

    def test_tabs(self):
        ui = UI()
        ui.tabs(["Chart", "Data"])
        d = ui.to_dict()
        assert d["layout"]["type"] == "tabs"
        assert d["layout"]["labels"] == ["Chart", "Data"]

    def test_tabs_no_labels(self):
        ui = UI()
        ui.tabs()
        d = ui.to_dict()
        assert d["layout"]["labels"] is None

    def test_grid(self):
        ui = UI()
        ui.grid(3)
        d = ui.to_dict()
        assert d["layout"]["type"] == "grid"
        assert d["layout"]["columns"] == 3

    def test_grid_default_columns(self):
        ui = UI()
        ui.grid()
        assert ui.to_dict()["layout"]["columns"] == 2

    def test_no_layout(self):
        ui = UI()
        ui.plotly(data=[])
        d = ui.to_dict()
        assert "layout" not in d


class TestUIControls:
    def test_slider(self):
        ui = UI()
        ui.slider("amplitude", min=0, max=10, step=0.5, value=5, label="Amp")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "slider"
        assert ctrl["name"] == "amplitude"
        assert ctrl["min"] == 0
        assert ctrl["max"] == 10
        assert ctrl["step"] == 0.5
        assert ctrl["value"] == 5
        assert ctrl["label"] == "Amp"

    def test_slider_defaults(self):
        ui = UI()
        ui.slider("x")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["min"] == 0
        assert ctrl["max"] == 100
        assert ctrl["step"] == 1
        assert ctrl["value"] == 50
        assert "label" not in ctrl

    def test_number_input(self):
        ui = UI()
        ui.number_input("count", value=42, label="Count")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "number"
        assert ctrl["value"] == 42
        assert ctrl["label"] == "Count"

    def test_number_input_defaults(self):
        ui = UI()
        ui.number_input("n")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["value"] == 0

    def test_text_input(self):
        ui = UI()
        ui.text_input("query", value="hello", label="Search")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "text"
        assert ctrl["value"] == "hello"

    def test_text_input_defaults(self):
        ui = UI()
        ui.text_input("q")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["value"] == ""

    def test_select(self):
        ui = UI()
        ui.select("color", ["red", "green", "blue"], value="green", label="Color")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "select"
        assert ctrl["options"] == ["red", "green", "blue"]
        assert ctrl["value"] == "green"

    def test_select_default_value(self):
        ui = UI()
        ui.select("x", ["a", "b"])
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["value"] == "a"

    def test_checkbox(self):
        ui = UI()
        ui.checkbox("wireframe", value=True, label="Wireframe")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "checkbox"
        assert ctrl["value"] is True

    def test_checkbox_defaults(self):
        ui = UI()
        ui.checkbox("show")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["value"] is False

    def test_file_drop(self):
        ui = UI()
        ui.file_drop(
            "data_file", accept=".csv,.xlsx", max_size_mb=20, auto_submit=False, label="Upload"
        )
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["type"] == "file-drop"
        assert ctrl["accept"] == ".csv,.xlsx"
        assert ctrl["max_size_mb"] == 20
        assert ctrl["auto_submit"] is False

    def test_file_drop_defaults(self):
        ui = UI()
        ui.file_drop("f")
        ctrl = ui.to_dict()["controls"][0]
        assert ctrl["accept"] == "*"
        assert ctrl["max_size_mb"] == 10
        assert ctrl["auto_submit"] is True

    def test_no_controls(self):
        ui = UI()
        ui.plotly(data=[])
        d = ui.to_dict()
        assert "controls" not in d


class TestUIChaining:
    def test_fluent_api(self):
        ui = UI()
        result = (
            ui.plotly(data=[{"x": [1], "y": [1]}])
            .table([{"name": "a"}], [{"a": 1}])
            .split_h(1, 2)
            .slider("x", min=0, max=10)
        )
        assert result is ui
        d = ui.to_dict()
        assert len(d["components"]) == 2
        assert d["layout"]["type"] == "split-h"
        assert len(d["controls"]) == 1

    def test_multiple_components(self):
        ui = UI()
        ui.plotly(data=[]).table([], []).three({"objects": []}).text("Hi").image("f.png")
        assert len(ui.to_dict()["components"]) == 5


class TestUISerialization:
    def test_to_json(self):
        ui = UI()
        ui.plotly(data=[{"x": [1], "y": [2]}])
        s = ui.to_json()
        parsed = json.loads(s)
        assert parsed["version"] == 1
        assert len(parsed["components"]) == 1

    def test_render(self):
        ui = UI()
        ui.text("test output")
        with tempfile.TemporaryDirectory() as tmpdir:
            ui.render(tmpdir)
            path = os.path.join(tmpdir, "ui.json")
            assert os.path.exists(path)
            with open(path) as f:
                data = json.load(f)
            assert data["version"] == 1
            assert data["components"][0]["content"] == "test output"

    def test_render_cwd(self):
        ui = UI()
        ui.text("cwd test")
        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                ui.render()
                assert os.path.exists(os.path.join(tmpdir, "ui.json"))
            finally:
                os.chdir(old_cwd)

    def test_full_descriptor(self):
        ui = UI()
        ui.plotly(data=[{"x": [1], "y": [1]}], id="chart")
        ui.table(
            [{"name": "x", "label": "X"}],
            [{"x": 42}],
            title="Results",
        )
        ui.split_h(2, 1)
        ui.slider("n", min=1, max=100)
        ui.checkbox("show_grid")

        d = ui.to_dict()
        assert d["version"] == 1
        assert len(d["components"]) == 2
        assert d["components"][0]["id"] == "chart"
        assert d["layout"]["type"] == "split-h"
        assert d["layout"]["ratios"] == [2, 1]
        assert len(d["controls"]) == 2

        # Round-trip through JSON
        s = ui.to_json()
        d2 = json.loads(s)
        assert d2 == d


class TestLoadParams:
    def test_defaults_when_no_file(self):
        class P(BaseModel):
            n: int = 42
            name: str = "hello"

        with tempfile.TemporaryDirectory() as tmpdir:
            old = os.getcwd()
            try:
                os.chdir(tmpdir)
                p = load_params(P)
                assert p.n == 42
                assert p.name == "hello"
            finally:
                os.chdir(old)

    def test_loads_from_file(self):
        class P(BaseModel):
            x: float = 0
            flag: bool = False

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "input.json"), "w") as f:
                json.dump({"x": 3.14, "flag": True}, f)
            old = os.getcwd()
            try:
                os.chdir(tmpdir)
                p = load_params(P)
                assert p.x == 3.14
                assert p.flag is True
            finally:
                os.chdir(old)

    def test_partial_override(self):
        class P(BaseModel):
            a: int = 1
            b: int = 2

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "input.json"), "w") as f:
                json.dump({"b": 99}, f)
            old = os.getcwd()
            try:
                os.chdir(tmpdir)
                p = load_params(P)
                assert p.a == 1
                assert p.b == 99
            finally:
                os.chdir(old)
