"""Tabbed view: chart, 3D scene, and data table in tabs."""
import math
import random

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    n: int = 30


p = load_params(Params)
random.seed(42)

# Chart data
x = list(range(p.n))
y1 = [math.sin(i / 5) * 10 + random.gauss(0, 2) for i in x]
y2 = [math.cos(i / 5) * 10 + random.gauss(0, 2) for i in x]

# 3D data - random point cloud as spheres
spheres = [
    {"type": "sphere", "radius": 0.15, "position": [random.gauss(0, 2), random.gauss(0, 2), random.gauss(0, 2)],
     "color": f"hsl({i * 360 // p.n}, 70%, 60%)"}
    for i in range(min(p.n, 50))
]

# Table data
rows = [{"i": i, "sin": round(y1[i], 2), "cos": round(y2[i], 2), "diff": round(y1[i] - y2[i], 2)} for i in x]

ui = UI()
ui.plotly(
    data=[
        {"x": x, "y": y1, "type": "scatter", "name": "sin + noise"},
        {"x": x, "y": y2, "type": "scatter", "name": "cos + noise"},
    ],
    layout={"title": "Time Series"},
)
ui.three({"objects": spheres, "background": "#1a1a2e"}, camera={"position": [5, 5, 5]})
ui.table(
    [{"name": "i", "label": "#", "field": "i"},
     {"name": "sin", "label": "Sin", "field": "sin"},
     {"name": "cos", "label": "Cos", "field": "cos"},
     {"name": "diff", "label": "Diff", "field": "diff"}],
    rows,
    title="Raw Data",
)
ui.tabs(["Chart", "3D Cloud", "Data"])
ui.slider("n", min=10, max=100, step=5, value=p.n, label="Points")
ui.render()
print(f"Multi-view with {p.n} points")
