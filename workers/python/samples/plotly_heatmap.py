"""Heatmap of a mathematical function."""
import math

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    resolution: int = 30


p = load_params(Params)

x = [i * 6 / p.resolution - 3 for i in range(p.resolution)]
y = list(x)
z = [[math.sin(xi ** 2 + yi ** 2) for xi in x] for yi in y]

ui = UI()
ui.plotly(
    data=[{"z": z, "x": x, "y": y, "type": "heatmap", "colorscale": "Viridis"}],
    layout={"title": f"sin(x² + y²)  [{p.resolution}×{p.resolution}]"},
)
ui.slider("resolution", min=10, max=80, step=5, value=p.resolution, label="Resolution")
ui.render()
print(f"Heatmap at {p.resolution}x{p.resolution}")
