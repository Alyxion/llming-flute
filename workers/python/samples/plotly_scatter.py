"""Interactive scatter plot with trend line."""
import math
import random

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    n: int = 50


p = load_params(Params)

random.seed(42)
x = [i / 5 for i in range(p.n)]
y = [math.sin(v) + random.gauss(0, 0.2) for v in x]
trend = [math.sin(v) for v in x]

ui = UI()
ui.plotly(
    data=[
        {"x": x, "y": y, "mode": "markers", "name": "Data", "type": "scatter"},
        {"x": x, "y": trend, "mode": "lines", "name": "sin(x)", "type": "scatter"},
    ],
    layout={"title": f"Scatter Plot ({p.n} points)", "xaxis": {"title": "x"}, "yaxis": {"title": "y"}},
)
ui.slider("n", min=10, max=200, step=10, value=p.n, label="Points")
ui.render()
print(f"Rendered scatter plot with {p.n} points")
