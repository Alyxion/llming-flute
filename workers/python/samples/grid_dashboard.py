"""Dashboard grid: 4 charts in a 2x2 grid."""
import math
import random

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    seed: int = 42


p = load_params(Params)
random.seed(p.seed)

# Line chart
x = list(range(50))
y = [math.sin(i / 8) * 20 + 50 + random.gauss(0, 5) for i in x]

# Pie chart
labels = ["Desktop", "Mobile", "Tablet"]
values = [random.randint(30, 60), random.randint(20, 40), random.randint(5, 15)]

# Histogram
samples = [random.gauss(0, 1) for _ in range(500)]

# Scatter
sx = [random.gauss(0, 1) for _ in range(100)]
sy = [0.7 * xi + random.gauss(0, 0.5) for xi in sx]

ui = UI()
ui.plotly(
    data=[{"x": x, "y": y, "type": "scatter", "mode": "lines+markers", "name": "Revenue"}],
    layout={"title": "Revenue Trend", "margin": {"t": 40, "b": 30}},
)
ui.plotly(
    data=[{"labels": labels, "values": values, "type": "pie"}],
    layout={"title": "Traffic Sources", "margin": {"t": 40, "b": 30}},
)
ui.plotly(
    data=[{"x": samples, "type": "histogram", "nbinsx": 30}],
    layout={"title": "Distribution", "margin": {"t": 40, "b": 30}},
)
ui.plotly(
    data=[{"x": sx, "y": sy, "mode": "markers", "type": "scatter"}],
    layout={"title": "Correlation", "margin": {"t": 40, "b": 30}},
)
ui.grid(2)
ui.number_input("seed", value=p.seed, label="Random seed")
ui.render()
print(f"Dashboard with seed {p.seed}")
