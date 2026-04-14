"""Side-by-side chart and data table."""
import math

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    frequency: float = 2


p = load_params(Params)

x = [i * 0.1 for i in range(100)]
y = [math.sin(p.frequency * v) for v in x]

# Build table of peaks
peaks = []
for i in range(1, len(y) - 1):
    if y[i] > y[i - 1] and y[i] > y[i + 1]:
        peaks.append({"index": i, "x": round(x[i], 2), "y": round(y[i], 4)})

ui = UI()
ui.plotly(
    data=[{"x": x, "y": y, "type": "scatter", "mode": "lines", "name": f"sin({p.frequency}x)"}],
    layout={"title": f"sin({p.frequency}x)", "xaxis": {"title": "x"}},
)
ui.table(
    [
        {"name": "index", "label": "#", "field": "index"},
        {"name": "x", "label": "X", "field": "x"},
        {"name": "y", "label": "Peak Y", "field": "y"},
    ],
    peaks,
    title="Detected Peaks",
)
ui.split_h(2, 1)
ui.slider("frequency", min=0.5, max=10, step=0.5, value=p.frequency, label="Frequency")
ui.render()
print(f"Found {len(peaks)} peaks at frequency {p.frequency}")
