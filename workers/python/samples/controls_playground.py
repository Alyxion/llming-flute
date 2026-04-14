"""Interactive controls: sliders, inputs, and dropdowns drive a chart."""
import math

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    amplitude: float = 5
    frequency: float = 2
    func: str = "sin"
    offset: float = 0
    show_grid: bool = True


p = load_params(Params)

x = [i * 0.05 for i in range(200)]
fn = {"sin": math.sin, "cos": math.cos, "tan": lambda v: max(-10, min(10, math.tan(v)))}
f = fn.get(p.func, math.sin)
y = [p.amplitude * f(p.frequency * v) + p.offset for v in x]

ui = UI()
ui.plotly(
    data=[{"x": x, "y": y, "type": "scatter", "mode": "lines",
           "name": f"{p.amplitude}·{p.func}({p.frequency}x) + {p.offset}"}],
    layout={
        "title": f"{p.func}(x) Explorer",
        "xaxis": {"title": "x", "showgrid": p.show_grid},
        "yaxis": {"title": "y", "showgrid": p.show_grid},
    },
)
ui.slider("amplitude", min=0.5, max=10, step=0.5, value=p.amplitude, label="Amplitude")
ui.slider("frequency", min=0.5, max=10, step=0.5, value=p.frequency, label="Frequency")
ui.select("func", ["sin", "cos", "tan"], value=p.func, label="Function")
ui.number_input("offset", value=p.offset, label="Y offset")
ui.checkbox("show_grid", value=p.show_grid, label="Grid")
ui.render()
print(f"{p.amplitude}·{p.func}({p.frequency}x) + {p.offset}")
