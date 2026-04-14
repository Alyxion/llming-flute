"""Classic matplotlib chart exported as image + displayed in UI."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    style: str = "seaborn-v0_8-darkgrid"


p = load_params(Params)

plt.style.use(p.style)
fig, axes = plt.subplots(1, 2, figsize=(10, 4))

x = np.linspace(0, 4 * np.pi, 200)
axes[0].plot(x, np.sin(x), label="sin(x)")
axes[0].plot(x, np.cos(x), label="cos(x)")
axes[0].set_title("Trigonometric Functions")
axes[0].legend()

data = np.random.randn(1000)
axes[1].hist(data, bins=40, edgecolor="black", alpha=0.7)
axes[1].set_title("Normal Distribution")

plt.tight_layout()
plt.savefig("chart.png", dpi=150, bbox_inches="tight")
plt.close()

ui = UI()
ui.image("chart.png", alt="Matplotlib dual chart")
ui.text(f"Style: {p.style}\nGenerated with matplotlib + numpy")
ui.split_h(3, 1)
ui.select("style", [
    "seaborn-v0_8-darkgrid", "seaborn-v0_8-whitegrid", "ggplot",
    "bmh", "dark_background", "fivethirtyeight",
], value=p.style, label="Style")
ui.render()
print(f"Matplotlib chart saved with style '{p.style}'")
