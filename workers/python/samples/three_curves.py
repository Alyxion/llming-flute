"""3D curves: Bézier, Catmull-Rom, and lines."""
import math

from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    turns: float = 3


p = load_params(Params)

# Generate a helix as catmull-rom points
helix = []
for i in range(60):
    t = i / 60 * p.turns * 2 * math.pi
    helix.append([math.cos(t) * 2, t / (2 * math.pi), math.sin(t) * 2])

ui = UI()
ui.three({
    "objects": [
        # Bézier curve
        {"type": "bezier", "points": [[-3, 0, 0], [-1, 3, 0], [1, -3, 0], [3, 0, 0]],
         "color": "#f38ba8", "segments": 80},
        # Catmull-Rom helix
        {"type": "catmull", "points": helix, "color": "#89b4fa"},
        # Simple line axes
        {"type": "line", "points": [[-4, 0, 0], [4, 0, 0]], "color": "#6c7086"},
        {"type": "line", "points": [[0, -1, 0], [0, 4, 0]], "color": "#6c7086"},
        {"type": "line", "points": [[0, 0, -4], [0, 0, 4]], "color": "#6c7086"},
    ],
    "background": "#1a1a2e",
}, camera={"position": [6, 4, 6]})
ui.slider("turns", min=1, max=8, step=0.5, value=p.turns, label="Helix turns")
ui.render()
print(f"Curves with {p.turns} helix turns")
