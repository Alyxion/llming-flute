"""3D mathematical surface plot."""
from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    resolution: int = 40
    wireframe: bool = False


p = load_params(Params)

ui = UI()
ui.three(
    {
        "objects": [
            {
                "type": "surface",
                "func": "Math.sin(Math.sqrt(x*x + y*y)) * 2",
                "range": [[-5, 5], [-5, 5]],
                "resolution": p.resolution,
                "color": "#89b4fa",
                "wireframe": p.wireframe,
            },
        ],
        "background": "#1a1a2e",
        "ambient_light": 0.4,
        "directional_light": {"intensity": 1.0, "position": [5, 10, 5]},
    },
    camera={"position": [8, 6, 8], "fov": 50},
)
ui.slider("resolution", min=10, max=80, step=5, value=p.resolution, label="Resolution")
ui.checkbox("wireframe", value=p.wireframe, label="Wireframe")
ui.render()
print(f"Surface at resolution {p.resolution}, wireframe={p.wireframe}")
