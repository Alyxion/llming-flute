"""3D scene with primitive shapes."""
from flute.ui import UI

ui = UI()
ui.three({
    "objects": [
        {"type": "box", "size": [1, 1, 1], "position": [-2, 0, 0], "color": "#f38ba8"},
        {"type": "sphere", "radius": 0.6, "position": [0, 0, 0], "color": "#89b4fa"},
        {"type": "cylinder", "radiusTop": 0.3, "radiusBottom": 0.5, "height": 1.2,
         "position": [2, 0, 0], "color": "#a6e3a1"},
        {"type": "box", "size": [0.6, 0.6, 0.6], "position": [0, 1.5, 0],
         "color": "#f9e2af", "rotation": [0.5, 0.5, 0]},
    ],
    "background": "#1a1a2e",
    "ambient_light": 0.5,
    "directional_light": {"intensity": 0.8, "position": [5, 10, 7]},
})
ui.render()
print("Rendered 3D primitives")
