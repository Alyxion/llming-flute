"""3D custom mesh: icosahedron built from vertices and faces."""
import math

from flute.ui import UI

# Build icosahedron vertices
phi = (1 + math.sqrt(5)) / 2  # golden ratio
verts = [
    [-1, phi, 0], [1, phi, 0], [-1, -phi, 0], [1, -phi, 0],
    [0, -1, phi], [0, 1, phi], [0, -1, -phi], [0, 1, -phi],
    [phi, 0, -1], [phi, 0, 1], [-phi, 0, -1], [-phi, 0, 1],
]
# Normalize to unit sphere
for v in verts:
    length = math.sqrt(sum(c ** 2 for c in v))
    v[0] /= length
    v[1] /= length
    v[2] /= length
    # Scale up
    v[0] *= 2
    v[1] *= 2
    v[2] *= 2

faces = [
    [0, 11, 5], [0, 5, 1], [0, 1, 7], [0, 7, 10], [0, 10, 11],
    [1, 5, 9], [5, 11, 4], [11, 10, 2], [10, 7, 6], [7, 1, 8],
    [3, 9, 4], [3, 4, 2], [3, 2, 6], [3, 6, 8], [3, 8, 9],
    [4, 9, 5], [2, 4, 11], [6, 2, 10], [8, 6, 7], [9, 8, 1],
]

ui = UI()
ui.three({
    "objects": [
        {"type": "mesh", "vertices": verts, "faces": faces, "color": "#cba6f7"},
        # Wireframe version shifted
        {"type": "mesh", "vertices": [[v[0] + 5, v[1], v[2]] for v in verts],
         "faces": faces, "color": "#89b4fa"},
    ],
    "background": "#1a1a2e",
    "ambient_light": 0.5,
    "directional_light": {"intensity": 0.7, "position": [5, 8, 5]},
}, camera={"position": [4, 4, 8]})
ui.render()
print("Icosahedron mesh rendered")
