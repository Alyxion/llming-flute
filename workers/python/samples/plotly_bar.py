"""Bar chart comparing categories."""
from flute.ui import UI

categories = ["Python", "JavaScript", "Rust", "Go", "Java", "C++"]
popularity = [30, 25, 12, 10, 15, 8]
growth = [5, 3, 18, 8, -2, 1]

ui = UI()
ui.plotly(
    data=[
        {"x": categories, "y": popularity, "type": "bar", "name": "Popularity %"},
        {"x": categories, "y": growth, "type": "bar", "name": "YoY Growth %"},
    ],
    layout={"title": "Language Trends", "barmode": "group"},
)
ui.render()
print("Rendered bar chart")
