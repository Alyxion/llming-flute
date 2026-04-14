"""Data table with sorting support."""
from flute.ui import UI

data = [
    {"name": "Alice", "department": "Engineering", "salary": 120000, "years": 5},
    {"name": "Bob", "department": "Marketing", "salary": 85000, "years": 3},
    {"name": "Charlie", "department": "Engineering", "salary": 135000, "years": 8},
    {"name": "Diana", "department": "Design", "salary": 95000, "years": 4},
    {"name": "Eve", "department": "Engineering", "salary": 110000, "years": 2},
    {"name": "Frank", "department": "Marketing", "salary": 90000, "years": 6},
    {"name": "Grace", "department": "Design", "salary": 105000, "years": 7},
    {"name": "Hank", "department": "Engineering", "salary": 145000, "years": 10},
]

columns = [
    {"name": "name", "label": "Name", "field": "name"},
    {"name": "department", "label": "Department", "field": "department"},
    {"name": "salary", "label": "Salary ($)", "field": "salary"},
    {"name": "years", "label": "Years", "field": "years"},
]

ui = UI()
ui.table(columns, data, title="Employee Directory")
ui.render()
print(f"Table with {len(data)} rows")
