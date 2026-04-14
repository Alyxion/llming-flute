"""Generate a table from Pandas DataFrame analysis."""
import numpy as np
import pandas as pd
from flute.ui import UI, load_params
from pydantic import BaseModel


class Params(BaseModel):
    n: int = 1000


p = load_params(Params)

np.random.seed(42)
df = pd.DataFrame({
    "category": np.random.choice(["A", "B", "C", "D"], p.n),
    "value": np.random.normal(100, 25, p.n),
    "quantity": np.random.poisson(10, p.n),
})

summary = df.groupby("category").agg(
    count=("value", "count"),
    mean_value=("value", "mean"),
    std_value=("value", "std"),
    total_qty=("quantity", "sum"),
).round(2).reset_index()

columns = [
    {"name": "category", "label": "Category", "field": "category"},
    {"name": "count", "label": "Count", "field": "count"},
    {"name": "mean_value", "label": "Mean Value", "field": "mean_value"},
    {"name": "std_value", "label": "Std Dev", "field": "std_value"},
    {"name": "total_qty", "label": "Total Qty", "field": "total_qty"},
]

ui = UI()
ui.table(columns, summary.to_dict("records"), title=f"Summary of {p.n} samples")
ui.slider("n", min=100, max=5000, step=100, value=p.n, label="Sample size")
ui.render()
print(f"Analyzed {p.n} samples across {len(summary)} categories")
