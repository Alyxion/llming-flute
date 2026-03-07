"""
End-to-end integration tests.

Require running Docker stack:
    docker compose up -d
    REDIS_URL=redis://localhost:6399/0 pytest tests/test_integration.py
"""

import textwrap

import pytest

from flute.client import SessionClient

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def c():
    return SessionClient()


# ---------------------------------------------------------------------------
# Typical ChatGPT-style apps
# ---------------------------------------------------------------------------


def test_basic_execution(c):
    sid = c.submit("print('hello world')", session_id="test-basic")
    res = c.wait(sid)
    assert res["status"] == "completed"
    assert "hello world" in res["logs"]


def test_input_files(c):
    code = textwrap.dedent("""\
        with open("input.txt") as f:
            data = f.read()
        with open("output.txt", "w") as f:
            f.write(data.upper())
    """)
    sid = c.submit(
        code, session_id="test-files", input_files={"input.txt": b"hello from input file"}
    )
    res = c.wait(sid)
    assert res["status"] == "completed"
    assert res["output_files"]["output.txt"] == b"HELLO FROM INPUT FILE"


def test_matplotlib_chart(c):
    code = textwrap.dedent("""\
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        x = np.linspace(0, 2 * np.pi, 200)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(x, np.sin(x), label="sin(x)")
        ax.plot(x, np.cos(x), label="cos(x)")
        ax.set_title("Trigonometric Functions")
        ax.legend()
        ax.grid(True)
        fig.savefig("chart.png", dpi=150)
        print("chart saved")
    """)
    sid = c.submit(code, session_id="test-matplotlib", max_runtime_seconds=30, max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    png = res["output_files"]["chart.png"]
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(png) > 10_000


def test_pandas_csv_analysis(c):
    csv_data = (
        b"name,age,city\nAlice,30,Berlin\nBob,25,Munich\n"
        b"Charlie,35,Berlin\nDiana,28,Hamburg\nEve,32,Berlin"
    )
    code = textwrap.dedent("""\
        import pandas as pd
        df = pd.read_csv("people.csv")
        summary = df.groupby("city").agg(count=("name","count"), avg_age=("age","mean"))
        summary.reset_index().to_csv("summary.csv", index=False)
        print("done")
    """)
    sid = c.submit(
        code, session_id="test-pandas", input_files={"people.csv": csv_data}, max_memory_mb=256
    )
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert "Berlin" in res["output_files"]["summary.csv"].decode()


def test_numpy_computation(c):
    code = textwrap.dedent("""\
        import numpy as np
        np.random.seed(42)
        A = np.random.randn(500, 500)
        B = np.random.randn(500, 500)
        C = A @ B
        print(f"C shape: {C.shape}")
        np.save("result.npy", C)
    """)
    sid = c.submit(code, session_id="test-numpy", max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert "C shape: (500, 500)" in res["logs"]
    assert "result.npy" in res["output_files"]


def test_excel_generation(c):
    code = textwrap.dedent("""\
        import openpyxl
        from openpyxl.styles import Font, PatternFill
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Sales"
        for col, h in enumerate(["Product","Q1","Q2","Total"], 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font = Font(bold=True)
        data = [("Widget",100,200), ("Gadget",300,400)]
        for row_idx, (name, q1, q2) in enumerate(data, 2):
            ws.cell(row=row_idx, column=1, value=name)
            ws.cell(row=row_idx, column=2, value=q1)
            ws.cell(row=row_idx, column=3, value=q2)
            ws.cell(row=row_idx, column=4, value=q1+q2)
        wb.save("report.xlsx")
    """)
    sid = c.submit(code, session_id="test-excel", max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert res["output_files"]["report.xlsx"][:2] == b"PK"


def test_multi_plot_report(c):
    code = textwrap.dedent("""\
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        np.random.seed(0)
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        axes[0,0].hist(np.random.normal(0,1,1000), bins=30)
        axes[0,0].set_title("Histogram")
        x,y = np.random.randn(2,200)
        axes[0,1].scatter(x, y, alpha=0.5)
        axes[0,1].set_title("Scatter")
        axes[1,0].bar(["A","B","C"], [10,20,30])
        axes[1,0].set_title("Bar")
        t = np.linspace(0,10,100)
        axes[1,1].plot(t, np.sin(t))
        axes[1,1].set_title("Line")
        fig.tight_layout()
        fig.savefig("report.png", dpi=150)
    """)
    sid = c.submit(code, session_id="test-multiplot", max_runtime_seconds=30, max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert res["output_files"]["report.png"][:8] == b"\x89PNG\r\n\x1a\n"
    assert len(res["output_files"]["report.png"]) > 30_000


def test_image_processing(c):
    code = textwrap.dedent("""\
        from PIL import Image, ImageDraw
        img = Image.new("RGB", (400, 200), color=(30, 30, 30))
        draw = ImageDraw.Draw(img)
        draw.rectangle([20, 20, 380, 180], outline="white", width=3)
        draw.text((50, 80), "Hello from Flute!", fill="cyan")
        rotated = img.rotate(15, expand=True, fillcolor=(30, 30, 30))
        rotated.save("greeting.png")
    """)
    sid = c.submit(code, session_id="test-pillow", max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert res["output_files"]["greeting.png"][:8] == b"\x89PNG\r\n\x1a\n"


def test_sympy_math(c):
    code = textwrap.dedent("""\
        from sympy import symbols, integrate, solve, sin, cos
        x = symbols("x")
        expr = sin(x)**2 + cos(x)**2
        print(f"sin^2+cos^2 = {expr.simplify()}")
        roots = solve(x**3 - 6*x**2 + 11*x - 6, x)
        print(f"roots = {roots}")
        with open("results.txt","w") as f:
            f.write(f"roots: {roots}\\n")
    """)
    sid = c.submit(code, session_id="test-sympy", max_runtime_seconds=30, max_memory_mb=256)
    res = c.wait(sid, timeout=30)
    assert res["status"] == "completed"
    assert "sin^2+cos^2 = 1" in res["logs"]
    assert "[1, 2, 3]" in res["logs"]
    assert "results.txt" in res["output_files"]


def test_multiple_output_files(c):
    code = textwrap.dedent("""\
        for i in range(3):
            with open(f"result_{i}.txt", "w") as f:
                f.write(f"result {i}")
    """)
    sid = c.submit(code, session_id="test-multi-out")
    res = c.wait(sid)
    assert res["status"] == "completed"
    assert len(res["output_files"]) == 3


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_syntax_error(c):
    sid = c.submit("def foo(\n", session_id="test-syntax")
    res = c.wait(sid)
    assert res["status"] == "error"
    assert "SyntaxError" in res["logs"]


def test_exception(c):
    sid = c.submit("raise ValueError('test error')", session_id="test-exception")
    res = c.wait(sid)
    assert res["status"] == "error"
    assert "test error" in res["logs"]


# ---------------------------------------------------------------------------
# Bust tests: disk
# ---------------------------------------------------------------------------


def test_bust_disk_single_large_file(c):
    code = textwrap.dedent("""\
        with open("bigfile.bin", "wb") as f:
            for i in range(500):
                f.write(b"X" * (1024 * 1024))
                f.flush()
        print("SHOULD NOT REACH HERE")
    """)
    sid = c.submit(code, session_id="test-bust-disk-single", max_disk_mb=10, max_runtime_seconds=30)
    res = c.wait(sid, timeout=35)
    assert res["status"] in ("killed", "error")
    assert "SHOULD NOT REACH HERE" not in res["logs"]


def test_bust_disk_many_small_files(c):
    code = textwrap.dedent("""\
        for i in range(200):
            with open(f"chunk_{i:04d}.bin", "wb") as f:
                f.write(b"Y" * (1024 * 1024))
        print("SHOULD NOT REACH HERE")
    """)
    sid = c.submit(code, session_id="test-bust-disk-many", max_disk_mb=10, max_runtime_seconds=30)
    res = c.wait(sid, timeout=35)
    assert res["status"] in ("killed", "error")
    assert "SHOULD NOT REACH HERE" not in res["logs"]


def test_bust_disk_nested_dirs(c):
    code = textwrap.dedent("""\
        import os, time
        for depth in range(50):
            path = os.path.join(*[f"d{d}" for d in range(depth + 1)])
            os.makedirs(path, exist_ok=True)
            for j in range(3):
                with open(os.path.join(path, f"data_{j}.bin"), "wb") as f:
                    f.write(b"Z" * (1024 * 1024))
            time.sleep(0.05)
        print("SHOULD NOT REACH HERE")
    """)
    sid = c.submit(code, session_id="test-bust-disk-nested", max_disk_mb=10, max_runtime_seconds=30)
    res = c.wait(sid, timeout=35)
    assert res["status"] in ("killed", "error")
    assert "SHOULD NOT REACH HERE" not in res["logs"]


def test_bust_disk_tmpfs_preserved(c):
    # First: bust attempt
    bust_code = textwrap.dedent("""\
        with open("bigfile.bin", "wb") as f:
            for i in range(100):
                f.write(b"A" * (1024 * 1024))
                f.flush()
    """)
    sid1 = c.submit(bust_code, session_id="test-disk-cleanup-bust", max_disk_mb=5,
                    max_runtime_seconds=15)
    c.wait(sid1, timeout=20)

    # Then: verify cleanup
    check_code = textwrap.dedent("""\
        import shutil
        usage = shutil.disk_usage("/tmp")
        used_mb = usage.used / (1024 * 1024)
        assert used_mb < 50, f"tmpfs not cleaned: {used_mb:.1f} MB"
        print("DISK CLEAN")
    """)
    sid2 = c.submit(check_code, session_id="test-disk-cleanup-verify", max_memory_mb=256)
    res2 = c.wait(sid2, timeout=15)
    assert res2["status"] == "completed"
    assert "DISK CLEAN" in res2["logs"]


# ---------------------------------------------------------------------------
# Bust tests: memory
# ---------------------------------------------------------------------------


def test_bust_memory_fast_alloc(c):
    code = textwrap.dedent("""\
        blocks = []
        for i in range(200):
            blocks.append(bytearray(10 * 1024 * 1024))
        print("SHOULD NOT REACH HERE")
    """)
    sid = c.submit(code, session_id="test-bust-mem-fast", max_memory_mb=64,
                   max_runtime_seconds=15)
    res = c.wait(sid, timeout=20)
    assert res["status"] in ("killed", "error")
    assert "SHOULD NOT REACH HERE" not in res["logs"]


def test_bust_memory_mmap(c):
    code = textwrap.dedent("""\
        import mmap
        maps = []
        for i in range(100):
            m = mmap.mmap(-1, 10 * 1024 * 1024)
            m.write(b"X" * (10 * 1024 * 1024))
            maps.append(m)
        print("SHOULD NOT REACH HERE")
    """)
    sid = c.submit(code, session_id="test-bust-mem-mmap", max_memory_mb=64,
                   max_runtime_seconds=15)
    res = c.wait(sid, timeout=20)
    assert res["status"] in ("killed", "error")
    assert "SHOULD NOT REACH HERE" not in res["logs"]


# ---------------------------------------------------------------------------
# Bust tests: loops
# ---------------------------------------------------------------------------


def test_bust_loop_busy_spin(c):
    code = "i = 0\nwhile True:\n    i += 1"
    sid = c.submit(code, session_id="test-bust-loop-spin", max_runtime_seconds=3)
    res = c.wait(sid, timeout=15)
    assert res["status"] == "killed"
    assert "runtime" in res["error"].lower()


def test_bust_loop_sleep(c):
    code = "import time\nwhile True:\n    time.sleep(0.01)"
    sid = c.submit(code, session_id="test-bust-loop-sleep", max_runtime_seconds=3)
    res = c.wait(sid, timeout=15)
    assert res["status"] == "killed"
    assert "runtime" in res["error"].lower()


def test_bust_loop_fork_bomb(c):
    code = textwrap.dedent("""\
        import subprocess, sys
        children = []
        for i in range(200):
            try:
                p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
                children.append(p.pid)
            except Exception as e:
                print(f"fork blocked at {i}: {e}")
                break
        print(f"spawned {len(children)} children")
    """)
    sid = c.submit(code, session_id="test-bust-fork", max_runtime_seconds=10, max_memory_mb=256)
    res = c.wait(sid, timeout=20)
    assert res["status"] in ("killed", "error", "completed")
    if res["status"] == "completed":
        assert "fork blocked" in res["logs"]


def test_bust_loop_subprocess_chain(c):
    code = textwrap.dedent("""\
        import subprocess, sys
        subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(999)"],
            start_new_session=True,
        )
        import time; time.sleep(999)
    """)
    sid = c.submit(code, session_id="test-bust-orphan", max_runtime_seconds=3)
    res = c.wait(sid, timeout=15)
    assert res["status"] in ("killed", "error")
