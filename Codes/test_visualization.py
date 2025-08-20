import ast
import types
import sys
from pathlib import Path

import pytest
import matplotlib
matplotlib.use("Agg")


def load_viz_module(monkeypatch, tmp_path):
    codes_dir = Path(__file__).resolve().parents[1] / "Codes"
    sys.path.insert(0, str(codes_dir))

    # Ensure config picks up overridden OUTPUT_DIR
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import config
    importlib.reload(config)

    file_path = codes_dir / "04_Visualization_of_Transformed_Data.py"
    source = file_path.read_text()
    module_ast = ast.parse(source)
    new_body = []
    for node in module_ast.body:
        if isinstance(node, ast.Try):
            break
        new_body.append(node)
    module_ast.body = new_body

    module = types.ModuleType("viz")
    exec(compile(module_ast, str(file_path), "exec"), module.__dict__)
    monkeypatch.setattr(module.plt, "show", lambda: None)
    return module


def test_plot_sales_by_category_valid(tmp_path, monkeypatch):
    viz = load_viz_module(monkeypatch, tmp_path)
    df = viz.spark.createDataFrame(
        [("Furniture", 100.0), ("Technology", 200.0)],
        ["Category", "total_sales"],
    )
    viz.plot_sales_by_category(df)
    assert (Path(viz.OUT_DIR) / "total_sales_by_category.png").exists()


def test_plot_sales_by_category_invalid(tmp_path, monkeypatch):
    viz = load_viz_module(monkeypatch, tmp_path)
    df = viz.spark.createDataFrame([("Furniture",)], ["Category"])
    with pytest.raises(ValueError):
        viz.plot_sales_by_category(df)


def test_plot_profit_by_region_valid(tmp_path, monkeypatch):
    viz = load_viz_module(monkeypatch, tmp_path)
    df = viz.spark.createDataFrame(
        [("East", 50.0), ("West", 80.0)],
        ["Region", "total_profit"],
    )
    viz.plot_profit_by_region(df)
    assert (Path(viz.OUT_DIR) / "total_profit_by_region.png").exists()


def test_plot_profit_by_region_invalid(tmp_path, monkeypatch):
    viz = load_viz_module(monkeypatch, tmp_path)
    df = viz.spark.createDataFrame([("East",)], ["Region"])
    with pytest.raises(ValueError):
        viz.plot_profit_by_region(df)
