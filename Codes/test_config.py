import importlib
import sys
from pathlib import Path

import pytest

# Ensure the Codes directory is on sys.path so `config` can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "Codes"))


def test_environment_overrides(monkeypatch):
    overrides = {
        "CSV_PATH": "/tmp/override.csv",
        "BRONZE_TABLE": "bronze.override",
        "SILVER_TABLE": "silver.override",
        "SILVER_PARTITIONED_TABLE": "silver.override_part",
        "GOLD_TABLE": "gold.override",
        "OUTPUT_DIR": "/tmp/output_override",
    }
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)

    import config
    importlib.reload(config)

    for key, value in overrides.items():
        assert getattr(config, key) == value
