import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
CODES_DIR = ROOT_DIR / "Codes"


def test_repo_layout():
    assert (ROOT_DIR / "README.md").exists()
    for fname in [
        "01_ETL_Operations.py",
        "02_Delta_Lake_For_Storage.py",
        "03_Spark_SQL_for_DataTransformations.py",
        "04_Visualization_of_Transformed_Data.py",
    ]:
        assert (CODES_DIR / fname).exists()
