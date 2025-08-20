import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Codes.etl import run_etl


def test_repo_layout():
    assert os.path.exists("README.md")
    assert os.path.exists("Codes/01_ETL_Operations.py")
    assert os.path.exists("Codes/02_Delta_Lake_For_Storage.py")
    assert os.path.exists("Codes/03_Spark_SQL_for_DataTransformations.py")
    assert os.path.exists("Codes/04_Visualization_of_Transformed_Data.py")


def test_run_etl(spark):
    data = [("CA-2016-152156", "11/08/2016", "11/11/2016")]
    df = spark.createDataFrame(data, ["Order ID", "Order Date", "Ship Date"])
    result = run_etl(df)
    assert result.columns == ["order_id", "order_date", "ship_date"]
    row = result.collect()[0]
    assert row.order_id == "CA-2016-152156"
    assert str(row.order_date) == "2016-11-08"
