import os

def test_repo_layout():
    assert os.path.exists("README.md")
    assert os.path.exists("01_ETL_Operations.py")
    assert os.path.exists("02_Delta_Lake_For_Storage.py")
    assert os.path.exists("03_Spark_SQL_for_DataTransformations.py")
    assert os.path.exists("04_Visualization_of_Transformed_Data.py")
