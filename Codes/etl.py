from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


def run_etl(df: DataFrame) -> DataFrame:
    """Perform a minimal ETL transformation on the given DataFrame."""
    return (
        df.withColumnRenamed("Order ID", "order_id")
        .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
        .withColumn("ship_date", to_date(col("Ship Date"), "MM/dd/yyyy"))
        .drop("Order Date", "Ship Date")
    )
