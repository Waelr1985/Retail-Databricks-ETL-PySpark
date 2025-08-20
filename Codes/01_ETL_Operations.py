# 01_ETL_Operations (Retail)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def run_etl_operations(
    spark: SparkSession,
    csv_path: str,
    bronze_table: str,
    silver_table: str,
) -> None:
    """Ingest raw data and write Bronze/Silver Delta tables."""
    raw = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

    df = (
        raw.withColumnRenamed("Order ID", "order_id")
        .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
        .withColumn("ship_date", to_date(col("Ship Date"), "MM/dd/yyyy"))
        .dropna(subset=["order_id", "order_date"])
    )

    raw.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    df.write.format("delta").mode("overwrite").saveAsTable(silver_table)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Retail_ETL").getOrCreate()
    run_etl_operations(
        spark,
        csv_path="/FileStore/tables/superstore.csv",
        bronze_table="bronze.superstore",
        silver_table="silver.superstore",
    )
