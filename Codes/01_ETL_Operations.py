# 01_ETL_Operations (Retail)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import config


def run_etl(
    csv_path: str,
    bronze_table: str,
    silver_table: str,
    spark: SparkSession | None = None,
):
    """Ingest a CSV file and create Bronze and Silver Delta tables."""
    spark = spark or SparkSession.builder.appName("Retail_ETL").getOrCreate()

    # fmt: off
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )
    # fmt: on

    df = (
        raw.withColumnRenamed("Order ID", "order_id")
        .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
        .withColumn("ship_date", to_date(col("Ship Date"), "MM/dd/yyyy"))
        .dropna(subset=["order_id", "order_date"])
    )

    raw.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    return df


if __name__ == "__main__":
    run_etl(config.CSV_PATH, config.BRONZE_TABLE, config.SILVER_TABLE)
