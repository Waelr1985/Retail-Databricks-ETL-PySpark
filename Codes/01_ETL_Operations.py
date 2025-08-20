# 01_ETL_Operations (Retail)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    DoubleType,
)
import config

# Explicit schema for the retail sales CSV. Using a static schema avoids the
# overhead of schema inference and ensures consistent column types across runs.
SALES_SCHEMA = StructType(
    [
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", IntegerType(), True),
        StructField("Region", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
    ]
)


def run_etl(
    csv_path: str,
    bronze_table: str,
    silver_table: str,
    spark: SparkSession | None = None,
):
    """Ingest a CSV file using ``SALES_SCHEMA`` and create Delta tables.

    The explicit schema prevents Spark from inferring column types at run
    time, which keeps ETL jobs deterministic and slightly faster.
    """
    spark = spark or SparkSession.builder.appName("Retail_ETL").getOrCreate()

    # fmt: off
    raw = (
        spark.read
        .option("header", True)
        .schema(SALES_SCHEMA)
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
