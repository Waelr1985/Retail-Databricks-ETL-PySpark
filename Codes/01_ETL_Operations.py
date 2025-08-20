# 01_ETL_Operations (Retail)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SparkSession is auto-created in Databricks, but you can include this for standalone PySpark
spark = SparkSession.builder.appName("Retail_ETL").getOrCreate()

# Bronze ingestion
csv_path = "/FileStore/tables/superstore.csv"
try:
    raw = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    logger.info("Read CSV successfully from %s", csv_path)
except Exception as e:
    logger.error("Failed to read CSV from %s: %s", csv_path, e)
    raise

# Quick clean for Silver
df = (
    raw.withColumnRenamed("Order ID", "order_id")
    .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
    .withColumn("ship_date", to_date(col("Ship Date"), "MM/dd/yyyy"))
    .dropna(subset=["order_id", "order_date"])
)

# Save Bronze & Silver tables
try:
    raw.write.format("delta").mode("overwrite").saveAsTable("bronze.superstore")
    logger.info("Bronze table saved to bronze.superstore")
except Exception as e:
    logger.error("Failed to write bronze table: %s", e)
    raise

try:
    df.write.format("delta").mode("overwrite").saveAsTable("silver.superstore")
    logger.info("Silver table saved to silver.superstore")
except Exception as e:
    logger.error("Failed to write silver table: %s", e)
    raise
