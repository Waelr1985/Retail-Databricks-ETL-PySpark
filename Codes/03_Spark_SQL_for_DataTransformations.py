# 03_Spark_SQL_For_DataTransformation (Retail → Gold)
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("Retail_Gold_Build").getOrCreate()

try:
    silver = spark.table("silver.superstore_partitioned")
    logger.info("Loaded table silver.superstore_partitioned")
except Exception as e:
    logger.error("Failed to load table silver.superstore_partitioned: %s", e)
    raise

gold = silver.groupBy("Category", "Region").agg(
    _sum("Sales").alias("total_sales"), _sum("Profit").alias("total_profit")
)

try:
    gold.write.format("delta").mode("overwrite").saveAsTable("gold.sales_summary")
    logger.info("Gold table saved to gold.sales_summary")
except Exception as e:
    logger.error("Failed to save gold table: %s", e)
    raise

# sanity check (works in Databricks notebooks)
try:
    display(spark.table("gold.sales_summary").orderBy("total_sales", ascending=False))
except NameError:
    # display() not available outside notebook
    logger.info(
        spark.table("gold.sales_summary")
        .orderBy("total_sales", ascending=False)
        .limit(10)
        .toPandas()
    )

logger.info("Gold table created: gold.sales_summary")
