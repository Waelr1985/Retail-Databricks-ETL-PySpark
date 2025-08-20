from pyspark.sql import SparkSession
import logging

# 02_Delta_Lake_For_Storage (Retail)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("Retail_Delta_Storage").getOrCreate()

try:
    silver = spark.table("silver.superstore")
    logger.info("Loaded table silver.superstore")
except Exception as e:
    logger.error("Failed to load table silver.superstore: %s", e)
    raise

# partition for scale (pick one: Region or order_date)
try:
    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("Region")
        .saveAsTable("silver.superstore_partitioned")
    )
    logger.info("Silver table partitioned to silver.superstore_partitioned")
except Exception as e:
    logger.error("Failed to write partitioned table: %s", e)
    raise

# (optional) compaction ops if enabled in your workspace:
# spark.sql("OPTIMIZE silver.superstore_partitioned ZORDER BY (Category, Region)")
logger.info("Silver partitioned table created: silver.superstore_partitioned")
