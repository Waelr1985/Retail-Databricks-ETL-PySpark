from pyspark.sql import SparkSession
import config

# 02_Delta_Lake_For_Storage (Retail)
spark = SparkSession.builder.appName("Retail_Delta_Storage").getOrCreate()

silver = spark.table(config.SILVER_TABLE)

# partition for scale (pick one: Region or order_date)
(silver.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("Region")
 .saveAsTable(config.SILVER_PARTITIONED_TABLE))

# (optional) compaction ops if enabled in your workspace:
# spark.sql(f"OPTIMIZE {config.SILVER_PARTITIONED_TABLE} ZORDER BY (Category, Region)")
print(f"✅ Silver partitioned table created: {config.SILVER_PARTITIONED_TABLE}")
