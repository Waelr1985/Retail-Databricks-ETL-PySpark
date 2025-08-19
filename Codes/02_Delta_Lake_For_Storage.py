from pyspark.sql import SparkSession

# 02_Delta_Lake_For_Storage (Retail)
spark = SparkSession.builder.appName("Retail_Delta_Storage").getOrCreate()

silver = spark.table("silver.superstore")

# partition for scale (pick one: Region or order_date)
(silver.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("Region")
 .saveAsTable("silver.superstore_partitioned"))

# (optional) compaction ops if enabled in your workspace:
# spark.sql("OPTIMIZE silver.superstore_partitioned ZORDER BY (Category, Region)")
print("✅ Silver partitioned table created: silver.superstore_partitioned")
