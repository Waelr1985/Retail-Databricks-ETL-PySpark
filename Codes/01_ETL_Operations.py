# 01_ETL_Operations (Retail)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# SparkSession is auto-created in Databricks, but you can include this for standalone PySpark
spark = SparkSession.builder.appName("Retail_ETL").getOrCreate()

# Bronze ingestion
csv_path = "/FileStore/tables/superstore.csv"
raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

# Quick clean for Silver
df = (
    raw.withColumnRenamed("Order ID", "order_id")
       .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
       .withColumn("ship_date",  to_date(col("Ship Date"),  "MM/dd/yyyy"))
       .dropna(subset=["order_id", "order_date"])
)

# Save Bronze & Silver tables
raw.write.format("delta").mode("overwrite").saveAsTable("bronze.superstore")
df.write.format("delta").mode("overwrite").saveAsTable("silver.superstore")
