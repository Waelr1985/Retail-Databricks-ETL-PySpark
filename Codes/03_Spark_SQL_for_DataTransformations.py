# 03_Spark_SQL_For_DataTransformation (Retail → Gold)
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import config

spark = SparkSession.builder.appName("Retail_Gold_Build").getOrCreate()

silver = spark.table(config.SILVER_PARTITIONED_TABLE)

gold = (silver.groupBy("Category","Region")
              .agg(_sum("Sales").alias("total_sales"),
                   _sum("Profit").alias("total_profit")))

gold.write.format("delta").mode("overwrite").saveAsTable(config.GOLD_TABLE)

# sanity check (works in Databricks notebooks)
try:
    display(spark.table(config.GOLD_TABLE).orderBy("total_sales", ascending=False))
except NameError:
    # display() not available outside notebook
    print(spark.table(config.GOLD_TABLE).orderBy("total_sales", ascending=False).limit(10).toPandas())

print(f"✅ Gold table created: {config.GOLD_TABLE}")
