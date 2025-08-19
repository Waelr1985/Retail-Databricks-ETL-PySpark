# 03_Spark_SQL_For_DataTransformation (Retail → Gold)
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("Retail_Gold_Build").getOrCreate()

silver = spark.table("silver.superstore_partitioned")

gold = (silver.groupBy("Category","Region")
              .agg(_sum("Sales").alias("total_sales"),
                   _sum("Profit").alias("total_profit")))

gold.write.format("delta").mode("overwrite").saveAsTable("gold.sales_summary")

# sanity check (works in Databricks notebooks)
try:
    display(spark.table("gold.sales_summary").orderBy("total_sales", ascending=False))
except NameError:
    # display() not available outside notebook
    print(spark.table("gold.sales_summary").orderBy("total_sales", ascending=False).limit(10).toPandas())

print("✅ Gold table created: gold.sales_summary")
