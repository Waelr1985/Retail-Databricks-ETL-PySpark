"""Build the Gold layer using Spark SQL transformations."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum


def transform_to_gold(
    spark: SparkSession,
    source_table: str,
    target_table: str,
) -> None:
    """Aggregate Silver data and persist a Gold table."""
    silver = spark.table(source_table)

    gold = silver.groupBy("Category", "Region").agg(
        _sum("Sales").alias("total_sales"),
        _sum("Profit").alias("total_profit"),
    )

    gold.write.format("delta").mode("overwrite").saveAsTable(target_table)

    # sanity check (works in Databricks notebooks)
    try:
        display(spark.table(target_table).orderBy("total_sales", ascending=False))
    except NameError:
        # display() not available outside notebook
        print(
            spark.table(target_table)
            .orderBy("total_sales", ascending=False)
            .limit(10)
            .toPandas()
        )

    print(f"✅ Gold table created: {target_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Retail_Gold_Build").getOrCreate()
    transform_to_gold(
        spark,
        source_table="silver.superstore_partitioned",
        target_table="gold.sales_summary",
    )
