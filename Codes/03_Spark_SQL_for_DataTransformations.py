from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import config


def run_gold(
    silver_partitioned_table: str,
    gold_table: str,
    spark: SparkSession | None = None,
):
    """Aggregate Silver data into a Gold summary table."""
    # fmt: off
    spark = spark or SparkSession.builder.appName(
        "Retail_Gold_Build"
    ).getOrCreate()
    # fmt: on

    silver = spark.table(silver_partitioned_table)

    gold = silver.groupBy("Category", "Region").agg(
        _sum("Sales").alias("total_sales"),
        _sum("Profit").alias("total_profit"),
    )

    gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)

    # sanity check (works in Databricks notebooks)
    try:
        # fmt: off
        display(
            spark.table(gold_table).orderBy("total_sales", ascending=False)
        )
        # fmt: on
    except NameError:
        # display() not available outside notebook
        print(
            spark.table(gold_table)
            .orderBy("total_sales", ascending=False)
            .limit(10)
            .toPandas()
        )

    print(f"✅ Gold table created: {gold_table}")


if __name__ == "__main__":
    run_gold(config.SILVER_PARTITIONED_TABLE, config.GOLD_TABLE)
