from pyspark.sql import SparkSession
import config


def run_partition(
    silver_table: str,
    partitioned_table: str,
    partition_col: str = "Region",
    spark: SparkSession | None = None,
):
    """Partition the Silver table and write it back as a new Delta table."""
    # fmt: off
    spark = spark or SparkSession.builder.appName(
        "Retail_Delta_Storage"
    ).getOrCreate()
    # fmt: on

    silver = spark.table(silver_table)

    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition_col)
        .saveAsTable(partitioned_table)
    )

    # (optional) compaction ops if enabled in your workspace:
    # spark.sql(
    #     f"OPTIMIZE {partitioned_table} ZORDER BY (Category, {partition_col})"
    # )
    print(f"✅ Silver partitioned table created: {partitioned_table}")


if __name__ == "__main__":
    run_partition(
        config.SILVER_TABLE,
        config.SILVER_PARTITIONED_TABLE,
    )
