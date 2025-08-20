from pyspark.sql import SparkSession


def delta_lake_for_storage(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    partition_column: str,
) -> None:
    """Partition and store data in Delta Lake."""
    silver = spark.table(source_table)

    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition_column)
        .saveAsTable(target_table)
    )

    # (optional) compaction ops if enabled in your workspace:
    # spark.sql(f"OPTIMIZE {target_table} ZORDER BY (Category, Region)")
    print(f"✅ Silver partitioned table created: {target_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Retail_Delta_Storage").getOrCreate()
    delta_lake_for_storage(
        spark,
        source_table="silver.superstore",
        target_table="silver.superstore_partitioned",
        partition_column="Region",
    )
