import os

CSV_PATH = os.getenv("CSV_PATH", "/FileStore/tables/superstore.csv")
BRONZE_TABLE = os.getenv("BRONZE_TABLE", "bronze.superstore")
SILVER_TABLE = os.getenv("SILVER_TABLE", "silver.superstore")
SILVER_PARTITIONED_TABLE = os.getenv("SILVER_PARTITIONED_TABLE", "silver.superstore_partitioned")
GOLD_TABLE = os.getenv("GOLD_TABLE", "gold.sales_summary")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/dbfs/tmp")
