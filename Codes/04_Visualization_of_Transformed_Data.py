# Databricks notebook source
"""
Visualization of Gold Layer (Retail ETL Project)

- Reads from gold.sales_summary
- Creates business-ready plots:
  1. Total Sales by Category
  2. Total Profit by Region
"""

import os
import logging
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession
spark = SparkSession.builder.appName("Retail_Gold_Visualization").getOrCreate()

OUT_DIR = "/dbfs/tmp"
os.makedirs(OUT_DIR, exist_ok=True)


def plot_sales_by_category(df):
    pdf = df.toPandas()
    if pdf.empty or "Category" not in pdf.columns or "total_sales" not in pdf.columns:
        raise ValueError("Missing required columns for Sales by Category")
    ax = pdf.sort_values("total_sales", ascending=False).plot(
        kind="bar", x="Category", y="total_sales"
    )
    ax.set_title("Total Sales by Category")
    ax.set_xlabel("Category")
    ax.set_ylabel("Total Sales")
    plt.tight_layout()
    out = f"{OUT_DIR}/total_sales_by_category.png"
    try:
        plt.savefig(out)
        plt.show()
        logger.info("Saved: %s", out)
    except Exception as e:
        logger.error("Failed to save sales by category plot: %s", e)
        raise


def plot_profit_by_region(df):
    pdf = df.toPandas()
    if pdf.empty or "Region" not in pdf.columns or "total_profit" not in pdf.columns:
        raise ValueError("Missing required columns for Profit by Region")
    ax = pdf.sort_values("total_profit", ascending=False).plot(
        kind="bar", x="Region", y="total_profit"
    )
    ax.set_title("Total Profit by Region")
    ax.set_xlabel("Region")
    ax.set_ylabel("Total Profit")
    plt.tight_layout()
    out = f"{OUT_DIR}/total_profit_by_region.png"
    try:
        plt.savefig(out)
        plt.show()
        logger.info("Saved: %s", out)
    except Exception as e:
        logger.error("Failed to save profit by region plot: %s", e)
        raise


try:
    # Query from Gold Layer
    sales_summary = spark.sql(
        "SELECT Category, Region, total_sales, total_profit FROM gold.sales_summary"
    )
    logger.info("Loaded data from gold.sales_summary")

    # Create two plots
    plot_sales_by_category(sales_summary.select("Category", "total_sales").distinct())
    plot_profit_by_region(sales_summary.select("Region", "total_profit").distinct())

    logger.info("Visualization complete. Open images via:")
    logger.info("   /files/tmp/total_sales_by_category.png")
    logger.info("   /files/tmp/total_profit_by_region.png")

except AnalysisException as e:
    logger.error("SQL query error: %s", e)
except ValueError as e:
    logger.error("Data validation error: %s", e)
except Exception as e:
    logger.error("Unexpected error: %s", e)
