# Databricks notebook source
"""Visualization of Gold Layer (Retail ETL Project)."""

import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def plot_sales_by_category(df, out_dir: str) -> None:
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
    out = f"{out_dir}/total_sales_by_category.png"
    plt.savefig(out)
    plt.show()
    print(f"✅ Saved: {out}")


def plot_profit_by_region(df, out_dir: str) -> None:
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
    out = f"{out_dir}/total_profit_by_region.png"
    plt.savefig(out)
    plt.show()
    print(f"✅ Saved: {out}")


def visualize_transformed_data(
    spark: SparkSession, gold_table: str, out_dir: str
) -> None:
    """Read Gold data and generate plots."""
    os.makedirs(out_dir, exist_ok=True)

    try:
        sales_summary = spark.sql(
            f"SELECT Category, Region, total_sales, total_profit FROM {gold_table}"
        )

        plot_sales_by_category(
            sales_summary.select("Category", "total_sales").distinct(), out_dir
        )
        plot_profit_by_region(
            sales_summary.select("Region", "total_profit").distinct(), out_dir
        )

        print("🎉 Visualization complete. Open images via:")
        path_for_display = out_dir.replace("/dbfs", "/files")
        print(f"   {path_for_display}/total_sales_by_category.png")
        print(f"   {path_for_display}/total_profit_by_region.png")

    except AnalysisException as e:
        print(f"SQL query error: {e}")
    except ValueError as e:
        print(f"Data validation error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Retail_Gold_Visualization").getOrCreate()
    visualize_transformed_data(
        spark,
        gold_table="gold.sales_summary",
        out_dir="/dbfs/tmp",
    )
