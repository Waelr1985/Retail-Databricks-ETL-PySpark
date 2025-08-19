# 🛍️ Retail Databricks ETL Pipeline

## Overview
This project demonstrates an **end-to-end ETL pipeline** using **Databricks + PySpark** on a retail dataset (*Sample Superstore*).
It follows the **Medallion Architecture** (Bronze → Silver → Gold) and shows how raw sales data becomes business-ready analytics.

## Architecture
- **Bronze** → Ingest raw retail CSV into Delta Lake.
- **Silver** → Clean/standardize (dates, null handling, renaming) and store in Delta (partitioned).
- **Gold** → Aggregate sales & profit by Category and Region for BI.
- **Visualization** → Create charts from Gold tables and save PNGs to DBFS.

## Repository Structure
```
RETAIL-DATABRICKS-ETL/
├── Codes/
│   ├── 01_ETL_Operations.py                  # Bronze & Silver ingestion/cleaning
│   ├── 02_Delta_Lake_For_Storage.py          # Partitioned Silver storage (Delta)
│   ├── 03_Spark_SQL_for_DataTransformations.py  # Gold aggregations
│   ├── 04_Visualization_of_Transformed_Data.py  # Charts (sales/profit)
│   ├── main.py                               # Entry stub (instructions / orchestration)
│   └── Test_main.py                          # Repo layout sanity test (pytest)
├── Data/
│   └── Sample - Superstore.csv               # Retail dataset (upload into DBFS for CE)
├── Resources/                                # Images used in README/demos
│   ├── 1116_ETL_Operations - Databricks.png
│   ├── 1116_Delta_Lake_For_Storage - Databricks.png
│   ├── 1116_Spark_SQL_for_DataTransformations - Databricks.png
│   ├── 1116_Automated Workflow - Databricks.png
│   └── 1116_Automated Workflow_Chart - Databricks.png
├── .github/workflows/                        # (Optional) CI pipelines
│   ├── 01_Install.yml
│   ├── 02_Format.yml
│   ├── 03_Lint.yml
│   └── 04_Test.yml
├── .devcontainer/                            # (Optional) VS Code dev container
├── .gitignore
├── Makefile                                  # Helper targets (install/format/lint/test)
├── requirements.txt
└── README.md
```

> **Note:** `.github/` and `.devcontainer/` are optional. Keep them if you want CI and containerized dev; they are not required to run the pipeline on Databricks.

## How to Run (Databricks Community Edition or Azure Databricks)
1. **Upload data**: In the UI, go to **Data → Add data → Upload** and upload `Sample - Superstore.csv`.
   - Path will be: `/FileStore/tables/superstore.csv` (or rename in the UI to match).
2. **Import code**:
   - Use **Repos → Add Repo** to connect this GitHub repo, **or**
   - **Workspace → Import** each file from `Codes/` as a notebook.
3. **Attach a cluster** and run in order:
   - `01_ETL_Operations.py` → creates `bronze.superstore`, `silver.superstore`
   - `02_Delta_Lake_For_Storage.py` → creates `silver.superstore_partitioned`
   - `03_Spark_SQL_for_DataTransformations.py` → creates `gold.sales_summary`
   - `04_Visualization_of_Transformed_Data.py` → saves charts to `/dbfs/tmp/`
4. **View charts** (inside a notebook):
   - `/files/tmp/total_sales_by_category.png`
   - `/files/tmp/total_profit_by_region.png`

## Example SQL (Gold)
```sql
SELECT Category, Region, total_sales, total_profit
FROM gold.sales_summary
ORDER BY total_sales DESC;
```

## Development Guide (Makefile + CI)
For local development, use the provided **Makefile** and **requirements.txt**.

### 1) Create a virtual environment (optional but recommended)
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 2) Install dependencies
```bash
make install
```

### 3) Format, lint, and test
```bash
make format
make lint
make test
```

The GitHub Actions workflows in `.github/workflows/` run similar steps automatically on every push/PR.

## Requirements (local dev/testing)
If you’re running tests/linters locally:
```bash
pip install -r requirements.txt
pytest
```

## Extensions (Future Work)
- Switch Bronze ingest to **Auto Loader** (streaming file ingestion).
- Add **DLT expectations** and **Unity Catalog** (governance).
- Orchestrate with **Jobs** / **ADF**; add **alerts**.
- Add **MLflow** forecasting on the Gold data.

---

**Author:** Wael Rahhal — Data Scientist / Data Engineering
