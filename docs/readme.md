# ❄️ Snowflake Data Pipeline — Airflow DAGs

## 📌 Overview

This repository includes **three Airflow DAGs** for orchestrating a Snowflake-based data pipeline. It automates infrastructure setup (DDL), CSV data ingestion, cleaning and transformation, change data capture (CDC), datamart creation, and secure access through Row-Level Security and views. Additionally, it enables table cloning using Snowflake’s **Time Travel** feature.

---

## ✅ DAG 1: `snowflake_ddl_dag` — Infrastructure Initialization

This DAG runs a DDL script (`DDL_queries.sql`) to prepare the Snowflake environment.

### It creates:
- **Warehouse**: `ETL_WAREHOUSE`
- **Database / Schema**: `RAW_DB.PUBLIC`
- **File Format**: `CSV_FORMAT`
- **Stage**: `my_stage`
- **Row-Level Security policy**: `region_filter_policy`
- **Secure View**: `secure_sales_mart`

### Tables:

#### Raw Layer:
- `raw_sales`
- `raw_customers`

#### Clean Layer:
- `sales_clean`
- `customers_clean`

#### Datamart:
- `sales_customers_mart`

#### Metadata:
- `file_load_log` — tracks loaded files
- `role_region_map` — defines access control per region/role

> ⏳ All tables have `DATA_RETENTION_TIME_IN_DAYS = 7` to enable **Time Travel** and recovery.

---

## 🔁 DAG 2: `snowflake_task_dag` — Main ETL Pipeline

This DAG handles full ETL processing: file loading, cleaning, change detection, transformation, and datamart updates.

### Tasks:

#### 1. `load_data_stage1`
- Uploads local `.csv` files to Snowflake stage (`@my_stage`)
- Skips files that already exist in the stage or have already been logged
- Uses `COPY INTO` to load into `RAW_SALES` and `RAW_CUSTOMERS`
- Logs loaded files to `FILE_LOAD_LOG`

#### 2. `clean_data_stage2`
- Cleans and transforms raw data:
  - Drops records with nulls in key fields
  - Replaces negative amounts with `0`
  - Formats names with `INITCAP`
  - Fills null emails with placeholder
- Inserts results into:
  - `CLEANED_SALES`
  - `CLEANED_CUSTOMERS`

#### 3. `cdc_merge_sales_customers`
- Performs **MERGE** operations to:
  - Update existing records if `last_updated` is newer
  - Insert new records
- Targets:
  - `SALES_CLEAN`
  - `CUSTOMERS_CLEAN`

#### 4. `merge_datamart_stage3`
- Joins `CLEANED_SALES` and `CLEANED_CUSTOMERS` into `sales_customers_mart`
- Uses MERGE to insert or update rows
- Logs row-level merge statistics and total count

#### P.S. To check that tables are updated correctly when loading new data, move the two new data files to another directory
---

## 🔐 Access Control & Security

The pipeline implements Row-Level Security using Snowflake policies:

| Role           | Region Access     |
|----------------|-------------------|
| `ACCOUNTADMIN` | All regions       |
| `EUROPE`       | Europe only       |
| `ASIA`         | Asia only         |
| `NORTH AMERICA`| North America only|

### Mechanism:
- Table: `role_region_map` — maps roles to regions
- Policy: `region_filter_policy` — applies row-level filtering
- View: `secure_sales_mart` — secure access to filtered datamart

---

## 🕒 DAG 3: `make_time_travel_table_clone` — Table Cloning with Time Travel

This DAG creates **snapshot clones** of `CUSTOMERS_CLEAN` and `SALES_CLEAN` tables using Snowflake Time Travel.

### How it works:
- Generates timestamped clone table names


### Project structure:
├── docs/

│   ├── readme.md


├── dags/

│   ├── snowflake_ddl_dag.py

│   ├── snowflake_task_dag.py

│   └── make_time_travel_table_clone.py


├── sql/

│   └── stage_1/
│       └── DDL_queries.sql


├── sample_data/

│   └── raw_data/
│       └── *.csv
