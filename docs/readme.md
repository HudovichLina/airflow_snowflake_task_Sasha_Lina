# ❄️ Snowflake Data Pipeline — Airflow DAGs

## 📌 Overview

This repository includes two Airflow DAGs that orchestrate the setup and operation of a Snowflake-based data warehouse (DWH). The pipeline includes automated DDL execution, raw data ingestion, CDC (Change Data Capture), data transformation, data mart creation, and access control via secure views.

---

## ✅ DAG 1: `snowflake_ddl_dag` — DDL Initialization

This DAG initializes all foundational components of the Snowflake DWH by executing a predefined DDL script.

### It creates:
- **Virtual Warehouse** (`ETL_WAREHOUSE`)
- **Database** (`RAW_DB`)
- **Schema** (`PUBLIC`)
- **File Format** (`CSV_FORMAT`)
- **Internal Stage** (`my_stage`)

### Tables created:

#### Raw Layer:
- `RAW_DB.PUBLIC.raw_sales`
- `RAW_DB.PUBLIC.raw_customers`

#### Clean Layer:
- `RAW_DB.PUBLIC.sales_clean`
- `RAW_DB.PUBLIC.customers_clean`

> ℹ️ All tables are configured with `DATA_RETENTION_TIME_IN_DAYS = 7` for time travel and recovery.

---

## 🔄 DAG 2: `snowflake_task_dag` — Main ETL Pipeline

This DAG handles the entire ETL process: data loading, change detection, transformation, data mart creation, and secure access via views.

### Tasks Breakdown:

#### 1. `load_data_stage1`

- Uploads CSV files (`new_sales_data.csv`, `new_customers_data.csv`) to the internal stage
- Uses `COPY INTO` to load data into raw tables

#### 2. `cdc_merge_sales`

- Executes a `MERGE` operation from `raw_sales` into `sales_clean`
- Updates existing records and inserts new ones based on `last_updated`

#### 3. `clean_data_stage2`

- Applies transformation and validation rules
- Inserts cleaned records into `sales_clean` and `customers_clean`
- Handles:
  - Filtering invalid data (e.g., null `sale_id`)
  - Normalizing negative amounts
  - Formatting names using `INITCAP`

#### 4. `create_datamart_stage3` 

- Creates a data mart table `sales_customers_mart` by joining cleaned sales and customer data
- Orders data by `discount_applied` for downstream analysis

#### 5. `create_secure_view`
- Creates a secure view `secure_sales_mart` with row-level security
- Grants conditional access based on the current role:

  - `USA_ANALYST` → Sees only U.S. data
  - `ASIA_ANALYST` → Sees only Asian region data

## Time travel DAG

**time_travel_dag_func** - make ```CLONE``` from snowflake time travel sytem with an offset of 1 hour (**the solution is simple and open for improvements**)
