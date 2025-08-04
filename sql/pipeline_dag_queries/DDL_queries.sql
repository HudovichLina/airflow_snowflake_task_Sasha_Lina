-- Create a warehouse for ETL operations
CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

-- Create database and schema for raw and transformed data
CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DB.PUBLIC;

-- ====================
-- Stage 1: Raw Tables
-- ====================

-- Raw sales data table
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.raw_sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    store_id INT,
    sale_date DATE,
    amount DECIMAL(10,2),
    discount_applied DECIMAL(5,2),
    region VARCHAR,
    data_source VARCHAR,
    last_updated TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Raw customer data table
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.raw_customers (
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    registration_date DATE,
    country VARCHAR,
    last_updated TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ============================
-- Stage 2: Cleaned Data Tables
-- ============================

-- Cleaned and validated sales data
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.sales_clean (
    sale_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    sale_date DATE NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    discount_applied DECIMAL(5,2) DEFAULT 0,
    region VARCHAR,
    data_source VARCHAR,
    last_updated TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Cleaned and standardized customer data
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.customers_clean (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR,
    last_updated TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ========================
-- Stage Configuration
-- ========================

-- CSV file format definition for staging files
CREATE OR REPLACE FILE FORMAT RAW_DB.PUBLIC.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', '');

-- Logging table to track processed files
CREATE TABLE IF NOT EXISTS RAW_DB.PUBLIC.FILE_LOAD_LOG (
    file_name STRING PRIMARY KEY,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ====================
-- Stage 3: Data Mart
-- ====================

-- Data mart combining customer and sales data
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.sales_customers_mart (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    product_id INT,
    country VARCHAR,
    region VARCHAR,
    discount_applied DECIMAL(5, 2)
)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Create Snowflake stage for file loading
CREATE STAGE IF NOT EXISTS RAW_DB.PUBLIC.my_stage;

-- =============================
-- Row-Level Security (RLS)
-- =============================

-- Create mapping table between roles and allowed regions
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.role_region_map (
    role_name STRING,
    allowed_region STRING
);

-- Insert allowed role-region pairs
INSERT INTO RAW_DB.PUBLIC.role_region_map VALUES
('ASIA', 'Asia'),
('NORTH AMERICA', 'North America'),
('EUROPE', 'Europe');

-- Define Row Access Policy using the mapping table
CREATE OR REPLACE ROW ACCESS POLICY RAW_DB.PUBLIC.region_filter_policy
AS (region STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN' OR
    EXISTS (
        SELECT 1
        FROM RAW_DB.PUBLIC.role_region_map
        WHERE role_name = CURRENT_ROLE() AND allowed_region = region
    );

ALTER TABLE RAW_DB.PUBLIC.sales_customers_mart
ADD ROW ACCESS POLICY RAW_DB.PUBLIC.region_filter_policy ON (region);

-- ====================
-- Secure Data Access
-- ====================

-- Create a secure view for protected access
CREATE OR REPLACE SECURE VIEW RAW_DB.PUBLIC.secure_sales_mart AS
SELECT * FROM RAW_DB.PUBLIC.sales_customers_mart;
