CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DB.PUBLIC;

--stage 1
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

--stage 2
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

CREATE OR REPLACE TABLE RAW_DB.PUBLIC.customers_clean(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR,
    last_updated TIMESTAMP
)
DATA_RETENTION_TIME_IN_DAYS = 7;

CREATE OR REPLACE FILE FORMAT RAW_DB.PUBLIC.csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', '');

CREATE STAGE IF NOT EXISTS RAW_DB.PUBLIC.my_stage;