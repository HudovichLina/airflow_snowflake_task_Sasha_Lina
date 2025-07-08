CREATE WAREHOUSE ETL_WAREHOUSE
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

CREATE DATABASE RAW_DB;
CREATE SCHEMA RAW_DB.PUBLIC;

CREATE TABLE RAW_DB.PUBLIC.raw_sales (
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
);

CREATE TABLE RAW_DB.PUBLIC.raw_customers (
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    registration_date DATE,
    country VARCHAR,
    last_updated TIMESTAMP
);

CREATE FILE FORMAT RAW_DB.PUBLIC.csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', '');

CREATE STAGE RAW_DB.PUBLIC.my_stage;
