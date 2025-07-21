LIST @RAW_DB.PUBLIC.my_stage;

-- Check number of rows in raw tables
SELECT COUNT(*) AS row_count FROM RAW_DB.PUBLIC.raw_sales;
SELECT COUNT(*) AS row_count FROM RAW_DB.PUBLIC.raw_customers;

-- View sample data from raw tables ordered by latest updates
SELECT * FROM RAW_DB.PUBLIC.raw_sales ORDER BY last_updated DESC LIMIT 10;
SELECT * FROM RAW_DB.PUBLIC.raw_customers ORDER BY last_updated DESC LIMIT 10;

-- Check number of rows after cleaning
SELECT COUNT(*) AS cleaned_sales FROM RAW_DB.PUBLIC.CLEANED_SALES;
SELECT COUNT(*) AS cleaned_customers FROM RAW_DB.PUBLIC.CLEANED_CUSTOMERS;

-- View sample data after transformation
SELECT * FROM RAW_DB.PUBLIC.CLEANED_SALES LIMIT 10;
SELECT * FROM RAW_DB.PUBLIC.CLEANED_CUSTOMERS LIMIT 10;

-- Check final row count in target tables
SELECT COUNT(*) AS final_sales FROM RAW_DB.PUBLIC.sales_clean;
SELECT COUNT(*) AS final_customers FROM RAW_DB.PUBLIC.customers_clean;

-- View latest updates in cleaned data
SELECT * FROM RAW_DB.PUBLIC.sales_clean ORDER BY last_updated DESC LIMIT 10;
SELECT * FROM RAW_DB.PUBLIC.customers_clean ORDER BY last_updated DESC LIMIT 10;

-- Row count in the datamart
SELECT COUNT(*) FROM RAW_DB.PUBLIC.sales_customers_mart;

-- Sample data: top 10 by discount
SELECT * FROM RAW_DB.PUBLIC.sales_customers_mart ORDER BY discount_applied DESC LIMIT 10;

-- List of all previously loaded files
SELECT * FROM RAW_DB.PUBLIC.FILE_LOAD_LOG ORDER BY load_time DESC;

-- Query history
SELECT *
FROM TABLE(snowflake.information_schema.query_history())
ORDER BY start_time DESC
LIMIT 10;

-- View secure datamart
SELECT * FROM RAW_DB.PUBLIC.secure_sales_mart;

-- Show all row access policies
SHOW ROW ACCESS POLICIES;

-- Create and use a role
CREATE ROLE Asia;
GRANT ROLE Asia TO USER ACCOUNTADMIN;
USE ROLE Asia;

-- Check distinct regions in secure data
SELECT DISTINCT region FROM RAW_DB.PUBLIC.secure_sales_mart;

-- Restore table to state from 1 hour ago
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.sales_clean_restored AS
SELECT * FROM RAW_DB.PUBLIC.sales_clean AT (OFFSET => -3600);

-- View version history of table over last 24 hours
SELECT * FROM RAW_DB.PUBLIC.sales_clean 
VERSIONS BETWEEN (TIMESTAMP => CURRENT_TIMESTAMP() - INTERVAL '1 DAY') AND CURRENT_TIMESTAMP();
