-- Create datamart as table
-- query should contain discount customers from USA with sales in Asia region  
CREATE OR REPLACE TABLE RAW_DB.PUBLIC.sales_customers_mart AS
SELECT c.first_name,
       c.last_name,
       s.product_id,
       c.country,
       s.region,
       s.discount_applied
FROM RAW_DB.PUBLIC.customers_clean c
     INNER JOIN RAW_DB.PUBLIC.sales_clean s ON c.customer_id = s.customer_id
WHERE c.country = 'USA' AND s.region = 'Asia'
ORDER BY s.discount_applied DESC