
INSERT INTO RAW_DB.PUBLIC.sales_clean
SELECT DISTINCT
    sale_id,
    customer_id,
    product_id,
    store_id,
    sale_date,
    CASE WHEN amount < 0 THEN 0 ELSE amount END AS amount,
    discount_applied,
    region,
    data_source,
    last_updated
FROM RAW_DB.PUBLIC.raw_sales
WHERE sale_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND sale_date IS NOT NULL
  AND discount_applied > 0; --remove all strings where discount_applied 0 to better analytics 

  CREATE OR REPLACE TABLE RAW_DB.PUBLIC.customers_clean AS
SELECT DISTINCT
    customer_id,
    INITCAP(first_name) AS first_name,
    INITCAP(last_name) AS last_name,
    LOWER(COALESCE(email, 'no_email@unknown.com')) AS email, --replace all null at email column to make data cleaner
    registration_date,
    country, 
    last_updated
FROM RAW_DB.PUBLIC.raw_customers
WHERE customer_id IS NOT NULL
  AND first_name IS NOT NULL
  AND registration_date <= CURRENT_DATE;