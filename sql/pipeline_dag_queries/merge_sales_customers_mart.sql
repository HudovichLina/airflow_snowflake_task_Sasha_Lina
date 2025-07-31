MERGE INTO RAW_DB.PUBLIC.sales_customers_mart tgt
USING (
    SELECT 
        s.sale_id,
        c.customer_id,
        c.first_name,
        c.last_name,
        s.product_id,
        c.country,
        s.region,
        s.discount_applied
    FROM RAW_DB.PUBLIC.customers_clean c
    JOIN RAW_DB.PUBLIC.sales_clean s ON c.customer_id = s.customer_id
) src
ON tgt.sale_id = src.sale_id
WHEN MATCHED THEN UPDATE SET
    customer_id = src.customer_id,
    first_name = src.first_name,
    last_name = src.last_name,
    product_id = src.product_id,
    country = src.country,
    region = src.region,
    discount_applied = src.discount_applied
WHEN NOT MATCHED THEN INSERT (
    sale_id, customer_id, first_name, last_name, product_id, country, region, discount_applied
) VALUES (
    src.sale_id, src.customer_id, src.first_name, src.last_name, src.product_id,
    src.country, src.region, src.discount_applied
);