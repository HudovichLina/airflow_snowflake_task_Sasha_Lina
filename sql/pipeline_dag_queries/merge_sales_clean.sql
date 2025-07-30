MERGE INTO SALES_CLEAN target 
USING (
    SELECT
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
    FROM RAW_SALES
    WHERE 
        sale_id IS NOT NULL
        AND customer_id IS NOT NULL 
        AND sale_date IS NOT NULL
        AND discount_applied > 0
) source
ON target.sale_id = source.sale_id
WHEN MATCHED AND source.last_updated > target.last_updated THEN
    UPDATE SET 
        customer_id = source.customer_id,
        product_id = source.product_id,
        store_id = source.store_id,
        sale_date = source.sale_date,
        amount = source.amount,
        discount_applied = source.discount_applied,
        region = source.region,
        data_source = source.data_source,
        last_updated = source.last_updated  
WHEN NOT MATCHED THEN
    INSERT (
        sale_id, customer_id, product_id, store_id, sale_date,
        amount, discount_applied, region, data_source, last_updated 
    )
    VALUES (
        source.sale_id, source.customer_id, source.product_id, source.store_id, source.sale_date,
        source.amount, source.discount_applied, source.region, source.data_source, source.last_updated
    );
