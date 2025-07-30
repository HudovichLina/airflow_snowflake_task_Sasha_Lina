MERGE INTO CUSTOMERS_CLEAN target
USING(
    SELECT 
        customer_id,
        INITCAP(first_name) AS first_name,
        INITCAP(last_name) AS last_name,
        COALESCE(email, 'unknown@example.com') AS email,
        registration_date,
        country,
        last_updated
    FROM RAW_CUSTOMERS
    WHERE 
        customer_id IS NOT NULL
        AND registration_date IS NOT NULL
) source 
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.last_updated > target.last_updated THEN
    UPDATE SET 
        first_name = source.first_name,
        last_name = source.last_name,
        email = source.email,
        registration_date = source.registration_date,
        country = source.country,
        last_updated = source.last_updated  
WHEN NOT MATCHED THEN 
    INSERT (
        customer_id, first_name, last_name, email, registration_date, country, last_updated
    )
    VALUES (
        source.customer_id, source.first_name, source.last_name, source.email,
        source.registration_date, source.country, source.last_updated
    );
