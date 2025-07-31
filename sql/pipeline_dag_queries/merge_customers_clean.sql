MERGE INTO RAW_DB.PUBLIC.CUSTOMERS_CLEAN target 
USING (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        registration_date,
        country,
        last_updated
    FROM (
        SELECT
            customer_id,
            first_name,
            last_name,
            email,
            registration_date,
            country,
            last_updated,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
        FROM RAW_DB.PUBLIC.RAW_CUSTOMERS
        WHERE 
            customer_id IS NOT NULL
            AND first_name IS NOT NULL
            AND last_name IS NOT NULL
            AND email IS NOT NULL
            AND registration_date IS NOT NULL
    ) ranked
    WHERE rn = 1
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