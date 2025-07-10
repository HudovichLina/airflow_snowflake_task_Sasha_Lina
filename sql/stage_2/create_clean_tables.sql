--stage 2
--Set INT PRIMARY KEY AND NOT NULL as table basic constraints
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
);

CREATE OR REPLACE TABLE RAW_DB.PUBLIC.customers_clean(
    customers_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR,
    last_updated TIMESTAMP


)

