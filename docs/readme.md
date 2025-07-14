# Snowflake DWH – Stage 1

## What’s Done:

- Created a database and a schema in Snowflake
- Created tables for storing **raw** data
- Configured an internal stage
- Uploaded CSV files to the internal stage
- Used `COPY INTO` to load data from the stage into raw tables

## Stage 1 Structure:

- Table `raw_sales`
- Table `raw_customers`
...

## Stage 2:
- Created tables for storing **clean** data
- The **raw** data has been cleared and inserted into tables
### Stage 2 structure:
- Table `customer_clean`
- Table `sales_clean`

## Stage 3
- Created datamart for analysis special amount of customers and discounts
### Stage 3 structure:
- Table `sales_customers_mart`

