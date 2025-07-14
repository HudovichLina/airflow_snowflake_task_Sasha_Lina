from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
import logging
import os

log = logging.getLogger(__name__)

@dag(
    dag_id="snowflake_task_dag",
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["task"]
)
def snowflake_dag():

    @task(task_id="load_data_to_snowflake")
    def load_data_stage1():
        log.info("Starting data load into Snowflake...")

        hook = SnowflakeHook(snowflake_conn_id="Snowflake_task_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        base_path = os.path.dirname(__file__)
        file_path_sales = os.path.join(base_path, "..", "sample_data", "new_sales_data.csv")
        file_path_customers = os.path.join(base_path, "..", "sample_data", "new_customers_data.csv")

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            log.info("Uploading sales and customers CSV to @my_stage...")
            cursor.execute(f"PUT file://{file_path_sales} @my_stage AUTO_COMPRESS=TRUE")
            cursor.execute(f"PUT file://{file_path_customers} @my_stage AUTO_COMPRESS=TRUE")

            log.info("Loading into RAW_SALES and RAW_CUSTOMERS tables...")
            cursor.execute("""
                COPY INTO RAW_SALES
                FROM @my_stage/new_sales_data.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            """)
            cursor.execute("""
                COPY INTO RAW_CUSTOMERS
                FROM @my_stage/new_customers_data.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            """)

            conn.commit()
            log.info("Raw data successfully loaded.")

        except Exception as e:
            log.error(f"Error loading data: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()
        return "success"

    @task(task_id="cdc_merge_sales")
    def CDC_check():
        log.info("Starting CDC MERGE for SALES_CLEAN...")

        hook = SnowflakeHook(snowflake_conn_id="Snowflake_task_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            merge_query = """
                MERGE INTO SALES_CLEAN target
                USING RAW_SALES source
                ON target.sale_id = source.sale_id
                WHEN MATCHED AND source.last_updated > target.last_updated THEN
                    UPDATE SET 
                        amount = source.amount,
                        discount_applied = source.discount_applied,
                        last_updated = source.last_updated
                WHEN NOT MATCHED THEN
                    INSERT (
                        sale_id, customer_id, product_id, store_id, sale_date,
                        amount, discount_applied, region, data_source, last_updated
                    )
                    VALUES (
                        source.sale_id, source.customer_id, source.product_id, source.store_id, source.sale_date,
                        source.amount, source.discount_applied, source.region, source.data_source, source.last_updated
                    )
            """
            cursor.execute(merge_query)
            conn.commit()
            log.info("CDC MERGE completed successfully.")

        except Exception as e:
            conn.rollback()
            log.error(f"Error during CDC MERGE: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()

    @task(task_id="transform_and_load_clean")
    def clean_data_stage2():
        log.info("Starting transformation and cleaning...")

        hook = SnowflakeHook(snowflake_conn_id="Snowflake_task_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            # Insert into SALES_CLEAN with rules
            sql_insert_sales = """
                INSERT INTO SALES_CLEAN (
                    sale_id, customer_id, product_id, store_id, sale_date,
                    amount, discount_applied, region, data_source, last_updated
                )
                SELECT 
                    sale_id,
                    customer_id,
                    product_id,
                    store_id,
                    sale_date,
                    CASE WHEN amount < 0 THEN 0 ELSE amount END,
                    discount_applied,
                    region,
                    data_source,
                    last_updated
                FROM RAW_SALES
                WHERE sale_id IS NOT NULL
                  AND customer_id IS NOT NULL 
                  AND sale_date IS NOT NULL
                  AND discount_applied > 0
            """
            cursor.execute(sql_insert_sales)

            # Insert into CUSTOMERS_CLEAN
            sql_insert_customers = """
                INSERT INTO CUSTOMERS_CLEAN (
                    customer_id, first_name, last_name, registration_date, country, last_updated
                )
                SELECT
                    customer_id,
                    INITCAP(first_name),
                    INITCAP(last_name),
                    registration_date,
                    country,
                    last_updated
                FROM RAW_CUSTOMERS
            """
            cursor.execute(sql_insert_customers)

            # Logging row counts
            cursor.execute("SELECT COUNT(*) FROM SALES_CLEAN")
            sales_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM CUSTOMERS_CLEAN")
            customers_count = cursor.fetchone()[0]

            conn.commit()
            log.info(f"Transformed rows — SALES: {sales_count}, CUSTOMERS: {customers_count}")
            return f"Clean Sales rows: {sales_count}, Clean Customers rows: {customers_count}"

        except Exception as e:
            conn.rollback()
            log.error(f"Error during transformation: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()

    
    @task(task_id = "create_datamart")
    def create_datamart_stage3():
        log.info("Starting creation of sales_customers_mart...")
        hook = SnowflakeHook(snowflake_conn_id= "Snowflake_task_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")
            sql_datamart = """CREATE OR REPLACE TABLE sales_customers_mart AS
                              SELECT 
                                  c.customer_id,
                                  c.first_name,
                                  c.last_name,
                                  s.product_id,
                                  c.country,
                                  s.region,
                                  s.discount_applied
                              FROM customers_clean c
                              JOIN sales_clean s ON c.customer_id = s.customer_id
                              ORDER BY s.discount_applied DESC"""
            
            cursor.execute(sql_datamart)

            cursor.execute("SELECT COUNT(*) FROM sales_customers_mart")
            count = cursor.fetchone()[0]
            conn.commit()
    
            log.info(f"sales_customers_mart created with {count} rows.")

        except Exception as e:
            conn.rollback()
            log.error(f"Error during creating datamart: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()
        
    @task(task_id = "secure_view")
    def create_secure_view():
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_task_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")
            secure_view = """CREATE OR REPLACE SECURE VIEW secure_sales_mart AS
                             SELECT * FROM sales_customers_mart
                             WHERE (CURRENT_ROLE() IN ('USA_ANALYST') AND country = 'USA')
                                OR (CURRENT_ROLE() IN ('ASIA_ANALYST') AND region = 'Asia');""" 
            
            cursor.execute(secure_view)
            log.info("Secure view created successfully.")
        
        except Exception as e:
            conn.rollback()
            log.error(f"Error creating secure view: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()    

    


    load_data_stage1() >> CDC_check() >> clean_data_stage2() >> create_datamart_stage3() >> create_secure_view()

dag = snowflake_dag()
