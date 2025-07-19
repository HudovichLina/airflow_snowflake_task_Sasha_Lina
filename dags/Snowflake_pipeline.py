from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
import logging
import os

log = logging.getLogger(__name__)

@dag(
    dag_id="snowflake_task_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["task"]
)
def snowflake_dag():

    @task(task_id="load_data_to_snowflake")
    def load_data_stage1():
        log.info("Starting data upload to Snowflake...")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()
        directory = "/opt/airflow/sample_data/raw_data"

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            cursor.execute("LIST @my_stage")
            stage_files = [row[0].split('/')[-1] for row in cursor.fetchall()]

            cursor.execute("SELECT file_name FROM FILE_LOAD_LOG")
            loaded_files = [row[0] for row in cursor.fetchall()]

            csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
            if not csv_files:
                log.warning("No CSV files found in the local directory.")
                return "no_csv_files"

            for file_name in csv_files:
                staged_file_name = file_name + '.gz'
                if staged_file_name in stage_files:
                    log.info(f"{staged_file_name} already exists in stage, skipping.")
                    continue

                file_path = os.path.join(directory, file_name)
                log.info(f"Uploading {file_name} to stage...")
                cursor.execute(f"PUT file://{file_path} @my_stage AUTO_COMPRESS=TRUE")

            # Refresh stage file list
            cursor.execute("LIST @my_stage")
            updated_stage_files = [row[0].split('/')[-1] for row in cursor.fetchall()]
            new_files_to_load = [f for f in updated_stage_files if f not in loaded_files]

            if not new_files_to_load:
                log.info("No new files found for loading.")
            else:
                for file_name in new_files_to_load:
                    log.info(f"Loading file {file_name} into appropriate RAW table...")

                    if "sales" in file_name.lower():
                        copy_sql = f"""
                            COPY INTO RAW_SALES
                            FROM @my_stage/{file_name}
                            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
                        """
                    elif "customers" in file_name.lower():
                        copy_sql = f"""
                            COPY INTO RAW_CUSTOMERS
                            FROM @my_stage/{file_name}
                            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
                        """
                    else:
                        log.warning(f"Unknown file pattern for {file_name}, skipping.")
                        continue

                    cursor.execute(copy_sql)
                    conn.commit()

                    # Log successfully loaded file
                    insert_sql = f"INSERT INTO FILE_LOAD_LOG(file_name) VALUES ('{file_name}.gz')"
                    cursor.execute(insert_sql)
                    conn.commit()
                    log.info(f"{file_name} loaded and logged successfully.")

        except Exception as e:
            log.error(f"Error during file loading: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()

    @task(task_id="clean_data_stage2")
    def clean_data_stage2():
        log.info("Starting data cleaning and transformation...")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            # Cleaned sales table
            cursor.execute("""
                CREATE OR REPLACE TABLE CLEANED_SALES AS
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
            """)

            # Cleaned customers table
            cursor.execute("""
                CREATE OR REPLACE TABLE CLEANED_CUSTOMERS AS
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
            """)

            # Log row counts
            cursor.execute("SELECT COUNT(*) FROM CLEANED_SALES")
            sales_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM CLEANED_CUSTOMERS")
            customers_count = cursor.fetchone()[0]

            conn.commit()
            log.info(f"CLEANED_SALES: {sales_count} rows, CLEANED_CUSTOMERS: {customers_count} rows.")
            return f"Cleaned sales: {sales_count}, cleaned customers: {customers_count}"

        except Exception as e:
            conn.rollback()
            log.error(f"Error during cleaning stage: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()

    @task(task_id="cdc_merge_sales_customers")
    def CDC_check():
        log.info("Starting CDC MERGE for cleaned data...")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            # Merge sales data
            cursor.execute("""
                MERGE INTO SALES_CLEAN target
                USING CLEANED_SALES source
                ON target.sale_id = source.sale_id
                WHEN MATCHED AND source.last_updated > target.last_updated THEN
                    UPDATE SET 
                        amount = source.amount,
                        discount_applied = source.discount_applied,
                        last_updated = source.last_updated,
                        customer_id = source.customer_id,
                        product_id = source.product_id,
                        store_id = source.store_id,
                        sale_date = source.sale_date,
                        region = source.region,
                        data_source = source.data_source
                WHEN NOT MATCHED THEN
                    INSERT (
                        sale_id, customer_id, product_id, store_id, sale_date,
                        amount, discount_applied, region, data_source, last_updated
                    )
                    VALUES (
                        source.sale_id, source.customer_id, source.product_id, source.store_id, source.sale_date,
                        source.amount, source.discount_applied, source.region, source.data_source, source.last_updated
                    )
            """)

            # Merge customers data
            cursor.execute("""
                MERGE INTO CUSTOMERS_CLEAN target
                USING CLEANED_CUSTOMERS source
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
                    )
            """)

            conn.commit()
            log.info("CDC MERGE completed successfully.")

        except Exception as e:
            conn.rollback()
            log.error(f"CDC MERGE failed: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()

    @task(task_id="merge_datamart")
    def merge_datamart_stage3():
        log.info("Starting datamart merge...")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE RAW_DB")
            cursor.execute("USE SCHEMA PUBLIC")

            merge_sql = """
                MERGE INTO sales_customers_mart tgt
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
                    FROM customers_clean c
                    JOIN sales_clean s ON c.customer_id = s.customer_id
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
                )
            """

            cursor.execute(merge_sql)

            # Capture MERGE result metrics
            cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            merge_result = cursor.fetchone()
            inserted = merge_result[0]
            updated = merge_result[1]

            cursor.execute("SELECT COUNT(*) FROM sales_customers_mart")
            total_count = cursor.fetchone()[0]
            conn.commit()

            log.info(f"MERGE done: inserted={inserted}, updated={updated}, total rows={total_count}")

        except Exception as e:
            conn.rollback()
            log.error(f"Error during datamart merge: {e}", exc_info=True)
            raise

        finally:
            cursor.close()
            conn.close()


    load_data_stage1() >> clean_data_stage2() >> CDC_check() >> merge_datamart_stage3()

dag = snowflake_dag()
