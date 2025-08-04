from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime, now
import logging

log = logging.getLogger(__name__)

@dag(
    dag_id="make_time_travel_table_clone",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["task"]
)
def time_travel_dag_func():
    @task(task_id="clone_backup_task")
    def make_clone():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            source_customer_table = "RAW_DB.PUBLIC.CUSTOMERS_CLEAN"
            source_sales_table = "RAW_DB.PUBLIC.SALES_CLEAN"

            timestamp_str = now().strftime('%Y%m%d_%H%M%S')
            clone_customer_table = f"RAW_DB.PUBLIC.CUSTOMERS_CLEAN_CLONE_{timestamp_str}"
            clone_sales_table = f"RAW_DB.PUBLIC.SALES_CLEAN_CLONE_{timestamp_str}"

            clone_customer_sql = f"""
            CREATE TABLE {clone_customer_table} CLONE {source_customer_table} AT (OFFSET => -3600)
            """
            clone_sales_sql = f"""
            CREATE TABLE {clone_sales_table} CLONE {source_sales_table} AT (OFFSET => -3600)
            """

            log.info(f"Executing SQL to clone customer table:\n{clone_customer_sql}")
            cursor.execute(clone_customer_sql)

            log.info(f"Executing SQL to clone sales table:\n{clone_sales_sql}")
            cursor.execute(clone_sales_sql)
        except Exception as e:
            log.error(f"Error of cloning tables: {e}", exc_info=True)
            raise
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    make_clone()

dag = time_travel_dag_func()
