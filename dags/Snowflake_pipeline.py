from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
from dags.utils.config import SNOWFLAKE_CONNECTION as SN
from dags.utils.snowflake_utils import render_sql_template, run_sql_file
import logging
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))       

log = logging.getLogger(__name__)

@dag(
    dag_id="snowflake_task_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["task"]
)
def snowflake_dag():


    @task()
    def upload_and_load_raw_data(file_type: str):

        '''Uploads CSV files to Snowflake stage and loads them into RAW tables.
        Also logs successfully loaded files.'''

        log.info("Starting data upload to Snowflake...")

        hook = SnowflakeHook(snowflake_conn_id=SN)
        directory = os.path.join(os.path.dirname(DAG_DIR), "sample_data")

        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        if not csv_files:
            log.warning("No CSV files found.")
            return "no_csv_files"

        for file_name in csv_files:
            file_path = os.path.join(directory, file_name)
            log.info(f"Uploading {file_name} to stage...")

            put_query = f"PUT file://{file_path} @my_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            hook.run(put_query)

        # Get latest files in stage
        stage_files = hook.run("LIST @my_stage", return_last=True) or []
        stage_file_names = [row[0].split('/')[-1] for row in stage_files]

        for file_name in stage_file_names:
            log.info(f"Loading staged file {file_name}...")

            if "sales" in file_name.lower():
                query = render_sql_template(DAG_DIR,"copy_into_raw_sales.sql", file_name=file_name)
            elif "customers" in file_name.lower():
                query = render_sql_template(DAG_DIR, "copy_into_raw_customers.sql", file_name=file_name)
            else:
                log.warning(f"Unrecognized file: {file_name}, skipping.")
                continue

            hook.run(query)

            insert_log_query = render_sql_template(DAG_DIR, "insert_file_log.sql", file_name=file_name)
            hook.run(insert_log_query)

            log.info(f"{file_name} loaded and logged.")
        
        return "success"

    @task()
    def transform_raw_to_cleaned() -> None:
        '''Cleans and merges raw sales and customer data into cleaned Snowflake tables.'''

        log.info("Cleaning data and merging raw -> clean tables...")
        run_sql_file(DAG_DIR, SN, "merge_sales_clean.sql")
        run_sql_file(DAG_DIR, SN, "merge_customers_clean.sql")
           
    log.info("Unified clean + CDC MERGE completed.")


    @task()
    def merge_clean_to_datamart():
        '''Merges cleaned data into the final datamart table and logs merge result.'''

        log.info("Starting datamart merge...")
        hook = SnowflakeHook(snowflake_conn_id=SN)

        run_sql_file(DAG_DIR, SN, "stage_3/merge_sales_customers_mart.sql")
        
        # Capture MERGE result metrics
        merge_result = hook.get_first("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        inserted = merge_result[0]
        updated = merge_result[1]
        total_count = hook.get_first("SELECT COUNT(*) FROM sales_customers_mart")
        log.info(f"MERGE done: inserted={inserted}, updated={updated}, total rows={total_count}")


    [upload_and_load_raw_data("sales"), upload_and_load_raw_data("customers")] >> transform_raw_to_cleaned() >> merge_clean_to_datamart()

dag = snowflake_dag()
