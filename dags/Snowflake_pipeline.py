from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
from utils.config import SNOWFLAKE_CONNECTION as SN
from utils.snowflake_utils import run_sql_file
import logging
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))       
log = logging.getLogger(__name__)

hook = SnowflakeHook(snowflake_conn_id=SN)

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

        directory = os.path.join(os.path.dirname(DAG_DIR), "sample_data", "raw_data")

        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        if not csv_files:
            log.warning("No CSV files found.")
            return "no_csv_files"

        for file_name in csv_files:
            file_path = os.path.join(directory, file_name)
            log.info(f"Uploading {file_name} to stage...")

            put_query = f"PUT file://{file_path} @RAW_DB.PUBLIC.my_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            hook.run(put_query)

        # Get latest files in stage
        stage_files = hook.get_records("LIST @RAW_DB.PUBLIC.my_stage")
        log.info(f"Stage files raw result: {stage_files}")
        stage_file_names = [row[0].split('/')[-1] for row in stage_files]
        log.info(f"Stage file names: {stage_file_names}")

        for file_name in stage_file_names:
            log.info(f"Loading staged file {file_name}...")

            if "sales" in file_name.lower():
                run_sql_file(DAG_DIR, hook, "copy_into_raw_sales.sql", file_name=file_name)
                log.info(f"Starting to load {file_name} into sales table")
            elif "customers" in file_name.lower():
                log.info(f"Starting to load {file_name} into customers table")
                run_sql_file(DAG_DIR, hook, "copy_into_raw_customers.sql", file_name=file_name)
            else:
                log.warning(f"Unrecognized file: {file_name}, skipping.")
                continue

            run_sql_file(DAG_DIR, hook, "insert_file_log.sql", file_name=file_name)
            log.info(f"{file_name} loaded and logged.")
        
        return "success"

    @task()
    def transform_raw_to_cleaned() -> None:
        '''Cleans and merges raw sales and customer data into cleaned Snowflake tables.'''

        log.info("Cleaning data and merging raw -> clean tables...")
        run_sql_file(DAG_DIR, hook, "merge_sales_clean.sql")
        run_sql_file(DAG_DIR, hook, "merge_customers_clean.sql")
           
        log.info("Unified clean + CDC MERGE completed.")

    @task()
    def merge_clean_to_datamart():
        '''Merges cleaned data into the final datamart table and logs merge result.'''

        log.info("Starting datamart merge...")

        run_sql_file(DAG_DIR, hook, "merge_sales_customers_mart.sql")

        total_count = hook.get_first("SELECT COUNT(*) FROM RAW_DB.PUBLIC.sales_customers_mart")[0]
        log.info(f"MERGE done: total rows={total_count}")
        return {"total_count": total_count}

    [upload_and_load_raw_data("sales"), upload_and_load_raw_data("customers")] >> transform_raw_to_cleaned() >> merge_clean_to_datamart()

dag = snowflake_dag()