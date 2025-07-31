from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.config import SNOWFLAKE_CONNECTION as SN
from utils.snowflake_utils import run_sql_file
import logging
import os

# Logger
log = logging.getLogger(__name__)

# Paths
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DDL_SQL_FILE = os.path.join(os.path.dirname(DAG_DIR), 'sql', 'pipeline_dag_queries', 'DDL_queries.sql')

hook = SnowflakeHook(snowflake_conn_id=SN)

@dag(
    dag_id='snowflake_ddl_dag',
    schedule=None,
    start_date=datetime(2025, 7, 7),
    catchup=False,
    tags=['snowflake', 'ddl'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def snowflake_ddl_dag():

    @task(task_id='run_ddl_sql')
    def execute_ddl_sql():
        """Executes DDL queries from a SQL file in Snowflake using run_sql_file."""
        log.info("Starting DDL execution from file: %s", DDL_SQL_FILE)

        if not os.path.exists(DDL_SQL_FILE):
            log.error("SQL file not found: %s", DDL_SQL_FILE)
            raise FileNotFoundError(f"SQL file not found: {DDL_SQL_FILE}")

        run_sql_file(DAG_DIR, hook, "DDL_queries.sql")
        log.info("All DDL statements executed successfully.")

    execute_ddl_sql()

dag = snowflake_ddl_dag()
