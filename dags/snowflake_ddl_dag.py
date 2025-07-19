from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
from datetime import timedelta
import logging
import os

# Logger
log = logging.getLogger(__name__)

# Paths
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SQL_TEMPLATES_DIR = os.path.join(os.path.dirname(DAG_FOLDER), 'sql')
DDL_SQL_PATH = os.path.join(SQL_TEMPLATES_DIR, 'stage_1', 'DDL_queries.sql')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='snowflake_ddl_dag',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 7, 7),
    catchup=False,
    tags=['snowflake', 'ddl'],
)
def snowflake_ddl_dag():

    @task(task_id='run_ddl_sql')
    def execute_ddl_sql():
        log.info("Starting DDL execution from file: %s", DDL_SQL_PATH)

        if not os.path.exists(DDL_SQL_PATH):
            log.error("SQL file not found: %s", DDL_SQL_PATH)
            raise FileNotFoundError(f"SQL file not found: {DDL_SQL_PATH}")

        with open(DDL_SQL_PATH, 'r') as file:
            sql_script = file.read()
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            for i, statement in enumerate(statements, 1):
                log.info("Executing SQL statement %d of %d", i, len(statements))
                cursor.execute(statement)
            conn.commit()
            log.info("All DDL statements executed successfully.")
        except Exception as e:
            log.error("Error during SQL execution: %s", str(e), exc_info=True)
            raise
        finally:
            cursor.close()
            conn.close()
            log.info("Snowflake connection closed.")

    execute_ddl_sql()

dag = snowflake_ddl_dag()
