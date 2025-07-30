import os
from jinja2 import Template
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

log = logging.getLogger(__name__)

def render_sql_template(dag_dir: str, filename: str, **kwargs) -> str:
    path = os.path.join(os.path.dirname(dag_dir), 'sql', 'pipeline_dag_queries', filename)
    with open(path, 'r') as file:
        template = Template(file.read())
    return Template(file.read()).render(**kwargs)

def run_sql_file(dag_dir: str, conn_id: str, filename: str, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    sql = render_sql_template(dag_dir, filename, **kwargs)
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        log.info(f"Executing SQL: {stmt[:100]}...")
        hook.run(stmt)