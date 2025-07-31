import os
from jinja2 import Template
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

log = logging.getLogger(__name__)

def render_sql_template(dag_dir: str, filename: str, **kwargs) -> str:
    """
    Loads and renders a Jinja SQL template with provided kwargs.
    """
    path = os.path.join(os.path.dirname(dag_dir), 'sql', 'pipeline_dag_queries', filename)
    with open(path, 'r') as file:
        content = file.read()
    template = Template(content)
    return template.render(**kwargs)

def run_sql_file(dag_dir: str, hook: SnowflakeHook, filename: str, **kwargs):
    """
    Executes SQL statements from a rendered Jinja template using the provided SnowflakeHook.
    """
    sql = render_sql_template(dag_dir, filename, **kwargs)
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        log.info(f"Executing SQL: {stmt[:100]}...")
        hook.run(stmt)