''' 
A basic dbt DAG that shows how to run dbt commands via the BashOperator
Follows the standard dbt run → test → snapshot pattern.
'''

from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import snowflake.connector

DBT_PROJECT_DIR = "/opt/airflow/LAB2_Group_10"
DBT_BIN_DIR = "/home/airflow/.local/bin"  # where pip puts dbt by default

conn = BaseHook.get_connection('snowflake_hw_5_viraat')

with DAG(
    "Lab2_ELT_with_DBT",
    start_date=datetime(2024, 11, 20),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_ROLE": conn.extra_dejson.get("role", "TRAINING_ROLE"), 
            "DBT_TYPE": "snowflake",
        }
    }
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"export PATH=$PATH:{DBT_BIN_DIR} && "
            f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"export PATH=$PATH:{DBT_BIN_DIR} && "
            f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"export PATH=$PATH:{DBT_BIN_DIR} && "
            f"dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run >> dbt_test >> dbt_snapshot