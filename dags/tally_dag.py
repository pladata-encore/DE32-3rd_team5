from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    "tally",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description="Data Tally",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=["tally", "agg", "avg", "cnt"],
) as dag:

    start = EmptyOperator(task_id="start")
    worker = BashOperator(bash_command="""""")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> worker >> end
