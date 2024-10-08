from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


with DAG(
    "agg",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description="Transform location to address using API",
    schedule="* */1 * * *",
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=[
        "agg",
        "gender",
        "address",
    ],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    task_spark = BashOperator(
        task_id="spark.exec",
        bash_commands="$SPARK_HOME/bin/spark-submit ~/code/DE32-3rd_team5/data/data.py",
    )

    start >> task_spark >> end
