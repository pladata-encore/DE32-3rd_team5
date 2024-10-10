from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


with DAG(
    "hourly_partition_dag",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "start_date": datetime(2024, 10, 7),
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    schedule_interval="@hourly",
) as dag:

    start = EmptyOperator(task_id="start")
    partitionning_worker = BashOperator(
        task_id="run_spark_submit",
        bash_command="""
        $SPARK_HOME/bin/spark-submit --master local /home/ubuntu/airflow/dags/pyspark/origin_data_partition.py {{ execution_date }}""",
    )
    end = EmptyOperator(task_id="end")

    start >> partitionning_worker >> end
