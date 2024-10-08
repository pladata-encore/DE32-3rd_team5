from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator,
)
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import mysql.connector
from de32_3rd_team5.data_generator import run  


with DAG(
    'Transfer_Location',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        'execution_timeout': timedelta(hours=2),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='Transform location to address using API',
    schedule="*/10 * * * *",
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=['API','geometry_transform'],


) as dag:

def data_generator_func():
	run()

    # Line notify 로그 적극 사용해보고자 함.
    #         login_fail   nothing_to_update    error
    # start >> db_login >> db_check >>          update >> task_succ >> end
	start = EmptyOperator(task_id='start')
	end = EmptyOperator(
		task_id='end', 
		trigger_rule='all_done',
	)
	data_generator = PythonOperator(
	    task_id='data.generator',
        python_callable=data_generator_func,
	)
	

start >> data_generator >> end
