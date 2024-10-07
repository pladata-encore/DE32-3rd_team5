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
def geom_trans(latitude: str, longitude: str):
	# lat : 위도
	# lon : 경도
	# 결과값 : add - str
	from de32_3rd_team5.geoutil import loc_trans

def db_check_func():
    # TODO
    # 업데이트 해야하는 DB 체크
	conn = MySqlHook(mysql_conn_id='pic_db')
	with conn.get_conn() as connection:
		cur = connection.cursor()
		cur.execute("SELECT COUNT(*) FROM picture WHERE address = ''")
		# address 컬럼이 비어있는 행 개수 확인
		empty_count = cur.fetchone()[0]
	if empty_count == 0:
		return 'a.noupdate'
		# 비어있는 행이 없으면 need_not_update 태스크로 이동
	else:
		return 'db.update'

def db_update_func():
	from de32_3rd_team5.geoutil import loc_trans
    # TODO
    # 업데이트 진행
	conn = MySqlHook(mysql_conn_id='pic_db')
	with conn.get_conn() as connection:
		cur = connection.cursor()
		#TODO
		# 1. address column이 비어있는 행의 latitude와 longitude를
		# location = f"{latitude}, {longitude}"의 꼴로 location에 저장
		
		# 2. add = trans_loc(location)으로 add를 변환한 후
		# 3. address에 add를 삽입하기
		cur.execute("SELECT latitude, longitude FROM picturedb WHERE address IS NULL")
		rows = cur.fetchall()

		for row in rows:
			latitude, longitude = row
			location = f"{latitude}, {longitude}"
			add = loc_trans(location)
			cur.execute("UPDATE picturedb SET address = %s WHERE latitude = %s AND longitude = %s", (add, latitude, longitude))

	conn.close_conn()

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
    schedule="3 * * * *",
    start_date=datetime(2024, 10, 1),
    catchup=True,
    tags=['API','geometry_transform'],


) as dag:
    

    # Line notify 로그 적극 사용해보고자 함.
    #         login_fail   nothing_to_update    error
    # start >> db_login >> db_check >>          update >> task_succ >> end
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='all_done')
    
    db_check = BranchPythonOperator(
        task_id='db.check',
        python_callable=db_check_func,
    )
    db_update = PythonVirtualenvOperator(
        task_id='db.update',
        python_callable=db_update_func,
		requirements=['git+https://github.com/pladata-encore/DE32-3rd_team5.git@0.1/fastapi_upload_imagefile'],
    )
    task_succ = BashOperator(
		task_id='a.succ',
		bash_command="""
        curl -X POST -H 'Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52' -F 'message=task Airflow tasks complete.' https://notify-api.line.me/api/notify
        """,
		trigger_rule='one_success',
	)
    need_not_update = BashOperator(
		task_id='a.noupdate',
		bash_command="""
        curl -X POST -H 'Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52' -F 'message=task Airflow tasks will close. Databases are not need update' https://notify-api.line.me/api/notify
		""",
	)

start >> db_check >> db_update >> task_succ >> end
db_check >> need_not_update >> task_succ >> end
