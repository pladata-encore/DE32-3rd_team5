from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
#from utils.geoutil import reverse_geo
from de32_3rd_team5.geoutil import reverse_geo
import pandas as pd
import uuid
import os




# 데이터베이스 확인 함수
"""
    address의 값이 NULL인 데이터를 호출해 온다.
"""


def db_check_func():
    conn = MySqlHook(mysql_conn_id="pic_db")
    if conn:
        with conn.get_conn() as connection:
            cur = connection.cursor()
            cur.execute("SELECT COUNT(*) FROM picture WHERE address IS NULL")
            empty_count = cur.fetchone()[0]

            if empty_count == 0:
                return "a.noupdate"
            return "db.update"
    return "e.login"

# 데이터베이스 업데이트 및 추출함수
"""
    NULL인 데이터는 reverse_geo() 함수를 사용하여 주소 값을 채워 준다.
	이렇게 채워진 데이터들에 한 해서 
"""


def db_update_func(**context):
    conn = MySqlHook(mysql_conn_id="pic_db")
    try:
        with conn.get_conn() as connection:
            cur = connection.cursor()
            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)
            cur.execute("SELECT latitude, longitude FROM picture WHERE address IS NULL")
            rows = cur.fetchall()
            print("Task 1 Success")

            for latitude, longitude in rows:
                add = reverse_geo(latitude, longitude) # 24.10.08 수정
                print(add, latitude, longitude)
                cur.execute("UPDATE picture SET address = %s WHERE latitude = %s AND longitude = %s", (add, latitude, longitude))
                connection.commit()
            
            print("Task 2 Success")
            execution_date = context['execution_date']
            year = execution_date.year
            month = execution_date.month
            day = execution_date.day
            hour = execution_date.hour
			

            # 저장 경로를 생성합니다.
            save_path = f"data/{year}/{month}/{day}/{hour}"
            os.makedirs(save_path, exist_ok=True)  # 디렉토리가 없으면 생성합니다.

            # uuid를 생성합니다.
            unique_filename = str(uuid.uuid4())
            updated_query = f"""
                SELECT *
                FROM picture
                WHERE address IS NOT NULL
                AND request_time >= '{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            cur.execute(updated_query)
            updated_rows = cur.fetchall()
            print("Task 3 Success")

            # Pandas DataFrame으로 변환
            columns = [desc[0] for desc in cur.description]  # 컬럼 이름 가져오기
            df = pd.DataFrame(updated_rows, columns=columns)
			
            # DataFrame을 parquet 파일로 저장합니다.
            df.to_parquet(f"{save_path}/{unique_filename}.parquet")
            print("Task 4 Success")
        connection.commit()

        return "a.succ"

    except Exception as e:
        context["ti"].xcom_push(key="error_message", value=str(e))
        return "e.update"


# 공통적으로 사용되는 LINE Notify 함수
"""
    여러 함수에서 사용할 수 있도록 Line Noti 함수 분리
"""


def send_line_notify(message):
    return dedent(
        f"""
    curl -X POST -H 'Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52' \
    -F 'message={message}' https://notify-api.line.me/api/notify
    """
    )


# DAG 정의
with DAG(
    "Transfer_Location_v2",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description="Transform location to address using API",
    schedule="0 * * * *",
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=["API", "geometry_transform"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # 데이터베이스 체크 및 분기 처리
    db_check = BranchPythonOperator(
        task_id="db.check",
        python_callable=db_check_func,
    )

    # 데이터베이스 업데이트 처리
    db_update = BranchPythonOperator(
        task_id="db.update",
        python_callable=db_update_func,
        provide_context=True,
    )

    # 성공 및 에러 처리
    task_succ = BashOperator(
        task_id="a.succ",
        bash_command=send_line_notify("모든 작업을 정상적으로 성공하였습니다."),
        trigger_rule="one_success",
    )

    # 업데이트 할 데이터가 없을 경우
    need_not_update = BashOperator(
        task_id="a.noupdate",
        bash_command=send_line_notify("데이터 베이스에 업데이트할 데이터가 없습니다."),
    )

    error_login = BashOperator(
        task_id="e.login",
        bash_command=send_line_notify(
            "Task Airflow failed login process. Please check database server online."
        ),
        trigger_rule="one_failed",
    )

    error_update = BashOperator(
        task_id="e.update",
        bash_command=dedent(
            """
        error_message="{{ ti.xcom_pull(task_ids='db.update', key='error_message') }}"
        if [ -z "$error_message" ]; then
            error_message="Unknown error occurred"
        fi

        curl -X POST -H "Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52" \
        -F "message=🚨 db_update 태스크에서 에러 발생! 🚨\n\n$error_message" \
        https://notify-api.line.me/api/notify
        """
        ),
        trigger_rule="one_success",
    )

    # DAG 흐름 설정
    start >> db_check >> [db_update, need_not_update, error_login]
    db_update >> task_succ >> end
    db_update >> error_update >> end
    need_not_update >> task_succ >> end
    error_login >> end
