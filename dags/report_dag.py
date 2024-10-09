from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from de32_3rd_team5 import reverse_geo


def db_update_func(**context):
    conn = MySqlHook(mysql_conn_id="pic_db")
    try:
        with conn.get_conn() as connection:
            cur = connection.cursor()
            cur.execute("")
            rows = cur.fetchall()

            cur.execute("")
            connection.commit()

        return "a.succ"

    except Exception as e:
        context["ti"].xcom_push(key="error_message", value=str(e))
        return "e.update"


with DAG(
    "Transfer_Location",
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

    # Line notify 로그 적극 사용해보고자 함.
    #         login_fail   nothing_to_update    error
    # start >> db_login >> db_check >>          update >> task_succ >> end
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done",
    )
    db_check = BranchPythonOperator(
        task_id="db.check",
        python_callable=db_check_func,
    )
    db_update = BranchPythonOperator(
        task_id="db.update", python_callable=db_update_func, provide_context=True
    )
    task_succ = BashOperator(
        task_id="a.succ",
        bash_command="""
        curl -X POST -H 'Authorization: Bearer OtM5cXkXBgCJdkhAUvaJqAszuNWESrXPATwEXMXHRZ0' -F 'message=task Airflow tasks complete.' https://notify-api.line.me/api/notify
        """,
        trigger_rule="one_success",
    )
    need_not_update = BashOperator(
        task_id="a.noupdate",
        bash_command="""
        curl -X POST -H 'Authorization: Bearer OtM5cXkXBgCJdkhAUvaJqAszuNWESrXPATwEXMXHRZ0' -F 'message=task Airflow tasks will close. Databases are not need update' https://notify-api.line.me/api/notify
		""",
    )
    error_login = BashOperator(
        task_id="e.login",
        bash_command="""
		curl -X POST -H 'Authorization: Bearer OtM5cXkXBgCJdkhAUvaJqAszuNWESrXPATwEXMXHRZ0' -F 'message=task Airflow failed login process. Please check database server online' https://notify-api.line.me/api/notify
		""",
        trigger_rule="one_failed",
    )

    error = BashOperator(
        task_id="e.update",
        bash_command=dedent(
            """
		curl -X POST -H "Authorization: Bearer OtM5cXkXBgCJdkhAUvaJqAszuNWESrXPATwEXMXHRZ0" -F "message=🚨 db_update 태스크에서 에러 발생! 🚨\n\n{{ti.xcom_pull(key='error_message')}}" https://notify-api.line.me/api/notify
		"""
        ),
        trigger_rule="one_success",
    )


start >> db_check >> db_update >> task_succ >> end
db_check >> need_not_update >> task_succ >> end
db_check >> error_login >> end
db_update >> error >> end
