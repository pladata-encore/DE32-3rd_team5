from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    row_number,
    lit,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)
import pymysql
import sys
import os
import requests
from datetime import datetime, timedelta

# SparkSession 생성
spark = SparkSession.builder.appName("DataPartitionning").getOrCreate()

# 실행 날짜 받기
execution_date_str = sys.argv[1] + " " + sys.argv[2]  # '2024-10-05 23:00:00+00:00'
execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d %H:%M:%S%z")

# 한 시간 빼기
execution_date = execution_date - timedelta(hours=1)


def send_line_noti(message):
    api_url = "https://notify-api.line.me/api/notify"
    token = os.getenv("LINE_NOTI_TOKEN", "NULL")
    headers = {"Authorization": "Bearer " + token}
    data = {"message": message}
    response = requests.post(api_url, headers=headers, data=data)
    print(response.text)
    print("SEND LINE NOTI")


def get_db_connection():
    connection = pymysql.connect(
        host=os.getenv("DB_IP", "43.201.252.238"),
        user="pic",
        password="1234",
        port=int(os.getenv("MY_PORT", "32768")),
        database="picturedb",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def deduplicate_write(dataframe, read_path):

    if os.path.exists(read_path):
        # 기존 데이터를 호출
        existing_df = spark.read.format("parquet").load(read_path)

        print("==== 기존 데이터 ====")
        existing_df.printSchema()
        existing_df.show(1)

        # 기존 데이터와 새로운 데이터를 병합
        combined_df = existing_df.union(dataframe)

        # 병합된 데이터에서 num 기준으로 groupby를 하고 etl_dtm이 제일 최신인 데이터만 나두고 이전 데이터는 버린다.
        window_spec = Window.partitionBy("num").orderBy(col("etl_dtm").desc())

        deduplicated_df = (
            combined_df.withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") == 1)
            .drop("rank")
        )

        # 중복 제거된 데이터를 다시 저장
        print(deduplicated_df.count())
        deduplicated_df.write.mode("overwrite").partitionBy(
            "year", "month", "day", "hour"
        ).format("parquet").save(read_path)
    else:
        print(dataframe.count())
        dataframe.write.mode("overwrite").partitionBy(
            "year", "month", "day", "hour"
        ).format("parquet").save(read_path)


def run():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = f"""
            SELECT num, file_name, file_path, gender, score, request_time, request_user, latitude, longitude, address
            FROM picture
            WHERE request_time LIKE '{execution_date.strftime('%Y-%m-%d %H')}%'
            """

            cursor.execute(sql)
            result = cursor.fetchall()

            if len(result) == 0:
                send_line_noti(
                    f"[TEAM 5]\n🚨{execution_date.strftime('%Y-%m-%d %H:%M')}시간에 파티셔닝할 데이터가 없습니다.🚨\n"
                )
                return

            schema = StructType(
                [
                    StructField("num", IntegerType(), True),
                    StructField("file_name", StringType(), True),
                    StructField("file_path", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("score", StringType(), True),
                    StructField("request_time", StringType(), True),
                    StructField("request_user", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("etl_dtm", StringType(), True),
                ]
            )
            df = spark.createDataFrame(result, schema)

            df = df.withColumn(
                "etl_dtm", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

            df = df.withColumn(
                "request_time", to_timestamp(col("request_time"), "yyyy-MM-dd HH:mm:ss")
            )

            df = (
                df.withColumn("year", year("request_time"))
                .withColumn("month", month("request_time"))
                .withColumn("day", dayofmonth("request_time"))
                .withColumn("hour", hour("request_time"))
            )

            partitioning_dir = os.getenv(
                "PARTITINNING_DIR", os.path.expanduser("~/pyspark_data/")
            )
            upload_path = os.path.join(partitioning_dir, "origin_data")

            print("=== 새로운 데이터 ===")
            df.printSchema()
            df.show(1)

            deduplicate_write(df, upload_path)

            send_line_noti(
                f"[TEAM 5]\n✅ 파티셔닝 성공 ✅\n{execution_date.strftime('%Y-%m-%d %H:%M')} 데이터 {len(result)}개 처리"
            )
    except Exception as e:
        send_line_noti(
            f"[TEAM 5]\n🚨{execution_date.strftime('%Y-%m-%d %H:%M')}일자 Task에서 에러 발생🚨\n{str(e)}"
        )
        print(e)
    finally:
        connection.close()
    spark.stop()


if __name__ == "__main__":
    run()
