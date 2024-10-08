from pyspark.sql import SparkSession

import pymysql
import os
import pandas as pd


spark = SparkSession.builder.appName("agg_data").getOrCreate()


def get_connection():
    connection = pymysql.connect(
        host=os.getenv("DB_IP", "43.201.252.238"),
        user="pic",
        password="1234",
        port=int(os.getenv("MY_PORT", "32768")),
        database="picturedb",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def get_agg_data():
    connection = get_connection()
    cursor = connection.cursor()

    # 랜덤으로 위도, 경도를 가져오는 SQL 쿼리
    sql = """
        SELECT 
            CASE WHEN substring_index(substring_index(address, ' ',2),' ',-1)='포항시' THEN '경상북도'
            ELSE substring_index(substring_index(address, ' ',2),' ',-1) END  as addr, 
            gender 
        FROM picture 
        WHERE address like '대한민국%'
    """
    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()
    connection.close()

    if result:
        # print(result)
        return result
    else:
        print("데이터를 가져오는 데 실패했습니다.")
        return None


df = pd.DataFrame(get_agg_data())

ddf = spark.createDataFrame(df)
ddf.createOrReplaceTempView("agg_data")
agg = spark.sql("SELECT addr, gender, COUNT(*) FROM agg_data GROUP BY addr, gender")
agg.show()

agg.write.mode("append").partitionBy(["addr", "gender"]).parquet(
    f"{os.path.dirname(os.path.abspath(__file__))}/parquet"
)
