from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# SparkSession 생성
spark = SparkSession.builder.appName("ScoreAverage").getOrCreate()
