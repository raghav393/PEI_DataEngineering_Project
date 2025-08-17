# spark_utils.py
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.getActiveSession() or SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
