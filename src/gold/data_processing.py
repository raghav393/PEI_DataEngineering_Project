from pyspark.sql.functions import sum,year,col
from pyspark.sql.types import DateType
from dateutil import parser
from pyspark.sql import SparkSession
import os,sys

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

# Get absolute path to the 'code' directory
code_root = os.path.abspath(os.path.join(os.getcwd(), "../../"))
sys.path.insert(0, code_root)
from shared.utils import *

#Function to perform aggregation
def data_aggregation(df):
    try:
    #df_extract_year=df.withColumn("Order_Year",year(col("Order_Date")).cast("Integer"))
        df_agg=df.groupBy("Category","Sub_Category","Customer_ID","Order_Year").agg(sum("Profit").alias("Total_Profit"))
        return df_agg
    except Exception as e:
        raise Exception("Aggregation could not be performed failed with {e}")