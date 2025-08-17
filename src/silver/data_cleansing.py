from pyspark.sql.functions import rlike,col,regexp_replace,pandas_udf,broadcast,round
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

#Function read data from bronze delta tables
def read_delta_tables():
    df_customers=spark.read.format("delta").load("abfss://bronze@dbinterviewvivek1.dfs.core.windows.net/customers").select("Customer_Name","Customer_ID","Country")
    df_products=spark.read.format("delta").load("abfss://bronze@dbinterviewvivek1.dfs.core.windows.net/products").select("Product_ID","Category","Sub_Category")
    df_orders=spark.read.format("delta").load("abfss://bronze@dbinterviewvivek1.dfs.core.windows.net/order").select("Order_ID","Order_Date","Customer_ID","Product_ID","Profit")
    return {"customers":df_customers,"products":df_products,"orders":df_orders}

#Pandas UDF to parse different date formats
@pandas_udf(DateType())
def format_dates(date_series):
    return date_series.apply(lambda x: parser.parse(x) if x else None)

#Function to format date
def date_formatting(df):
    df_orderscleansed=df.withColumn("Order_Date",format_dates(col("Order_Date")))
    return df_orderscleansed

#Function to load cleansing rules config file
def load_config_rules():
    config_path = "abfss://config@dbinterviewvivek1.dfs.core.windows.net/rules_config.json"
    config = load_config_from_adls(config_path,spark)
    return config
