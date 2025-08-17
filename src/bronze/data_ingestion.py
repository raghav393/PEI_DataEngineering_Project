from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import json
from pyspark.sql.functions import lit,col
from pyspark.sql import SparkSession
import os,sys

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

# Get absolute path to the 'code' directory
code_root = os.path.abspath(os.path.join(os.getcwd(), "../../"))
sys.path.insert(0, code_root)
from shared.utils import *


def read_data(job_parameters):
    df_products=data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).read_data(job_parameters["products_source"])
    df_orders=data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).read_data(job_parameters["orders_source"])
    df_customers=data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).read_data(job_parameters["customer_source"])
    return {"products":df_products,"orders":df_orders,"customer":df_customers}


def evolve_schema(read_data,schemas):
    try:
        evolved={}
        config_path="abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json"
        ingestor = data_ingest(config_path,spark)
        for key in read_data:
            evolved[f"df_{key}schemaevolved"] = ingestor.schema_evolution(read_data[key], schemas[key])
        return evolved
    except Exception as e:
        raise ValueError(f"Schema could not be evolved and failed with error {e}")

def cleanse_columns(evolve_map,read_dataframes):
    try:
        renamed={}
        config_path="abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json"
        ingestor = data_ingest(config_path,spark)
        for key in read_dataframes:
            renamed[f"df_{key}columnsrenamed"] = ingestor.column_names_cleansing(evolve_map[f"df_{key}schemaevolved"])
        return renamed
    except Exception as e:
        raise ValueError(f"Columns names could not be cleansed and failed with error {e}")

def write_productsdata(df_productscolumnsrenamed,job_parameters):
    try:
        return data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).write_delta_tables(df_productscolumnsrenamed,job_parameters["products_source"])
    except Exception as e:
        raise ValueError(f"Data could not be written and failed with error {e}")
    
def write_ordersdata(df_orderscolumnsrenamed,job_parameters):
    try:
        return data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).write_delta_tables(df_orderscolumnsrenamed,job_parameters["orders_source"])
    except Exception as e:
        raise ValueError(f"Data could not be written and failed with error {e}")    

def write_customersdata(df_customercolumnsrenamed,job_parameters):
    try:
        return data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).write_delta_tables(df_customercolumnsrenamed,job_parameters["customer_source"])
    except Exception as e:
        raise ValueError(f"Data could not be written and failed with error {e}")

