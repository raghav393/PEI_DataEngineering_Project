import pytest
import pyspark
import sys
import os
from spark_utils import get_spark
import importlib.util
from pyspark.sql.types import StructType, StructField, StringType,DoubleType,DateType
from datetime import datetime

# Get current working directory (usually the notebook or script location)
cwd = os.getcwd()
# Add the repo root or 'code' folder to sys.path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(repo_root)
print("Repo root added to sys.path:", repo_root)

#Importing the schemas
from schema.schemas import *

#Setting the utils directory path
utils_path = os.path.join(repo_root, "shared", "utils.py")
spec = importlib.util.spec_from_file_location("utils", utils_path)
print(spec)
utils = importlib.util.module_from_spec(spec)

try:
    spec.loader.exec_module(utils)
except Exception as e:
    print("❌ Failed to load utils.py:", e)

#Setting the silver directory path
src_path = os.path.join(repo_root, "src", "silver","data_cleansing.py")
spec = importlib.util.spec_from_file_location("silver", src_path)
silver = importlib.util.module_from_spec(spec)

try:
    spec.loader.exec_module(silver)
except Exception as e:
    print("❌ Failed to load data_cleansing.py:", e)

#Getting the spark session
spark = get_spark()

#Loading rules config file
config=silver.load_config_rules()


@pytest.fixture
def sampledataset():
    data_products=[("ABCDF123","Furniture","Tables & Chairs"),(None,"Furniture","Tables & Chairs"),("","Furniture","Tables & Chairs"),("ABCDF123","Electronic","Washing Machine")]
    schema = StructType([
            StructField("Product_ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub_Category", StringType(), True)
    ])
    df_products=spark.createDataFrame(data=data_products,schema=schema)

    data_customers=[("Ram","1234","US"),("Shyam",None,"India"),("Ramesh","","India"),("Shyam","1234","India"),("BCDF$/#%^G1234","2345","US"),("BCDF....G1234","4567A","US")]
    schema_customer=StructType([
            StructField("Customer_Name", StringType(), True),
            StructField("Customer_ID", StringType(), True),
            StructField("Country", StringType(), True)
        ])
    df_customers=spark.createDataFrame(data=data_customers,schema=schema_customer)

    data_orders=[("123","2025/01/01","1234","ABCDF123",100.23),("124","01/2/2025","1234","ABCDF123",150.45),("123","1/1/2024","1234","ABCDF123",100.23),(None,"2025/01/01","1234","ABCDF123",100.23),("","2025/01/01","1234","ABCDF123",100.23)]
    schema_orders=StructType([
            StructField("Order_ID", StringType(), True),
            StructField("Order_Date", StringType(), True),
            StructField("Customer_ID", StringType(), True),
            StructField("Product_ID", StringType(), True),
            StructField("Profit", DoubleType(), True)
        ])
    df_orders=spark.createDataFrame(data=data_orders,schema=schema_orders)
    return {"customers":df_customers,"products":df_products,"orders":df_orders}

@pytest.mark.unittest
@pytest.mark.usefixtures("sampledataset")
def test_cleansing_from_columns(sampledataset):
        dataframes=sampledataset
        results=utils.cleansing(dataframes,config)
        df_products_cleansed=results["products"]
        df_customers_cleansed=results["customers"]
        df_orders_cleansed=results["orders"]
        df_orders_final_cleansed=silver.date_formatting(df_orders_cleansed)

        expected_products_output=[("ABCDF123","Furniture","Tables & Chairs")]
        schema = StructType([
            StructField("Product_ID", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Sub_Category", StringType(), True)
        ])
        df_expected_products=spark.createDataFrame(data=expected_products_output,schema=schema)

        expected_customer_output=[("Ram","1234","US")]
        schema_customer=StructType([
            StructField("Customer_Name", StringType(), True),
            StructField("Customer_ID", StringType(), True),
            StructField("Country", StringType(), True)
        ])
        df_expected_customer=spark.createDataFrame(data=expected_customer_output,schema=schema_customer)

        expected_orders_output=[("123",datetime.strptime("2025-01-01", "%Y-%m-%d").date(),"1234","ABCDF123",100.23),("124",datetime.strptime("2025-01-02", "%Y-%m-%d").date(),"1234","ABCDF123",150.45)]
        schema_orders=StructType([
            StructField("Order_ID", StringType(), True),
            StructField("Order_Date", DateType(), True),
            StructField("Customer_ID", StringType(), True),
            StructField("Product_ID", StringType(), True),
            StructField("Profit", DoubleType(), True)
        ])

        df_expected_orders=spark.createDataFrame(data=expected_orders_output,schema=schema_orders)

        # Validate column alignment
        assert sorted(df_products_cleansed.collect()) == sorted(df_expected_products.collect()), \
            f"Test case 1 failed: expected {df_products_cleansed.collect()}, got {df_expected_products.collect()}"
        print(f"✅ Test case 1 passed. Expected {df_expected_products.collect()}, present {df_products_cleansed.collect()} ")
        assert sorted(df_customers_cleansed.collect()) == sorted(df_expected_customer.collect()), \
            f"Test case 2 failed: expected {df_customers_cleansed.collect()}, got #{df_expected_customer.collect()}"
        print(f"✅ Test case 2 passed. Expected {df_expected_customer.collect()}, present {df_customers_cleansed.collect()} ")
        assert sorted(df_orders_final_cleansed.collect()) == sorted(df_expected_orders.collect()), \
            f"Test case 3 failed: expected {df_orders_final_cleansed.collect()}, got {df_expected_orders.collect()}"
        print(f"✅ Test case 3 passed. Expected {df_expected_orders.collect()}, present {df_orders_final_cleansed.collect()} ")