import pytest
import pyspark
import sys
import os
from spark_utils import get_spark
import importlib.util
from pyspark.sql.types import StructType, StructField, StringType,DoubleType,DateType,IntegerType
from datetime import datetime

# Get current working directory (usually the notebook or script location)
cwd = os.getcwd()
# Add the repo root or 'code' folder to sys.path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(repo_root)
print("Repo root added to sys.path:", repo_root)


src_path = os.path.join(repo_root, "src", "gold","data_processing.py")
spec = importlib.util.spec_from_file_location("gold", src_path)
gold = importlib.util.module_from_spec(spec)

try:
    spec.loader.exec_module(gold)
except Exception as e:
    print("❌ Failed to load data_processing.py:", e)

spark = get_spark()

@pytest.fixture
def sample_dataset():
    data=[("Ram","123","India","O1234",datetime.strptime("2025-01-01", "%Y-%m-%d").date(),150.00,"P123","Furniture","Tables & Chairs",2025),("Ram","123","India","O1235",datetime.strptime("2025-01-02", "%Y-%m-%d").date(),160.00,"P123","Furniture","Tables & Chairs",2025),("Ram","123","India","O1236",datetime.strptime("2025-01-03", "%Y-%m-%d").date(),170.00,"P124","Electronics","Washing Machine",2025),("Shyam","124","US","O1237",datetime.strptime("2025-01-01", "%Y-%m-%d").date(),250.00,"P124","Electronics","Washing Machine",2025),("Shyam","124","US","O1238",datetime.strptime("2025-01-02", "%Y-%m-%d").date(),1000.00,"P123","Furniture","Tables & Chairs",2025)]
    schema=StructType([
        StructField("Customer_Name",StringType(),True),
        StructField("Customer_ID",StringType(),True),
        StructField("Country",StringType(),True),
        StructField("Order_ID",StringType(),True),
        StructField("Order_Date",DateType(),True),
        StructField("Profit",DoubleType(),True),
        StructField("Product_ID",StringType(),True),
        StructField("Category",StringType(),True),
        StructField("Sub_Category",StringType(),True),
        StructField("Order_year",IntegerType(),True)
    ])

    df_sales_test=spark.createDataFrame(data=data,schema=schema)
    return df_sales_test

@pytest.mark.unittest
@pytest.mark.usefixtures("sample_dataset")
def test_aggregation(sample_dataset):
    df_sales_test=sample_dataset
    df_agg_test=gold.data_aggregation(df_sales_test)
    expected_data=[("123",2025,310.00,"Furniture","Tables & Chairs"),("123",2025,170.00,"Electronics","Washing Machine"),("124",2025,250.00,"Electronics","Washing Machine"),("124",2025,1000.00,"Furniture","Tables & Chairs")]
    schema=StructType([
        StructField("Customer_ID",StringType(),True),
        StructField("Order_year",IntegerType(),True),
        StructField("Total_Profit",DoubleType(),True),
        StructField("Category",StringType(),True),
        StructField("Sub_Category",StringType(),True)
    ])
    df_expected=spark.createDataFrame(data=expected_data,schema=schema)
    df_expected_select=df_expected.select("Category","Sub_Category","Customer_ID","Order_year","Total_Profit")
    assert sorted(df_agg_test.collect())==sorted(df_expected_select.collect()),\
        f"Test case 2 failed: expected {df_agg_test.collect()}, got #{df_expected_select.collect()}"
    print(f"✅ Test case passed. Expected {df_expected_select.collect()}, present {df_agg_test.collect()} ")

