import pytest
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import sys
import os
from spark_utils import get_spark
import importlib.util


# Get current working directory (usually the notebook or script location)
cwd = os.getcwd()
# Add the repo root or 'code' folder to sys.path
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(repo_root)
print("Repo root added to sys.path:", repo_root)
print("utils.py exists:", os.path.exists(os.path.join(repo_root, "shared", "utils.py")))

from schema.schemas import *

utils_path = os.path.join(repo_root, "shared", "utils.py")
spec = importlib.util.spec_from_file_location("utils", utils_path)
utils = importlib.util.module_from_spec(spec)

try:
    spec.loader.exec_module(utils)
except Exception as e:
    print("❌ Failed to load utils.py:", e)

spark = get_spark()

def create_dummy_df(column_names):
    return spark.createDataFrame([["dummy"] * len(column_names)], column_names)

schema_list=schemas()

@pytest.fixture
def samplecolumns():
    return {"products":[
        {
        "columns":["Product ID","Category","Sub-Category","Product Name","State","Price per product"],"expected":["Product ID","Category","Sub-Category","Product Name","State","Price per product"],
                                  "schema":schema_list["products"]},{"columns":["Product ID","Category","Sub-Category","Product Name","State","Price per product","ID"],"expected":["Product ID","Category","Sub-Category","Product Name","State","Price per product","ID"],
                                  "schema":schema_list["products"]
                                      
                                  },{"columns":["Product ID","Category","Sub-Category","Product Name","Price per product"],"expected":["Product ID","Category","Sub-Category","Product Name","State","Price per product"],
                                  "schema":schema_list["products"]
                                      
                                  },{"columns":["Product ID","Category","Sub-Category","Product Nme","Price per product","State"],"expected":["Product ID","Category","Sub-Category","Product Name","State","Price per product","Product Nme"],
                                  "schema":schema_list["products"]
                                      
                                  }
    ]
    }

@pytest.mark.unittest
@pytest.mark.usefixtures("samplecolumns")
def test_schema_evolution_from_columns(samplecolumns, filename="products"):
    configs = samplecolumns[filename]

    for idx, config in enumerate(configs):
        expected_columns = config["expected"]
        input_schema = config["schema"]
        input_columns=config["columns"]

        # Create dummy DataFrame
        df = create_dummy_df(input_columns)

        # Apply schema evolution
        evolved_df=  utils.data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark).schema_evolution(df, input_schema)


        # Validate column alignment
        assert sorted(evolved_df.columns) == sorted(expected_columns), \
            f"Test case {idx + 1} failed: expected {expected_columns}, got {evolved_df.columns}"
        
        print(f"Test case {idx + 1} passed.")

@pytest.fixture
def sampledataset():
    return {"products":[
        {
        "columns":["Product ID","Category","Sub-Category","Product Name","State","Price per product"],"expected":["Product_ID","Category","Sub_Category","Product_Name","State","Price_per_product"],
                                  "schema":schema_list["products"]}
                                                                      
    ]
    }

@pytest.mark.unittest
@pytest.mark.usefixtures("sampledataset")
def test_cleansing_columns(sampledataset, filename="products"):
    configs = sampledataset[filename]

    for idx, config in enumerate(configs):
        expected_columns = config["expected"]
        input_schema = config["schema"]
        input_columns=config["columns"]

        # Create dummy DataFrame
        df = create_dummy_df(input_columns)

        # Apply columns cleansing
        evolved_df = utils.data_ingest("abfss://config@dbinterviewvivek1.dfs.core.windows.net/config.json",spark) \
                        .column_names_cleansing(df)


        # Validate column alignment
        assert sorted(evolved_df.columns) == sorted(expected_columns), \
            f"Test case {idx + 1} failed: expected {expected_columns}, got {evolved_df.columns}"
        
        print(f"Test case {idx + 1} passed: expected {expected_columns}, present {evolved_df.columns}")
