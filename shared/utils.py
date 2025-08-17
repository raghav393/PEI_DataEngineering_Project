# Databricks notebook source
from pyspark.sql.functions import lit
class data_ingest:
    def __init__(self,config_path: str,spark):
        self.spark=spark
        self.config = self._load_config(config_path)
        self.sources = {src["name"]: src for src in self.config["sources"]}
    
    def _load_config(self,path:str):
        try:
            df = self.spark.read.format("json").option("multiline",True).load(path)
            config_row = df.select("sources").collect()[0]
            return {"sources": config_row["sources"]}
        except Exception as e:
            raise FileNotFoundError(f"Config File not found and failed with error {e}")
    
    def import_config(self,name):
        source=self.sources.get(name)
        if not source:
            raise ValueError(f"Source '{name}' not found in config.")

        filename=source["filename"]
        filetype=source["format"]
        bronze_write=source["bronze_write"]
        bronze_write_options={k: v for k, v in bronze_write.asDict().items() if v is not None}
        options_read=source["options"]
        options = {k: v for k, v in options_read.asDict().items() if v is not None}
        return {"filename":filename,"filetype":filetype,"bronze_write_options":bronze_write_options,"options":options}
    
    def schema_evolution(self,df,schema):
        try:
            actual_columns=df.columns
            #expected_columns=[field.name for field in schema.fields]
            extra_columns=[col for col in actual_columns if col not in schema]
            if extra_columns is None:
                expected_order=schema
            else:
                expected_order=schema +extra_columns
            for col in actual_columns:
                if extra_columns is not None:
                    if col in extra_columns:
                        df = df.withColumn(col, lit(None))
                else:
                    df=df
            for col in schema:
                if col not in actual_columns:
                    df = df.withColumn(col, lit(None))
            df=df.select([col for col in expected_order])
            return df
        except Exception as e:
            raise ValueError(f"Schema validation failed and failed with error {e}")
    
    def column_names_cleansing(self,df):
        try:
            column_list=df.columns
            for col in column_list:
                df=df.withColumnRenamed(col,col.replace(" ","_"))
                df=df.withColumnRenamed(col,col.replace("-","_"))
            return df
        except Exception as e:
            raise ValueError(f"Column names could not be cleansed and failed with error {e}")

    def read_data(self,name):
        try:
            config=self.import_config(name)
            options=config["options"]
            print(options)
            df_read=self.spark.read.format(config["filetype"]).options(**options).load(config["filename"])
            return df_read
        except Exception as e:
            raise ValueError(f"values could not be read from config file and failed with error {e}")

    def write_delta_tables(self,df,name):
        try:
            config=self.import_config(name)
            bronze_write_options=config["bronze_write_options"]
            return df.write.mode(bronze_write_options["mode"]).format(bronze_write_options["format"]).option("path",bronze_write_options["external_location"]).saveAsTable(bronze_write_options["table_name"])
        except Exception as e:
            raise ValueError(f"Table could not be created and failed with error {e}")   

# COMMAND ----------

from pyspark.sql.functions import col
from typing import Dict, Any
import re
import json

def cleanse_dataframe(df, rules: Dict[str, Any]):
    for column, conditions in rules.get("filters", {}).items():
        for cond in conditions:
            if cond == "not_null":
                df = df.filter(col(column).isNotNull())
            elif cond == "not_empty":
                df = df.filter(col(column) != "")
            elif cond.startswith("regex:"):
                pattern = cond.split("regex:")[1]
                df = df.filter(col(column).rlike(pattern))
    if "dedup_key" in rules:
        df = df.dropDuplicates([rules["dedup_key"]])
    return df

def cleansing(dataframes: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    cleansed = {}
    for name, df in dataframes.items():
        rules = config.get(name, {})
        cleansed[name] = cleanse_dataframe(df, rules)
    return cleansed

# COMMAND ----------

def load_config_from_adls(config_path: str,spark) -> Dict[str, Any]:
    # Read the config file as text from ADLS
    lines = spark.read.text(config_path).rdd.map(lambda r: r[0]).collect()
    
    # Join all lines to rebuild the full JSON string
    json_str = "\n".join(lines)
    
    # Parse into Python dictionary
    return json.loads(json_str)



# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
def path_exists(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except:
        return False

def merge_data(df,table_name,key,name,path,spark):
    if path_exists(path):

        try:
            target_table = DeltaTable.forPath(spark, path)
            target_table.alias("target").merge(
                df.alias("source"),
                f"target.{key} = source.{key}"  # Replace with your unique key
            ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        except AnalysisException as e:
            print(f"DeltaTable.forPath failed: {e}")
            return
    else:
        print(f"Table {table_name} not found. Creating new Delta table.")
        df.coalesce(5).write.format("delta").mode("overwrite").option("path",path).saveAsTable(table_name)


    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({key})")
