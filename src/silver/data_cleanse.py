# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "16") 

# COMMAND ----------

from data_cleansing import *
from shared.utils import *
from schema.schemas import *
from pyspark.sql.functions import *

# COMMAND ----------

dataframes=read_delta_tables()

# COMMAND ----------

dataframes["customers"].cache()
dataframes["products"].cache()
dataframes["orders"].cache()
dataframes["customers"].limit(5).display()
dataframes["products"].limit(5).display()
dataframes["orders"].limit(5).display()

# COMMAND ----------

config=load_config_rules()

# COMMAND ----------

cleansed_data = cleansing(dataframes, config)

# COMMAND ----------

df_cleansecustomers=cleansed_data["customers"]
df_cleanseproducts=cleansed_data["products"]
df_cleanseorders=cleansed_data["orders"]

# COMMAND ----------

df_orderscleansed=date_formatting(df_cleanseorders)

# COMMAND ----------

merge_data(df_cleansecustomers,"dbcatalog.dbsilver.customer","Customer_ID","customer","abfss://silver@dbinterviewvivek1.dfs.core.windows.net/customer",spark)
merge_data(df_cleanseproducts,"dbcatalog.dbsilver.products","Product_ID","products","abfss://silver@dbinterviewvivek1.dfs.core.windows.net/products",spark)

# COMMAND ----------

df_joined_orders_customers=df_orderscleansed.alias("orders").join(broadcast(df_cleansecustomers).alias("customers"),on=col("customers.Customer_ID")==col("orders.Customer_ID"),how="inner").select("Order_ID","Order_Date","customers.Customer_ID","Customer_Name","Country","Product_ID",round(col("orders.Profit"),2).alias("Profit"))
df_joined=df_joined_orders_customers.alias("joined").join(broadcast(df_cleanseproducts).alias("products"),on=col("products.Product_ID")==col("joined.Product_ID"),how="inner").select("joined.Customer_Name","joined.Customer_ID","joined.Country","joined.Order_ID","joined.Order_Date",round(col("joined.Profit"),2).alias("Profit"),"products.Product_ID","products.Category","products.Sub_Category")
df_final=df_joined.withColumn("Order_year",year(col("Order_Date")).cast("Integer"))


# COMMAND ----------

df_final.write.format("delta").partitionBy("Order_year").mode("overwrite").option("path","abfss://silver@dbinterviewvivek1.dfs.core.windows.net/merged").saveAsTable("dbcatalog.dbsilver.enriched_table")

# COMMAND ----------

spark.sql("OPTIMIZE dbcatalog.dbsilver.enriched_table ZORDER BY (Customer_ID,Category)")