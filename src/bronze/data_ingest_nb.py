# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC **INGESTING CUSTOMERS, ORDERS, PRODUCTS DATA INTO BRONZE LAYER FROM RAW FILES**

# COMMAND ----------

from data_ingestion import *
from shared.utils import *
from schema.schemas import *
from pyspark.sql.functions import *

# COMMAND ----------

def get_job_parameters():
    #orders_source = dbutils.widgets.get("source1")
    #products_source = dbutils.widgets.get("source2")
    #customer_source = dbutils.widgets.get("source3")
    orders_source="Orders"
    products_source="Products"
    customer_source="Customer"
    return {"orders_source":orders_source,"products_source":products_source,"customer_source":customer_source}

# COMMAND ----------

job_parameters=get_job_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching The DataFrames**

# COMMAND ----------

read_dataframes=read_data(job_parameters)
df_products=read_dataframes["products"]
df_orders=read_dataframes["orders"]
df_customers=read_dataframes["customer"]

# COMMAND ----------

df_products.cache()
df_orders.cache()
df_customers.cache()
df_products.limit(5).display()
df_orders.limit(5).display()
df_customers.limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Evolution**

# COMMAND ----------

schemas = schemas()
products_schema=schemas["products"]
orders_schema=schemas["orders"]
customer_schema=schemas["customer"]

# COMMAND ----------

evolve_map=evolve_schema(read_dataframes,schemas)
df_productsschemaevolved=evolve_map["df_productsschemaevolved"]
df_ordersschemaevolved=evolve_map["df_ordersschemaevolved"]
df_customersschemaevolved=evolve_map["df_customerschemaevolved"]    

# COMMAND ----------

# MAGIC %md
# MAGIC **Cleansing The Column Names**

# COMMAND ----------

cleansed=cleanse_columns(evolve_map,read_dataframes)
df_productscolumnsrenamed=cleansed["df_productscolumnsrenamed"]
df_orderscolumnsrenamed=cleansed["df_orderscolumnsrenamed"]
df_customercolumnsrenamed=cleansed["df_customercolumnsrenamed"]

# COMMAND ----------

write_productsdata(df_productscolumnsrenamed,job_parameters)
write_ordersdata(df_orderscolumnsrenamed,job_parameters)
write_customersdata(df_customercolumnsrenamed,job_parameters)