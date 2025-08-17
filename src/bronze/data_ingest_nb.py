# Databricks notebook source
# MAGIC %md
# MAGIC **Adding autoreload so that if any changes are done in corresponding Python file. It gets reflected here automatically**

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC **importing the modules**

# COMMAND ----------

from data_ingestion import *
from shared.utils import *
from schema.schemas import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing the values of job Parameters**

# COMMAND ----------

def get_job_parameters():
    orders_source = dbutils.widgets.get("source1")
    products_source = dbutils.widgets.get("source2")
    customer_source = dbutils.widgets.get("source3")
    return {"orders_source":orders_source,"products_source":products_source,"customer_source":customer_source}

# COMMAND ----------

job_parameters=get_job_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading source files**

# COMMAND ----------

read_dataframes=read_data(job_parameters)
df_products=read_dataframes["products"]
df_orders=read_dataframes["orders"]
df_customers=read_dataframes["customer"]

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching the dataframes**

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

# MAGIC %md
# MAGIC **Performing Schema Evolution**

# COMMAND ----------

schemas = schemas()
products_schema=schemas["products"]
orders_schema=schemas["orders"]
customer_schema=schemas["customer"]

# COMMAND ----------

# MAGIC %md
# MAGIC **Cleansing Column Names**

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

# MAGIC %md
# MAGIC **Writing the data to delta tables**

# COMMAND ----------

write_productsdata(df_productscolumnsrenamed,job_parameters)
write_ordersdata(df_orderscolumnsrenamed,job_parameters)
write_customersdata(df_customercolumnsrenamed,job_parameters)