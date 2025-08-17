# Databricks notebook source
# MAGIC %md
# MAGIC **Adding autoreload so that if any changes are done in corresponding Python file. It gets reflected here automatically**

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing the modules**

# COMMAND ----------

from data_processing import *
from shared.utils import *
from schema.schemas import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "16")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the enriched table loaded in silver layer**

# COMMAND ----------

df_read=spark.read.format("delta").load("abfss://silver@dbinterviewvivek1.dfs.core.windows.net/merged")

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching the dataframe**

# COMMAND ----------

df_read.cache()
df_read.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Performing Aggregation on CUSTOMER_ID,ORDER_YEAR,CATEGORY,SUB_CATEGORY**

# COMMAND ----------

df_agg=data_aggregation(df_read)

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing Aggregated data in delta table**

# COMMAND ----------

df_agg.write.format("delta").partitionBy("Order_year").mode("overwrite").option("path","abfss://gold@dbinterviewvivek1.dfs.core.windows.net/sales_snapshot").saveAsTable("dbcatalog.dbgold.sales_snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimizing the delta table**

# COMMAND ----------

spark.sql("OPTIMIZE dbcatalog.dbgold.sales_snapshot ZORDER BY (Customer_ID,CATEGORY)")