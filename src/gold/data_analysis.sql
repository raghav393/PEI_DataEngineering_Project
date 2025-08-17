-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Profit by Year**

-- COMMAND ----------

SELECT ORDER_YEAR,ROUND(SUM(TOTAL_PROFIT),2) as TOTAL_PROFIT FROM dbcatalog.dbgold.sales_snapshot group by ORDER_YEAR
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Profit by Year + Product Category**

-- COMMAND ----------

SELECT CATEGORY,ORDER_YEAR,ROUND(SUM(TOTAL_PROFIT),2) as TOTAL_PROFIT FROM dbcatalog.dbgold.sales_snapshot group by CATEGORY,ORDER_YEAR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Profit by Customer**

-- COMMAND ----------

SELECT CUSTOMER_ID,ROUND(SUM(TOTAL_PROFIT),2) as TOTAL_PROFIT FROM dbcatalog.dbgold.sales_snapshot group by CUSTOMER_ID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Profit by Customer + Year**

-- COMMAND ----------

SELECT CUSTOMER_ID,ORDER_YEAR,ROUND(SUM(TOTAL_PROFIT),2) as TOTAL_PROFIT FROM dbcatalog.dbgold.sales_snapshot group by CUSTOMER_ID,ORDER_YEAR