# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

def schemas():
    customer_schema=[
        "Customer ID",
        "Customer Name",
        "email",
        "phone",
        "address",
        "Segment",
        "Country",
        "City",
        "State",
        "Postal Code",
        "Region"
    ]

    products_schema=[
        "Product ID",
        "Category",
        "Sub-Category",
        "Product Name",
        "State",
        "Price per product"
    ]

    orders_schema=[
        "Customer ID",
        "Discount",
        "Order Date",
        "Order ID",
        "Price",
        "Product ID",
        "Profit",
        "Quantity",
        "Row ID",
        "Ship Date",
        "Ship Mode"
    ]
    return {"customer":customer_schema,"products":products_schema,"orders":orders_schema}

# COMMAND ----------

