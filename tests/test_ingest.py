# Databricks notebook source
# MAGIC %md
# MAGIC **Disabling the I/O cache so that always tests run from a fresh**

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Modules**

# COMMAND ----------

import pytest
import os
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting so that it skips writing to cache file**

# COMMAND ----------

# Skip writing pyc files on a readonly filesystem.
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"


# COMMAND ----------

# MAGIC %md
# MAGIC **Deleting the already existing file from /tmp **

# COMMAND ----------

import shutil
tmp_filename = "/tmp/test_ingestion.py"
tmp_dir = "/tmp"
tmp_path = os.path.join(tmp_dir, tmp_filename)
if os.path.exists(tmp_path):
    try:
        os.remove(tmp_path)
        print(f"Deleted old file: {tmp_path}")
    except Exception as e:
        print(f"Failed to delete {tmp_path}: {e}")




# COMMAND ----------

# MAGIC %md
# MAGIC **Since the repo is read only, hence copying the file to /tmp location**

# COMMAND ----------

shutil.copy("test_ingestion.py", "/tmp/test_ingestion.py")
shutil.copy("spark_utils.py", "/tmp/spark_utils.py")
sys.path.insert(0, "/tmp")

# COMMAND ----------

import sys
sys.modules.pop("test_ingestion", None)


# COMMAND ----------

# MAGIC %md
# MAGIC **Running the tests**

# COMMAND ----------

pytest.main(["/tmp/test_ingestion.py", "-v", "-s", "--cache-clear"])
