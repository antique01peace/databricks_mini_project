# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data frpm circuits.csv file 

# COMMAND ----------

account_key = dbutils.secrets.get(scope='formula1-scope',key='StorageAccountKey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakesbwsstrg.dfs.core.windows.net",
    account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalakesbwsstrg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalakesbwsstrg.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


