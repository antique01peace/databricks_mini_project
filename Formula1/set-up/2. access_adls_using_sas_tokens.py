# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access Azure Data Loke using access keys
# MAGIC 1. Set the authentication type in spark.config as SAS
# MAGIC 2. List files in demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakesbwsstrg.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalakesbwsstrg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalakesbwsstrg.dfs.core.windows.net", "?sv=2023-11-03&st=2024-03-11T20%3A25%3A21Z&se=2024-03-12T20%3A25%3A21Z&sr=c&sp=rl&sig=HLpdHqkGOha1GYd%2Fba7GX5RjsaOzjRrdn8H3ZPdYaoM%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalakesbwsstrg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalakesbwsstrg.dfs.core.windows.net/circuits.csv"))
