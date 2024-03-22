# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/Client id, directory/tenant and secret
# MAGIC 4. Assign role "Storage Blob Data Contributor" to the data lake

# COMMAND ----------

client_id = "84e235c8-0067-41d3-990e-6b19761833b8"
tenant_id = "e9574697-1ca1-46b3-a2ed-63b9ee25bd83"
client_secret = "yaT8Q~uG3lLbllWdW64J_ueTmZW4yRT4HrJ4LaYX"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakesbwsstrg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakesbwsstrg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakesbwsstrg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakesbwsstrg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakesbwsstrg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalakesbwsstrg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalakesbwsstrg.dfs.core.windows.net/circuits.csv"))