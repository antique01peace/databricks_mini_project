# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id amd client_secret from key vault
# MAGIC 2. Set Spark Cinfig with App/ClientID, Directory/TenantID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'clientID')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'clientSecret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------


dbutils.fs.mount(
  source = "abfss://presentation@datalakesbwsstrg.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/presentation",
  extra_configs = configs)



# COMMAND ----------

display(spark.read.option("header","true").csv("/mnt/formula1dl/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


