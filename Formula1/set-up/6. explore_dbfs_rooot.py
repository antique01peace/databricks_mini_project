# Databricks notebook source
# MAGIC %md
# MAGIC ##### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS file browser
# MAGIC 3. Upload file to DBFS root 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

circuitsDF = spark.read.option("header","true").csv('/FileStore/circuits.csv')

# COMMAND ----------

display(circuitsDF)

# COMMAND ----------


display(circuitsDF.printSchema)

# COMMAND ----------

