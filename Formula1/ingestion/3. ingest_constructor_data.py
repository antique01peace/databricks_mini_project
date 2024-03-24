# Databricks notebook source
# MAGIC %md #### Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %md #####Step 1 - Read json file using Dataframe Reader

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.option("schema", "constructors_schema").json(
    "/mnt/formula1dl/raw/constructors.json"
)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


constructors_df = constructors_df.drop(col("url"))
display(constructors_df.limit(10))

# COMMAND ----------

constructors_df = (constructors_df
                   .withColumnRenamed("constructorId", "constructor_id")
                   .withColumnRenamed("constructorRef", "constructor_ref")
                   .withColumn("data_source", lit(v_data_source))
)

# COMMAND ----------


constructors_df = add_ingestion_date(constructors_df)

# COMMAND ----------

display(constructors_df.limit(10))

# COMMAND ----------

constructors_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
