# Databricks notebook source
# MAGIC %md #### Ingest data from drivers.json

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField

# COMMAND ----------

name_schema = StructType(fields = [
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

driver_schema = StructType(fields=[
    StructField('driverId', StringType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json('/mnt/formula1dl/raw/drivers.json')
display(drivers_df.limit(10))

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

drivers_df = (drivers_df.withColumnRenamed('driverId', 'driver_id')
              .withColumnRenamed('driverRef', 'driver_ref')
              .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))
              .drop(col('url'))
              .withColumn("data_source", lit(v_data_source))
              )

display(drivers_df.limit(10))

# COMMAND ----------

drivers_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl/processed/drivers/').limit(5))

# COMMAND ----------

dbutils.notebook.exit("Success")
