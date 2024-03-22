# Databricks notebook source
# MAGIC %md #### Ingest data from drivers.json

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
              .withColumn('ingestion_date', current_timestamp())
              .drop(col('url'))
              )

display(drivers_df.limit(10))

# COMMAND ----------

drivers_df.write.mode('overwrite').parquet('/mnt/formula1dl/processed/drivers/')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl/processed/drivers/').limit(5))

# COMMAND ----------

