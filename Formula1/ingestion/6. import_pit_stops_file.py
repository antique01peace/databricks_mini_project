# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stop_df = spark.read.schema(pit_stop_schema).json('/mnt/formula1dl/raw/pit_stops.json')

# COMMAND ----------

display(pit_stop_df.limit(5))

# COMMAND ----------

pit_stop_df = (spark.read
               .option('multiLine', True)
               .schema(pit_stop_schema)
               .json('/mnt/formula1dl/raw/pit_stops.json'))

# COMMAND ----------

display(pit_stop_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stop_df = (pit_stop_df
               .withColumnRenamed('driverId', 'driver_id')
               .withColumnRenamed('raceId', 'race_id')
               .withColumn('ingestion_date', current_timestamp()))

# COMMAND ----------

pit_stop_df.write.mode('overwrite').parquet('/mnt/formula1dl/processed/pit_stops')

# COMMAND ----------

