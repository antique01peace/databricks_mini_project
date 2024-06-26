# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructField, StructType, FloatType

# COMMAND ----------

result_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', FloatType(), True),
    StructField('statusId', StringType(), True)
])

# COMMAND ----------

results_df = (spark.read
              .schema(result_schema)
              .json('/mnt/formula1dl/raw/results.json'))


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

results_df = (results_df.withColumnRenamed('resultId','result_id')
              .withColumnRenamed('raceid','race_id')
              .withColumnRenamed('driverId', 'driver_id')
              .withColumnRenamed('constructorId', 'constructor_id')
              .withColumnRenamed('positionText', 'position_text')
              .withColumnRenamed('positionOrder','position_order')
              .withColumnRenamed('fastestLap', 'fastest_lap')
              .withColumnRenamed('fastestLapTime', 'fastest_lap_time')
              .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')
              .withColumn("data_source", lit(v_data_source))
              .drop('StatusId'))

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

results_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl/processed/results'))

# COMMAND ----------

dbutils.notebook.exit("Success")
