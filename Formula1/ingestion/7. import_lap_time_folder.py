# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

lap_time_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

lap_times_df = (spark.read
                .schema(lap_time_schema)
                .csv('/mnt/formula1dl/raw/lap_times'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_df = (lap_times_df
    .withColumnRenamed('raceId', 'race_id')
    .withColumnRenamed('driverId', 'driver_id')
    .withColumn('data_source', lit(v_data_source))
)

# COMMAND ----------

lap_times_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl/processed/lap_times'))

# COMMAND ----------

dbutils.notebook.exit("Success")
