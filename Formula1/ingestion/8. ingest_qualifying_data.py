# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), False),
    StructField("q2", StringType(), False),
    StructField("q3", StringType(), False)
])

# COMMAND ----------

qualifying_df = (spark.read
                 .schema(qualifying_schema)
                 .option("multiLine", True)
                 .json("/mnt/formula1dl/raw/qualifying/*.json")
)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_df = (qualifying_df
                 .withColumnRenamed("qualifyId", "qualify_id")
                 .withColumnRenamed("raceId", "race_id")
                 .withColumnRenamed("driverId", "driver_id")
                 .withColumnRenamed("constructorId", "constructor_id")
                 .withColumn("data_source", lit(v_data_source))
)


# COMMAND ----------

qualifying_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
