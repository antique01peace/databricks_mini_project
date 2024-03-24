# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingest circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df.printSchema)

# COMMAND ----------

circuits_df = (spark.read
               .option("header", True)
               .option("inferSchema",True)
               .csv(f"{raw_folder_path}/circuits.csv")
)
print(circuits_df.printSchema)

# COMMAND ----------

# MAGIC %md #####Step 2 - Define Schema
# MAGIC InferSchema can slow down your read. Better to pass the custom schema as a structype data or string and pass it as parameter in .option method

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng",DoubleType(), True),
                                       StructField("alt",IntegerType(), True),
                                       StructField("url",StringType(), True)
                                    ])




# COMMAND ----------

circuits_df = (spark.read
               .option("header", True)
               .schema(circuits_schema)
               .csv(f"{raw_folder_path}/circuits.csv")
            )
print(circuits_df.printSchema)

# COMMAND ----------

display(circuits_df.limit(5))

# COMMAND ----------

# MAGIC %md #####Step 3 - Selecting only the required columns

# COMMAND ----------

#1. Write column name under double quotes
circuits_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# 2. use dataframe.columnanme
circuits_df = circuits_df.select(
    circuits_df.circuitId,
    circuits_df.circuitRef,
    circuits_df.name,
    circuits_df.location,
    circuits_df.country,
    circuits_df.lat,
    circuits_df.lng,
    circuits_df.alt,
)

# COMMAND ----------

# 3. use datagrame["column_name"]
circuits_df = circuits_df.select(
    circuits_df["circuitId"],
    circuits_df["circuitRef"],
    circuits_df["name"],
    circuits_df["location"],
    circuits_df["country"],
    circuits_df["lat"],
    circuits_df["lng"],
    circuits_df["alt"],
)

# COMMAND ----------

# 4. use function col(column_name)
from pyspark.sql.functions import col

circuits_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt"),
)

# COMMAND ----------

# MAGIC %md ##### Rename the colmns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df = (
    circuits_df.withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitTRef", "circuit_ref")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("lng", "longitude")
    .withColumnRenamed("alt", "altititude")
    .withColumn("data_source", lit(v_data_source))
)

# COMMAND ----------

display(circuits_df.limit(5))

# COMMAND ----------

# MAGIC %md #####Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_df = add_ingestion_date(circuits_df)
#              .withColumn("env",lit("Production")

# COMMAND ----------

display(circuits_df.limit(5))

# COMMAND ----------

# MAGIC %md ######Write data to the data lake as parquet file

# COMMAND ----------

circuits_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs ls /mnt/formula1dl/processed/circuits

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


