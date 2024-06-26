# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date",StringType(), True),
                                    StructField("time",StringType(), True),
                                    StructField("url",StringType(), True)
                                    ])

# COMMAND ----------

races_df = (spark.read
           .option("header", True)
           .schema(races_schema)
           .csv("/mnt/formula1dl/raw/races.csv")
)
display(races_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat, current_timestamp

# COMMAND ----------

races_df = races_df.select(col("raceId").alias("race_id")
                           , col("year").alias("race_year")
                           , col("round")
                           , col("circuitId").alias("circuit_id")
                           , col("name")
                           , col("date")
                           , col("time"))

# COMMAND ----------

races_df = (races_df
            .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))
            .drop(col("date"),col("time"))
            .withColumn("data_source", lit(v_data_source))
            )
               

# COMMAND ----------

races_df = add_ingestion_date(races_df)

# COMMAND ----------

display(races_df.limit(5))

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy("race_year").format('parquet').saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl/processed/races/race_year=2021"))

# COMMAND ----------

dbutils.notebook.exit("Success")
