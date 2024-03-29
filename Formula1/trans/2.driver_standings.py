# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

driver_standings_df = race_results_df\
    .groupBy("race_year", "driver_name", "driver_nationality", "team")\
    .agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

rank_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

driver_standings_df = driver_standings_df\
    .withColumn("rank", rank().over(rank_window_spec))

# COMMAND ----------

display(driver_standings_df.filter("race_year == 2011"))

# COMMAND ----------

driver_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------


