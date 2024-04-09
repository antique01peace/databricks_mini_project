# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

constructor_standings_df = race_results_df\
    .groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

rank_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

constructor_standings_df = constructor_standings_df\
    .withColumn("rank", rank().over(rank_window_spec))

# COMMAND ----------

display(constructor_standings_df.filter("race_year == 2020"))

# COMMAND ----------

constructor_standings_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.construction_standings")

# COMMAND ----------


