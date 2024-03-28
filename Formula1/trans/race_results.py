# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = (spark.read.parquet(f"{processed_folder_path}/drivers")
              .withColumnRenamed("number", "driver_number")
              .withColumnRenamed("name", "driver_name")
              .withColumnRenamed("nationality", "driver_nationality")
)

# COMMAND ----------

constructors_df = (spark.read.parquet(f"{processed_folder_path}/constructors")
                   .withColumnRenamed("name", "team")
                   )


# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = (spark.read.parquet(f"{processed_folder_path}/races")
            .withColumnRenamed("name", "race_name")
            .withColumnRenamed("race_timestamp", "race_date")
)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_df = (results_df
                   .join(races_df, races_df.race_id == results_df.race_id, "inner")
                   .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")
                   .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")
                   .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner")
                   .select(races_df.race_year
                           ,races_df.race_name
                           ,races_df.race_date
                           ,circuits_df.circuit_location
                           ,drivers_df.driver_name
                           ,drivers_df.driver_number
                           ,drivers_df.driver_nationality
                           ,constructors_df.team
                           ,results_df.grid
                           ,results_df.fastest_lap
                           ,results_df.race_time
                           ,results_df.points)
                   .withColumn("ingestion_date", current_timestamp())
                   )

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

display(race_results_df.filter((race_results_df["race_year"] == 2020) & (race_results_df["race_name"] == 'Abu Dhabi Grand Prix')).orderBy(results_df.points.desc()))

# COMMAND ----------

race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


