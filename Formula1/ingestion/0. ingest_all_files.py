# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. ingest_races_data", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructor_data", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructor_data", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_drivers_data", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_results_data", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. import_pit_stops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------


