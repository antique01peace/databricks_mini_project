-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet("/mnt/formula1dl/processed/circuits").write.saveAsTable("demo.circuits")
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls "/mnt/formula1dl/processed"

-- COMMAND ----------

describe demo.circuits

-- COMMAND ----------

USE default;

-- COMMAND ----------

show databases;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration" 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") 

-- COMMAND ----------

-- MAGIC %fs ls "/mnt/formula1dl"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.saveAsTable("demo.race_results")

-- COMMAND ----------

-- MAGIC %fs ls "/user/hive/warehouse/demo.db"

-- COMMAND ----------

show tables;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

desc extended demo.race_results

-- COMMAND ----------

desc extended demo.race_results_python_new

-- COMMAND ----------

drop table demo.race_results_python_new

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "dbfs:/user/hive/warehouse/demo.db/"

-- COMMAND ----------

create table race_results_2020
as
select * 
  from demo.race_results
where race_year = 2020;

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/demo.db/"

-- COMMAND ----------

desc extended demo.race_results_2020;

-- COMMAND ----------

drop table as;

-- COMMAND ----------

drop table demo.race_results_2020;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/demo.db"))

-- COMMAND ----------


