-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md ###Explore Managed Tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

desc catalog extended spark_catalog

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet(f"{presentation_folder_path}/race_results").write.saveAsTable("race_results_python")
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls "/user/hive/warehouse"

-- COMMAND ----------

-- MAGIC %fs ls "/user/hive/warehouse/demo.db"

-- COMMAND ----------

use default;
show tables;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

desc extended demo.race_results_python

-- COMMAND ----------

create table race_results_sql
as
select * 
  from demo.race_results_python

-- COMMAND ----------

select * from race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "/user/hive/warehouse/demo.db"

-- COMMAND ----------

create table race_results_2020
as
select * 
  from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

desc extended demo.race_results_2020;

-- COMMAND ----------

drop table demo.race_results_2020;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/demo.db"))

-- COMMAND ----------

-- MAGIC %md ###EXTERNAL TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/formula1dl/presentation/race_results_ext_sql"))

-- COMMAND ----------

select * from demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.printSchema()

-- COMMAND ----------

create table race_results_ext_sql
(
  race_year int,
  race_name string, 
  race_date timestamp,
  circuit_location string, 
  driver_name string,
  driver_number integer,
  driver_nationality string,
  team string,
  grid integer,
  fastest_lap integer, 
  race_time string,
  points float, 
  position integer,
  ingestion_date timestamp
)
using parquet
location "/mnt/formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

desc extended race_results_ext_sql;

-- COMMAND ----------

insert into demo.race_results_ext_sql
 select * from demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####VIEWS
-- MAGIC 1. Temp Views
-- MAGIC 2. Global Temp View
-- MAGIC 3. Permanent View

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * 
  from race_results_python
  where race_year = 2020

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
  select * from race_results_python
  where race_year = 2018

-- COMMAND ----------

show databases;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------


