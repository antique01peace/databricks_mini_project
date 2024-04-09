-- Databricks notebook source
create database if not exists f1_processed
LOCATION "/mnt/formula1dl/processed";

-- COMMAND ----------

desc database f1_raw;

-- COMMAND ----------

desc database f1_processed;

-- COMMAND ----------

select * from f1_processed.races

-- COMMAND ----------


