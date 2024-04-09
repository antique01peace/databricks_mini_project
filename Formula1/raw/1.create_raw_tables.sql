-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitRef string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)
using csv
options (path "/mnt/formula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date string,
  time string,
  url string
)
using csv 
options (path "/mnt/formula1dl/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
using json
options(path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number int,
  code string,
  name struct<forename: string, surname: string>,
  dob date,
  nationality string,
  url string
)
using json
options (path "/mnt/formula1dl/raw/drivers.json")


-- COMMAND ----------

select count(*) from f1_raw.drivers;

-- COMMAND ----------

show tables in f1_raw;

-- COMMAND ----------

desc extended f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField
-- MAGIC
-- MAGIC name_schema = StructType(fields = [
-- MAGIC     StructField('forename', StringType(), True),
-- MAGIC     StructField('surname', StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC driver_schema = StructType(fields=[
-- MAGIC     StructField('driverId', IntegerType(), False),
-- MAGIC     StructField('driverRef', StringType(), True),
-- MAGIC     StructField('number', IntegerType(), True),
-- MAGIC     StructField('code', StringType(), True),
-- MAGIC     StructField('name', name_schema),
-- MAGIC     StructField('dob', DateType(), True),
-- MAGIC     StructField('nationality', StringType(), True),
-- MAGIC     StructField('url', StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC drivers_df = spark.read.schema(driver_schema).json('/mnt/formula1dl/raw/drivers.json')
-- MAGIC
-- MAGIC drivers_df.createOrReplaceTempView("drivers")

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers
as
select * from drivers;

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

desc extended f1_raw.drivers;

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed float,
  statusId string
)
using json
options(path "/mnt/formula1dl/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results where constructorId is not null;

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time string
)
using json
options(path "/mnt/formula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options(path "/mnt/formula1dl/raw/lap_times", header true)

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string
)
using json
options(path "/mnt/formula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.drivers;

-- COMMAND ----------


