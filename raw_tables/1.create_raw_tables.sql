-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
loaction STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING)
USING csv
OPTIONS (path="/mnt/formula1sa/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date date,
time STRING,
url STRING)
USING csv
OPTIONS (path="/mnt/formula1sa/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for Json files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructor table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING)
USING json
OPTIONS(path="/mnt/formula1sa/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId INT, driverRef STRING, number INT, code STRING, name STRUCT<forename: STRING, surname: STRING>,
                                          dob DATE,
                                          nationality STRING,
                                          url STRING)
USING json
OPTIONS(path="/mnt/formula1sa/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(resultId INT,
raceId INT, 
driverId INT, 
constructorId INT, 
number INT, 
grid INT, 
position INT, 
positionText STRING, 
positionOrder INT, 
points INT, 
laps INT, 
time STRING, 
milliseconds INT, 
fastestLap INT, 
rank INT, 
fastestLapTime STRING, 
fastestLapSpeed STRING, 
statusId INT)
USING json
OPTIONS(path="/mnt/formula1sa/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT)
USING json
OPTIONS(path="/mnt/formula1sa/raw/pit_stops.json", multiLine True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create lap_times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
miliseconds INT)
USING csv
OPTIONS(path="/mnt/formula1sa/raw/lap_times")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING)
USING json
OPTIONS(path="/mnt/formula1sa/raw/qualifying", multiline true)


