-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Processed database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed 
LOCATION "/mnt/formula1sa/processed"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Presentation database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1sa/presentation"

-- COMMAND ----------


