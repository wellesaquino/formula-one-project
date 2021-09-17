# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying/*.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the Json file using the spark dataframe reader

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/qualifying", schema=qualifying_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns, add ingestion date and drop statusId

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
                                   .withColumnRenamed('raceId', 'race_id') \
                                   .withColumnRenamed('driverId', 'driver_id') \
                                   .withColumnRenamed('constructorId', 'constructor_id') \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
