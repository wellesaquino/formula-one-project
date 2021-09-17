# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('stop', StringType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('duration', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/pit_stops.json", schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns, add ingestion date and drop statusId

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                                 .withColumnRenamed('driverId', 'driver_id') \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
