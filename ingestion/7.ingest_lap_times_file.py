# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times/*.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('position', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('miliseconds', IntegerType(), True),
])

# COMMAND ----------

lap_times_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/lap_times", schema=lap_times_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns, add ingestion date and drop statusId

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
                                 .withColumnRenamed('driverId', 'driver_id') \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
