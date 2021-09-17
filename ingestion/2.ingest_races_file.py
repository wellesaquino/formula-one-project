# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, to_timestamp, concat, col, lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_schema = StructType(fields=[StructField(name="race_id", dataType=IntegerType(), nullable=False),
                                  StructField(name="race_year", dataType=IntegerType(), nullable=True),
                                  StructField(name="round", dataType=IntegerType(), nullable=True),
                                  StructField(name="circuit_id", dataType=IntegerType(), nullable=True),
                                  StructField(name="name", dataType=StringType(), nullable=True),
                                  StructField(name="date", dataType=DateType(), nullable=True),
                                  StructField(name="time", dataType=StringType(), nullable=True),
                                  StructField(name="url", dataType=StringType(), nullable=True)
])

# COMMAND ----------

races_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/races.csv", schema=races_schema, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 -  Transform date and add ingestion date to the dataframe

# COMMAND ----------

races_with_dates_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                              .withColumn("data_source", lit(v_data_source)) \
                              .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_dates_df = add_ingestion_date(races_with_dates_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3- Select only required columns

# COMMAND ----------

races_selected_df = races_with_dates_df.select("race_id","race_year","round","circuit_id","name","race_timestamp", "data_source", "file_date", "ingestion_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_selected_df.write.partitionBy('race_year').mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
