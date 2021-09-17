# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run  "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_schema = StructType(fields=[StructField(name="circuitId", dataType=IntegerType(), nullable=False),
                                     StructField(name="circuitRef", dataType=StringType(), nullable=True),
                                     StructField(name="name", dataType=StringType(), nullable=True),
                                     StructField(name="loaction", dataType=StringType(), nullable=True),
                                     StructField(name="country", dataType=StringType(), nullable=True),
                                     StructField(name="lat", dataType=DoubleType(), nullable=True),
                                     StructField(name="lng", dataType=DoubleType(), nullable=True),
                                     StructField(name="alt", dataType=DoubleType(), nullable=True),
                                     StructField(name="url", dataType=StringType(), nullable=True)
])

# COMMAND ----------

circuits_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/circuits.csv", schema=circuits_schema, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","loaction","country","lat","lng","alt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_rename_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                         .withColumnRenamed("circuitRef", "circuit_ref") \
                                         .withColumnRenamed("loaction", "location") \
                                         .withColumnRenamed("lat", "latitude") \
                                         .withColumnRenamed("lng", "longitude") \
                                         .withColumnRenamed("alt", "altitude") \
                                         .withColumn("data_source", lit(v_data_source)) \
                                         .withColumn("file_date", lit(v_file_date))
        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
