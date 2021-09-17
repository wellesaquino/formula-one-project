# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
  return input_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

def spark_partition_overwrite_mode(mode = "dynamic"):
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)

# COMMAND ----------

def get_all_fields_and_put_partition_to_last_field(input_df, partition_field):
  new_fields = []
  for field in input_df.schema.names:
    if (field != partition_field):
      new_fields.append(field)
  new_fields.append(partition_field)
  return input_df.select(new_fields)
      

# COMMAND ----------

def df_write_with_append(df, database_name, table_name, partition_name):
  #Change the overwrite mode
  spark_partition_overwrite_mode("dynamic")
  #get all fields and move the partition field to last position
  df = get_all_fields_and_put_partition_to_last_field(df, partition_name)
  if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
    df.write.mode("overwrite").insertInto(f"{database_name}.{table_name}")
  else:
    df.write.mode("overwrite").partitionBy(partition_name).format("parquet").saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, database_name, table_name, partition_name, folder_path, merge_condition):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
  
  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
          input_df.alias("src"),
          merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_name).format("delta").saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list;

# COMMAND ----------


