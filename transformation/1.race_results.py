# Databricks notebook source
# MAGIC %md 
# MAGIC #### Mount the presentation 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Load dataframes

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results") \
            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}/races")
circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits")
drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/drivers")
constructors_df = spark.read.format('delta').load(f"{processed_folder_path}/constructors")

# COMMAND ----------

presentation_df = results_df.join(races_df, results_df.race_id == races_df.race_id, "inner") \
                            .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
                  .select(races_df.race_id, races_df.race_year, races_df.name.alias("race_name"), races_df.race_timestamp.alias("race_date"),
                          circuits_df.location.alias("circuit_location"),
                          drivers_df.name.alias("driver_name"), drivers_df.number.alias("driver_number"), drivers_df.nationality.alias("driver_nationality"),
                          constructors_df.name.alias("team"),
                          results_df.grid, results_df.fastest_lap, results_df.time.alias("race_time"), results_df.points, results_df.position, results_df.file_date
                  ).withColumn("created_date", current_timestamp())


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(presentation_df, "f1_presentation", "race_results", "race_id", presentation_folder_path, merge_condition)
