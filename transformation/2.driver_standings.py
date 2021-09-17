# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver stangings

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
                  .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
                  .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df \
                      .groupBy("race_year", "driver_name", "driver_nationality") \
                      .agg(sum("points").alias("total_points"),
                           count(when(col("position") == 1, True)).alias("wins")                          
                      )

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year"). orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "driver_standings", "race_year", presentation_folder_path, merge_condition)