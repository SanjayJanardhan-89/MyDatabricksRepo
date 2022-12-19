# Databricks notebook source
# MAGIC %run ../00_Academy/data-engineer-learning-path/Includes/Classroom-Setup-04.1

# COMMAND ----------

pipeline_language = "SQL"
# pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------


