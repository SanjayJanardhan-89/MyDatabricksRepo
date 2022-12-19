# Databricks notebook source
# MAGIC %run ../00_Academy/data-engineer-learning-path/Includes/Classroom-Setup-02.1

# COMMAND ----------

# MAGIC %python
# MAGIC print(DA.paths.kafka_events)
# MAGIC 
# MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
# MAGIC display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW MyDemoView
# MAGIC AS
# MAGIC   SELECT count(1) as a 
# MAGIC   FROM json.`${DA.paths.kafka_events}`;
# MAGIC   
# MAGIC select * from mydemoview

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select * from binaryFile.`${DA.paths.kafka_events}`

# COMMAND ----------

# MAGIC %python 
# MAGIC DA.cleanup()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM csv.`${DA.paths.sales_csv}`

# COMMAND ----------


