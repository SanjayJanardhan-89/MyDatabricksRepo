-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/dlt/demo-datasets/bokstore/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/dlt/demo-datasets/bokstore/system/events/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select * from delta.`dbfs:/mnt/dlt/demo-datasets/bokstore/system/events/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/dlt/demo-datasets/bokstore/tables/")
-- MAGIC display(files)

-- COMMAND ----------

USE demo_bookstore_dlt_db;
SELECT * FROM customers

-- COMMAND ----------


