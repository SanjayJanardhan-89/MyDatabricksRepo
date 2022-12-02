# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS TaxiServiceWarehouse

# COMMAND ----------

GreenTaxiTripData_201812.write.mode(SaveMode.Overwrite).saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripManaged")

# COMMAND ----------


