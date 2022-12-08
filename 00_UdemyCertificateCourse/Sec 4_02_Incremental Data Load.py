# Databricks notebook source
# MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(
    spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","parquet")
            .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/orders_checkpoint")
            .load(f"{dataset_bookstore}/orders-raw")
        .writeStream
            .option('checkpointLocation',"dbfs:/mnt/demo/orders_checkpoint")
            .table("orders_upadtes")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from orders_upadtes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from orders_upadtes

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from orders_upadtes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY orders_upadtes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table orders_upadtes

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint",True)

# COMMAND ----------


