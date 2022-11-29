# Databricks notebook source
from delta.tables import *

# COMMAND ----------

(DeltaTable.create(spark)
 .tableName("delta_demo")
 .addColumn("emp_id", "INT")
 .addColumn("emp_name", "STRING")
 .addColumn("gender", "STRING")
 .addColumn("salary", "INT")
 .addColumn("dept", "STRING")
 .property("description","Demo table")
 .location("/FileStore/tables/delta/arch_demo")
 .execute() 
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000000.crc

# COMMAND ----------

display(spark.read.format("delta").load("/FileStore/tables/delta/arch_demo"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into delta_demo values(200,"Sanjay","M",1000,"HR");
# MAGIC insert into delta_demo values(300,"Janardhan","M",5000,"HR");

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into delta_demo values(400,"K","M",1000,"HR");
# MAGIC insert into delta_demo values(500,"J","M",5000,"HR");
# MAGIC 
# MAGIC insert into delta_demo values(400,"K","M",1000,"HR");
# MAGIC insert into delta_demo values(500,"J","M",5000,"HR");
# MAGIC 
# MAGIC insert into delta_demo values(400,"K","M",1000,"HR");
# MAGIC insert into delta_demo values(500,"J","M",5000,"HR");

# COMMAND ----------

display(spark.read.format("delta").load("/FileStore/tables/delta/arch_demo"))

# COMMAND ----------

df = spark.read.format("delta").load("/FileStore/tables/delta/arch_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC delete from delta_demo where emp_id=200

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000011.json

# COMMAND ----------

# MAGIC %fs
# MAGIC ls  /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

df = spark.read.format("parquet").load("/FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.checkpoint.parquet")
display(df)


# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000011.json

# COMMAND ----------


