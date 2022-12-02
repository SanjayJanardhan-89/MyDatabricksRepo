# Databricks notebook source
# MAGIC %md # PySpark

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

(DeltaTable.create(spark)
 .tableName("delta_demo_2")
 .addColumn("emp_id", "INT")
 .addColumn("emp_name", "STRING")
 .addColumn("gender", "STRING")
 .addColumn("salary", "INT")
 .addColumn("dept", "STRING")
 .property("description","Demo table")
 .location("/FileStore/tables/delta/arch_demo2")
 .execute() 
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists delta_demo_2
# MAGIC select * from delta_demo_2

# COMMAND ----------

(DeltaTable.createIfNotExists(spark)
 .tableName("delta_demo_2")
 .addColumn("emp_id", "INT")
 .addColumn("emp_name", "STRING")
 .addColumn("gender", "STRING")
 .addColumn("salary", "INT")
 .addColumn("dept", "STRING")
 .property("description","Demo table")
 .location("/FileStore/tables/delta/arch_demo2")
 .execute() 
)

# COMMAND ----------

(DeltaTable.createOrReplace(spark)
 .tableName("default.delta_demo_2")
 .addColumn("emp_id", "INT")
 .addColumn("emp_name", "STRING")
 .addColumn("gender", "STRING")
 .addColumn("salary", "INT")
 .addColumn("dept", "STRING")
 .property("description","Demo table")
 .location("/FileStore/tables/delta/arch_demo2")
 .execute() 
)

# COMMAND ----------

# MAGIC %md # SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE delta_demo_2(
# MAGIC  emp_id INT
# MAGIC ,emp_name STRING
# MAGIC ,gender STRING
# MAGIC ,salary INT
# MAGIC ,dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS delta_demo_2(
# MAGIC  emp_id INT
# MAGIC ,emp_name STRING
# MAGIC ,gender STRING
# MAGIC ,salary INT
# MAGIC ,dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE or replace TABLE delta_demo_2(
# MAGIC  emp_id INT
# MAGIC ,emp_name STRING
# MAGIC ,gender STRING
# MAGIC ,salary INT
# MAGIC ,dept STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION "/FileStore/tables/delta/arch_demo2"

# COMMAND ----------

# MAGIC %md # DataFrame

# COMMAND ----------

employee_data=[(100,'Sajay','M',2000,'IT')
               ,(200,'Janadhan','M',500,'tt')
              ]
employee_schema=["emp_id",'emp_name','gender','salary','dep']

df=spark.createDataFrame(data=employee_data, schema=employee_schema)
display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("employee_demo1")

# COMMAND ----------

display(spark.read.format('delta').load('/user/hive/warehouse/employee_demo1'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo1

# COMMAND ----------


