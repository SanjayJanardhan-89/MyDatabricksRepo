# Databricks notebook source
df = spark.read\
  .format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

import pandas as pd

data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]

pdf = pd.DataFrame(data, columns=["id", "name"])

df1 = spark.createDataFrame(pdf)
df2 = spark.createDataFrame(data, schema="id LONG, name STRING")

# COMMAND ----------

joined_df = df1.join(df2, how="inner", on="id")

# COMMAND ----------

df.write.format("json").save("/tmp/json_data")

# COMMAND ----------

df.write.saveAsTable("my_table")

# COMMAND ----------

table_name = "my_table"

query_df = spark.sql(f"SELECT * FROM {table_name}")

# COMMAND ----------


