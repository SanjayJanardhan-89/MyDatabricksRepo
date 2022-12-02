# Databricks notebook source
import pandas as pd


data=[[1,"Sanjay"],[2,"Janardhan"],[3,"Kythanahalli"]]
pdf = pd.DataFrame(data, columns=["id","name"])

df1=spark.createDataFrame(pdf)
df2=spark.createDataFrame(data, schema="id LONG, name STRING")

# COMMAND ----------


