# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

f = open('/dbfs/databricks-datasets/README.md', 'r')
print(f.read())

# COMMAND ----------

print(f.read())

# COMMAND ----------

filePath = "dbfs:/databricks-datasets/SPARK_README.md"

# COMMAND ----------

lines = sc.textFile(filePath)

# COMMAND ----------

lines.take(10)

# COMMAND ----------

nPart = lines.getNumPartitions()
print(nPart)

# COMMAND ----------

words = lines.flatMap(lambda x:x.split(' '))
words.take(10)

# COMMAND ----------

stopWords = ['','a','*','and','is','of','the','a']
filteredWords = words.filter(lambda x:x.lower() not in stopWords)

# COMMAND ----------

filteredWords.take(10)

# COMMAND ----------

filteredWords.cache()

# COMMAND ----------

wTuple = filteredWords.map(lambda x:(x,1))
wTuple.take(10)

# COMMAND ----------

wTupleCount = wTuple.reduceByKey(lambda x,y:x+y)
wTupleCount.take(10)

# COMMAND ----------

#sortwTupleCount = wTupleCount.top(10,key=lambda (x,y) : y)
sortwTupleCount = wTupleCount.top(10,key=lambda (x, y): y)

for tuple in sortwTupleCount:
    print str(tuple)

# COMMAND ----------

import pandas as pd

df = pd.DataFrame({'a':[1,1,2,2], 'b':[1,2,3,4]})
df.iloc[0]

# COMMAND ----------

from pyspark.sql import *

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

# COMMAND ----------

import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession

# COMMAND ----------

s = ps.Series([1,3,4,5])

# COMMAND ----------

s

# COMMAND ----------

textFile = sc.textFile("/databricks-datasets/samples/docs/README.md")

# COMMAND ----------

textFile.take(10)

# COMMAND ----------

textFile.count()

# COMMAND ----------

 s = spark.range(100)

# COMMAND ----------

s.collect()

# COMMAND ----------

type(s)

# COMMAND ----------

ds=open('dbfs:/databricks-datasets/COVID/','r')
print(ds.read())

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------


