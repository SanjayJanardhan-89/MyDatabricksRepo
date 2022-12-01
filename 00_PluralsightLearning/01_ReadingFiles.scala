// Databricks notebook source
// MAGIC %scala
// MAGIC 
// MAGIC display(dbutils.fs.ls("/mnt/datalake/taxisource/"))

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.head("dbfs:/mnt/datalake/taxisource/TaxiZones.csv")

// COMMAND ----------

// MAGIC %fs ls "/mnt/datalake/taxisource/"

// COMMAND ----------

// MAGIC %fs head "dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv"

// COMMAND ----------

var df=spark
  .read
  .csv("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")

display(df)

//var df2=spark.read.format("csv").load("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")
//display(df2)

// COMMAND ----------

var YellowDF=spark
  .read
  .option("header","true")
  .csv("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")

display(df)

//var df2=spark.read.format("csv").load("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")
//display(df2)

// COMMAND ----------

var GreenDF = spark
  .read
  .option("header","true")
  .option("delimeter",'\t')
 .format("csv")
.load("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")

// COMMAND ----------

display(GreenDF)

// COMMAND ----------

// MAGIC %fs ls '/mnt/datalake/taxisource'

// COMMAND ----------

// MAGIC %fs head 'dbfs:/mnt/datalake/taxisource/PaymentTypes.json'

// COMMAND ----------

var PaymentDF = spark.read.format("json").load("dbfs:/mnt/datalake/taxisource/PaymentTypes.json")
display(PaymentDF)

// COMMAND ----------


