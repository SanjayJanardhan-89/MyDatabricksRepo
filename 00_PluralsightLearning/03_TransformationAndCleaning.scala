// Databricks notebook source
var yellowTaxiTripDataDF=spark
  .read
  .option("header","true")
  .option("inferSchema","true")
  .csv("dbfs:/mnt/datalake/taxisource/YellowTaxiTripData_201812.csv")

// COMMAND ----------

display(yellowTaxiTripDataDF)

// COMMAND ----------

display(yellowTaxiTripDataDF.describe("passenger_count","trip_distance"))

// COMMAND ----------

// MAGIC %md ##Filter
// MAGIC - Filter and where are same, we can use them interchangebaly

// COMMAND ----------

println("before count :"+yellowTaxiTripDataDF.count())

//Filter the data
yellowTaxiTripDataDF=   yellowTaxiTripDataDF
                      .where("trip_distance>0")
                

println("After count :"+yellowTaxiTripDataDF.count())

// COMMAND ----------

println("before count :"+yellowTaxiTripDataDF.count())

//Filter the data
yellowTaxiTripDataDF=   yellowTaxiTripDataDF
                      .where("passenger_count>0")
                      .filter("trip_distance>0.0")

println("After count :"+yellowTaxiTripDataDF.count())

// COMMAND ----------

// MAGIC %md ## Check Dat Competeness 
// MAGIC - DataFrameNaFunctions and it can be used using "dataframe".na

// COMMAND ----------

println("before count :"+yellowTaxiTripDataDF.count())

//Remove when the pickup or drop location is null
yellowTaxiTripDataDF=   yellowTaxiTripDataDF
                      .na
                      .drop(Seq("PULocationId","DOLocationID"))

println("After count :"+yellowTaxiTripDataDF.count())

// COMMAND ----------

// MAGIC %md ##Fill missing data
// MAGIC - Replace NULL values with default values

// COMMAND ----------

var defMapping = Map("payment_type"->5, "RatecodeID"->1)

yellowTaxiTripDataDF=yellowTaxiTripDataDF.na.fill(defMapping)



// COMMAND ----------

// MAGIC %md ##Remove duplication
// MAGIC - If no columns specified then complete row will be checked
// MAGIC - If columns specified, then column list will be used

// COMMAND ----------

println("before count :"+yellowTaxiTripDataDF.count())

//Remove when the pickup or drop location is null
yellowTaxiTripDataDF=   yellowTaxiTripDataDF.dropDuplicates()

println("After count :"+yellowTaxiTripDataDF.count())

// COMMAND ----------

// MAGIC %md ##Remove records which are not in Range

// COMMAND ----------

println("before count :"+yellowTaxiTripDataDF.count())

//Remove when the pickup or drop location is null
yellowTaxiTripDataDF=   yellowTaxiTripDataDF.filter("tpep_pickup_datetime >= '2018-12-01' and tpep_dropoff_datetime < '2019-01-01'")

println("After count :"+yellowTaxiTripDataDF.count())

// COMMAND ----------

// MAGIC %md ## We can chain all these operation together
// MAGIC - Above example all are tranformation operations
// MAGIC - They will be applied only when there is action
// MAGIC - It does no execute sequentially
// MAGIC - Spark SQL will optimise the execution plan for the better performance

// COMMAND ----------

var defMapping = Map("payment_type"->5, "RatecodeID"->1)

println("before count :"+yellowTaxiTripDataDF.count())

//Filter the data
yellowTaxiTripDataDF=   yellowTaxiTripDataDF
                      .where("trip_distance>0")
                      .filter("trip_distance>0.0")
                      .na.drop(Seq("PULocationId","DOLocationID"))             
                      .na.fill(defMapping)
                      .dropDuplicates()
                      .filter("tpep_pickup_datetime >= '2018-12-01' and tpep_dropoff_datetime < '2019-01-01'")

println("After count :"+yellowTaxiTripDataDF.count())



// COMMAND ----------


