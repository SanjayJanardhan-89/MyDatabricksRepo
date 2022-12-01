// Databricks notebook source
display(dbutils.fs.mounts())

// COMMAND ----------

// MAGIC %fs ls /mnt/storage

// COMMAND ----------

var TaxiTripDF = spark
  .read
  .option("header","true")
  .option("inferschema","true")
  .csv("dbfs:/mnt/storage/FHVTaxiTripData_201812_01.csv")
display(TaxiTripDF)

// COMMAND ----------

// MAGIC %md it tool a lot of time because it has to read the whole file. Instead of that we can create out own schema

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

var fhvTaxiTripSchema = 
  StructType(
          List(
              StructField("Pickup_DateTime", TimestampType, true),
              StructField("DropOff_datetime", TimestampType, true),
              StructField("PUlocationID", IntegerType, true),
              StructField("DOlocationID", IntegerType, true),
              StructField("SR_Flag", IntegerType, true),
              StructField("Dispatching_base_number", StringType, true),
              StructField("Dispatching_base_num", StringType, true)          
          )

    )



// COMMAND ----------

var fhvTaxiTripDF = spark.read.schema(fhvTaxiTripSchema).csv("dbfs:/mnt/storage/FHVTaxiTripData_201812_01.csv")

// COMMAND ----------

// MAGIC %md this took just few seconds to load 1.6 GB data
// MAGIC Because its no longer reading the file

// COMMAND ----------

display(fhvTaxiTripDF)

// COMMAND ----------

// MAGIC %md You can read multiple files using the wild character. So that we dont have to read the multiple files separately

// COMMAND ----------

var fhvTaxiTripDF = 
  spark
  .read
  .schema(fhvTaxiTripSchema)
  .csv("dbfs:/mnt/storage/FHVTaxiTripData_201812_*.csv")

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/storage/FhvBases.json

// COMMAND ----------

var fhvBaseDF = spark
    .read
    .format("json")
    .load("dbfs:/mnt/storage/FhvBases.json")

display(fhvBaseDF)

// COMMAND ----------

// MAGIC %md error occured as it is a multi line json. So need to specify the option

// COMMAND ----------

var fhvBaseDF = spark
    .read
    .format("json")
    .option("multiline","true")
    .load("dbfs:/mnt/storage/FhvBases.json")

display(fhvBaseDF)

// COMMAND ----------

// Create schema for FHV Bases
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val fhvBasesSchema = StructType(
  List(
    StructField("License Number", StringType, true),
    StructField("Entity Name", StringType, true),
    StructField("Telephone Number", LongType, true),
    StructField("SHL Endorsed", StringType, true),
    StructField("Type of Base", StringType, true),
    
    StructField("Address", 
                StructType(List(
                    StructField("Building", StringType, true),
                    StructField("Street", StringType, true), 
                    StructField("City", StringType, true), 
                    StructField("State", StringType, true), 
                    StructField("Postcode", StringType, true))),
                true
               ),
                
    StructField("GeoLocation", 
                StructType(List(
                    StructField("Latitude", StringType, true),
                    StructField("Longitude", StringType, true), 
                    StructField("Location", StringType, true))),
                true
              )   
  )
)

// COMMAND ----------

var fhvBaseDF2 = spark
    .read
    .schema(fhvBasesSchema)    
    .format("json")    
    .option("multiline","true")
    .load("dbfs:/mnt/storage/FhvBases.json")

display(fhvBaseDF2)

// COMMAND ----------


