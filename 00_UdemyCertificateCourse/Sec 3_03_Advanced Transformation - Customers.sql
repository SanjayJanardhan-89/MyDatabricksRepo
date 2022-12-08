-- Databricks notebook source
-- MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

DESCRIBE CUSTOMERS

-- COMMAND ----------

--Built in support for JSON datatype

SELECT
  customer_id
,profile:first_name
,profile:address:country
FROM
  Customers

-- COMMAND ----------

SELECT from_json(profile) as ProfileStruct FROM Customers;

-- COMMAND ----------

SELECT profile FROM Customers

-- COMMAND ----------

SELECT from_json(profile, schema_of_json('{"first_name":"Gregoor","last_name":"Lenard","gender":"Male","address":{"street":"0 Superior Park","city":"Trelleborg","country":"Sweden"}}')) as ProfileStruct 
FROM Customers;

-- COMMAND ----------

CREATE OR REPLACE VIEW ParsedCustomer 
AS
  SELECT 
    customer_id
  , from_json(profile, schema_of_json('{"first_name":"Gregoor","last_name":"Lenard","gender":"Male","address":{"street":"0 Superior Park","city":"Trelleborg","country":"Sweden"}}')) as ProfileStruct 
FROM Customers;

SELECT * FROM ParsedCustomer

-- COMMAND ----------

DESCRIBE ParsedCustomer

-- COMMAND ----------

SELECT 
  ProfileStruct.address.country
, ProfileStruct.first_name
FROM
  ParsedCustomer

-- COMMAND ----------

CREATE OR REPLACE VIEW ParsedCustomer_Final
AS
  SELECT 
    customer_id
  , ProfileStruct.* 
FROM ParsedCustomer;

SELECT * FROM ParsedCustomer_Final

-- COMMAND ----------


