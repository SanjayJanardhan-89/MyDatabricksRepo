-- Databricks notebook source
-- MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md ##First method to overwrite the data

-- COMMAND ----------

CREATE OR REPLACE TABLE Orders 
AS
  SELECT * FROM parquet.`${dataset.bookstore}/orders`;
  
SELECT * FROM Orders;

-- COMMAND ----------

DESCRIBE HISTORY ORDERS;

-- COMMAND ----------

SELECT COUNT(1) FROM Orders

-- COMMAND ----------

-- MAGIC %md ##Second method to overwrite the data
-- MAGIC - Overwrite can replace the data only in existing table

-- COMMAND ----------

INSERT OVERWRITE Orders
  SELECT * FROM parquet.`${dataset.bookstore}/orders`;


-- COMMAND ----------

DESCRIBE HISTORY ORDERS;

-- COMMAND ----------

INSERT OVERWRITE Orders
  SELECT *,current_timestamp() FROM parquet.`${dataset.bookstore}/orders`;


-- COMMAND ----------

-- MAGIC %md ##Append Records to table
-- MAGIC - It may insert duplicate records
-- MAGIC - Re executing uery will insert records again

-- COMMAND ----------

INSERT INTO Orders
  SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT COUNT(1) FROM Orders

-- COMMAND ----------

DESCRIBE HISTORY ORDERS;

-- COMMAND ----------

-- MAGIC %md ##Upsert data
-- MAGIC - To avoid duplicates

-- COMMAND ----------

CREATE OR REPLACE VIEW Customer_Updates
AS
  SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;
  
  
MERGE INTO Customers C
USING Customer_Updates u
ON C.customer_id = u.customer_id
WHEN MATCHED AND C.email IS NULL and u.email IS NOT NULL THEN 
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW book_updates
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS(
  path="${dataset.bookstore}/books-csv-new",
  header="true",
  delimiter=";"
);

SELECT * FROM book_updates;

-- COMMAND ----------

MERGE INTO bookS b
USING book_updates U
on B.book_id = U.book_id AND B.TITLE = U.TITLE
WHEN NOT MATCHED  AND U.CATEGORY='Computer Science' THEN
  INSERT *;

-- COMMAND ----------


