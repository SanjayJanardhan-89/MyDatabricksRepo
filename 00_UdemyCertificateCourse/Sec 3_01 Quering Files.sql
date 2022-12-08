-- Databricks notebook source
-- MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   * 
-- MAGIC FROM 
-- MAGIC   json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   * 
-- MAGIC FROM 
-- MAGIC   json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   count(1) 
-- MAGIC FROM 
-- MAGIC   json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   COUNT(1)
-- MAGIC FROM 
-- MAGIC   json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC  input_file_name() as SourceFile
-- MAGIC , *
-- MAGIC FROM 
-- MAGIC   json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT
-- MAGIC  *
-- MAGIC FROM
-- MAGIC     text.`${dataset.bookstore}/customers-json`
-- MAGIC LIMIT 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

DROP TABLE IF EXISTS book_csv;

CREATE TABLE book_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv";

SELECT * FROM book_csv;

-- COMMAND ----------

DESCRIBE EXTENDED book_csv;

-- COMMAND ----------

-- MAGIC %md ## Add some files to the directory, to try the delta table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC (spark
-- MAGIC     .read
-- MAGIC     .table("book_csv")
-- MAGIC     .write
-- MAGIC     .mode("append")
-- MAGIC     .format("csv")
-- MAGIC     .option('header','true')
-- MAGIC     .option('delimiter',';')
-- MAGIC     .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC files =dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT
  COUNT(1)
FROM
  book_csv

-- COMMAND ----------

REFRESH TABLE book_csv

-- COMMAND ----------

SELECT
  COUNT(1)
FROM
  book_csv

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   COUNT(1)
-- MAGIC FROM 
-- MAGIC   csv.`${dataset.bookstore}/books-csv/`

-- COMMAND ----------

DROP TABLE IF EXISTS Customers;

CREATE TABLE Customers
AS
  SELECT * FROM json.`${dataset.bookstore}/customers-json`;
  
DESCRIBE EXTENDED Customers;

-- COMMAND ----------

-- MAGIC %md ## CTAS will create the delta table, but does not allow to use OPTIOn

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_temp_view
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS
(
  path="${dataset.bookstore}/books-csv/export_*.csv"
,  header="true"
,delimiter=';'
);

DROP TABLE IF EXISTS books;

CREATE TABLE books
AS
  SELECT * FROM books_temp_view;
  
SELECT * FROM books;

-- COMMAND ----------

SELECT COUNT(1) FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books;

-- COMMAND ----------


