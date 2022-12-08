-- Databricks notebook source
-- MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

-- COMMAND ----------

SELECT * FROM Orders
--books columns in complex data type(List containing json data)

-- COMMAND ----------

-- MAGIC %md ##FILTER

-- COMMAND ----------

SELECT 
  ORDER_ID
,books
,FILTER(books, i->i.quantity >=2) as MultipleCopies
FROM 
  Orders

-- COMMAND ----------

SELECT order_id, multiplecopies
FROM
  (SELECT 
    ORDER_ID
  ,books
  ,FILTER(books, i->i.quantity >=2) as MultipleCopies
  FROM 
    Orders
  )
WHERE
  size(MultipleCopies)>0

-- COMMAND ----------

-- MAGIC %md ##TRANSFORM

-- COMMAND ----------

SELECT 
 order_id
,books
, books.subtotal
,transform(books, i->cast(i.subtotal *0.8 as int)) as SubTotalAfterDisc
FROM 
  Orders

-- COMMAND ----------

-- MAGIC %md ##User Defined Function

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN CONCAT("https://www.",split(email,'@')[1])

-- COMMAND ----------

SELECT get_url('sanjay@gmail.com')

-- COMMAND ----------

select email, get_url(email) domain
from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type(EMAIL STRING)
RETURNS STRING

RETURN
  CASE 
    WHEN email LIKE '%.com' THEN "Comm"
    when email LIKE '%.org' THEN "NP"
    WHEN email LIKE '%.edu' THEN "edu"
    ELSE CONCAT("uNKNOWN :",SPLIT(EMAIL,"@")[1])
  END;

-- COMMAND ----------

select email, get_url(email) domain, SITE_TYPE(email)
from customers

-- COMMAND ----------


