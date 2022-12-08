-- Databricks notebook source
-- MAGIC %md ## Example to work with list(Arrays)

-- COMMAND ----------

-- MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

-- COMMAND ----------

SELECT order_id, customer_id,books FROM orders

-- COMMAND ----------

SELECT order_id, customer_id,EXPLODE(books)
FROM orders

-- COMMAND ----------

SELECT 
--  order_id,
 customer_id
,collect_set(order_id) as order_set
,collect_set(books.book_id) as book_set
FROM orders
group by 
 --order_id,
 customer_id

-- COMMAND ----------

select
  customer_id
, collect_set(books.book_id) as before_flatten
, array_distinct(flatten(collect_set(books.book_id)))
from
  orders
group by customer_id

-- COMMAND ----------

-- MAGIC %md ## Trying the Join Operation

-- COMMAND ----------

CREATE OR REPLACE VIEW Orders_Enriched
AS
  SELECT
    *
  FROM
    ( SELECT *,explode(books) as book FROM Orders) o
    
    INNER JOIN books B
    ON B.book_id = O.book.book_id;
    
    
select * from Orders_Enriched

-- COMMAND ----------

-- MAGIC %md ## SET Operation

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_updates
AS
  SELECT
    *
  FROM
    parquet.`${dataset.bookstore}/orders-new`;
    
select * from orders
UNION
select * from orders_updates

-- COMMAND ----------

select * from orders
INTERSECT
select * from orders_updates

-- COMMAND ----------

select * from orders
MINUS
select * from orders_updates

-- COMMAND ----------

-- MAGIC %md ## pivot Operation

-- COMMAND ----------


