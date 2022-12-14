-- Databricks notebook source
-- MAGIC %md ## Bronze layer
-- MAGIC -streaming kyword with the LIVE indicates it as auto loader

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE Orders_raw
COMMENT "Demo raw table using streaming live"
AS
  SELECT
    *
  FROM
    cloud_files("${dataset_path}/orders-raw"
                ,"parquet"
                ,map("schema","order_id STRING, order_timestand LONG, customer_id STRING, quantity LONG"));

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE Customers
COMMENT 'Just Live table'
AS
  SELECT
    order_id
  from
    json.`${dataset_path}\customers-json`;

-- COMMAND ----------

-- MAGIC %md ## Silver layer

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE Orders_Cleaned
(
  CONSTRAINT valid_order_number EXPECT(order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bronze-Cleand orders"
AS
  SELECT
      order_id
  , quantity
  , o.customer_id
  , c.profile:first_name as f_name
  , c.profile:last_name as l_name
  , cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp
  , o.books
  ,c.profile:address:country as country    
  FROM
    STREAM(LIVE.Orders_raw) Ord
    
    LEFT JOIN LIVE.Customers Cust
    ON Ord.customer_id = Cust.customer_id

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.Orders_Cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)


-- COMMAND ----------


