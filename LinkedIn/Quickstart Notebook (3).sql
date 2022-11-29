-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks Quick Start

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create and use a cluster
-- MAGIC 
-- MAGIC 1. In the sidebar, right-click the **Clusters** button and open the link in a new window.
-- MAGIC 1. On the Clusters page, click **Create Cluster**.
-- MAGIC 1. Name the cluster **demo-default**.
-- MAGIC 1. In the Databricks Runtime Version drop-down, select **4.3 (includes Apache Spark 2.3.1, Scala 11)**.
-- MAGIC 1. Click **Create Cluster**.
-- MAGIC 1. Return to this notebook. 
-- MAGIC 1. In the notebook menu bar, select **<img src="http://docs.databricks.com/_static/images/notebooks/detached.png"/></a> > demo-default**.
-- MAGIC 1. When the cluster changes from <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>, then you can run the commands in this notebook on your cluster.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Create a table from a Databricks dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create and use a cluster

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds

  USING csv
  OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manipulate the data and display the results 
-- MAGIC 
-- MAGIC 1. Selects color and price columns, averages the price, and groups and orders by color.
-- MAGIC 1. Displays a table of the results.

-- COMMAND ----------

SELECT color, avg(price) AS price 
FROM diamonds 
GROUP BY color 
ORDER BY color

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat using the Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter.  
-- MAGIC To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manipulate the data and display the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC   
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")))

-- COMMAND ----------


