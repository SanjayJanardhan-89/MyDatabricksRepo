-- Databricks notebook source
-- MAGIC %md ## Managed Demo

-- COMMAND ----------

CREATE TABLE ManagedDemo
(
  Name STRING
,AGE INT
);

INSERT INTO ManagedDemo values
('Sanjay',10),
('Jan',220);


-- COMMAND ----------

DESCRIBE EXTENDED ManagedDemO

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/manageddemo'

-- COMMAND ----------

DROP TABLE manageddemo

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/manageddemo'

-- COMMAND ----------

DROP TABLE manageddemo

-- COMMAND ----------

-- MAGIC %md ## Exteral Demo

-- COMMAND ----------

CREATE TABLE ExternalDemo
(
  Name STRING
,AGE INT
)
LOCATION 'dbfs:/mnt/demo/ExternalDemo'
;

INSERT INTO ExternalDemo values
('Sanjay',10),
('Jan',220);


-- COMMAND ----------

DESCRIBE EXTENDED ExternalDemo

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/ExternalDemo'

-- COMMAND ----------

DROP TABLE ExternalDemo

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/ExternalDemo'

-- COMMAND ----------

-- MAGIC %md ## New Schema Demo

-- COMMAND ----------

CREATE DATABASE new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE ND_ManagedDemo
(
  Name STRING
,AGE INT
);

INSERT INTO ND_ManagedDemo values
('Sanjay',10),
('Jan',220);


CREATE TABLE ND_ExternalDemo
(
  Name STRING
,AGE INT
)
LOCATION 'dbfs:/mnt/demo/ND_ExternalDemo'
;

INSERT INTO ND_ExternalDemo values
('Sanjay',10),
('Jan',220);

-- COMMAND ----------

DESCRIBE EXTENDED ND_ManagedDemo

-- COMMAND ----------

DESCRIBE EXTENDED ND_ExternalDemo

-- COMMAND ----------

DROP TABLE ND_ManagedDemo;
DROP TABLE ND_ExternalDemo;

--Its case sensitive while using the fs command

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/ND_ExternalDemo'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/nd_manageddemo'

-- COMMAND ----------


