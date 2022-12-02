-- Databricks notebook source
CREATE TABLE IF NOT EXISTS Employee
(
  Id INT
, Name STRING
);

INSERT INTO Employee VALUES
  (1,'Sanjay')
, (2,'Janardhan')
, (3, 'Kythanahalli')

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE VIEW EmpView
AS
  SELECT * FROM Employee WHERE Id=1

-- COMMAND ----------

SELECT * FROM EmpView

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW EmpTempView
AS
  SELECT * FROM Employee WHERE Id=2

-- COMMAND ----------

SELECT * FROM EmpTempView

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW EmpGlobalTempView
AS
  SELECT * FROM Employee WHERE Id=3;
  
--Global temp view will not be listed in Show tables, to view it use SHOW TABLES IN global_temp
SHOW TABLES

-- COMMAND ----------

select * from EmpGlobalTempView

-- COMMAND ----------

-- MAGIC %md ## Need to add the schema name as global_temp

-- COMMAND ----------

select * from GLobal_temp.EmpGlobalTempView

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

select * from global_temp.empglobaltempview

-- COMMAND ----------


