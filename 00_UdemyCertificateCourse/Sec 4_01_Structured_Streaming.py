# Databricks notebook source
# MAGIC %run ../00_UdemyCertificateCourse/Copy-Datasets

# COMMAND ----------

(spark
     .readStream
     .table("books")
     .createOrReplaceTempView('books_as_streming_tmpvw')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from books_as_streming_tmpvw

# COMMAND ----------

# MAGIC %md ## Streaming query as its popinted to streaming data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   author
# MAGIC ,count(book_id) as TotalBooks
# MAGIC from
# MAGIC   books_as_streming_tmpvw
# MAGIC group by
# MAGIC   author

# COMMAND ----------

# MAGIC %md ## Sorting is not supported on streaming data
# MAGIC - Watermarking can be used

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from books_as_streming_tmpvw order by author

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW author_count_temp_view
# MAGIC AS
# MAGIC     select
# MAGIC       author
# MAGIC     ,count(book_id) as TotalBooks
# MAGIC     from
# MAGIC       books_as_streming_tmpvw
# MAGIC     group by
# MAGIC       author;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_count_temp_view

# COMMAND ----------

(
spark
    .table("author_count_temp_view")
    .writeStream
        .trigger(processingTime='4 seconds')
        .outputMode('Complete')
        .option("checkpointLocation","dbfs:/mnt/demo/author_counts_chkp")
        .table("author_counts")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC ('A','B','C','D',25),
# MAGIC ('A','B','C','D',25),
# MAGIC ('B','B','C','D',25)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC ('C','B','d','D',25),
# MAGIC ('D','B','e','D',25),
# MAGIC ('E','B','f','D',25)

# COMMAND ----------

(
spark
    .table("author_count_temp_view")
    .writeStream
        .trigger(availableNow=True)
        .outputMode('Complete')
        .option("checkpointLocation","dbfs:/mnt/demo/author_counts_chkp")
        .table("author_counts")
        .awaitTermination()
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from author_counts

# COMMAND ----------


