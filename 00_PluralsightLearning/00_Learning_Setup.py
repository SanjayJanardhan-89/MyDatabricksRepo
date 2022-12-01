# Databricks notebook source
# MAGIC %scala
# MAGIC val configs = Map(
# MAGIC   "fs.adl.oauth2.access.token.provider.type" -> "ClientCredential",
# MAGIC   "fs.adl.oauth2.client.id" -> "67252a96-8625-4e4b-8710-2f2198958263",
# MAGIC   "fs.adl.oauth2.credential" -> "yyN8Q~fODBfyTtUYdQZK4cWf6fV7Howr6x89VatK",
# MAGIC   "fs.adl.oauth2.refresh.url" -> "https://login.microsoftonline.com/5d471751-9675-428d-917b-70f44f9630b0/oauth2/token"
# MAGIC )
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "adl://sanjaydlsgen1.azuredatalakestore.net/",
# MAGIC   mountPoint = "/mnt/datalake",
# MAGIC   extraConfigs = configs
# MAGIC )

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount('/mnt/datalake')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/datalake/taxisource/'))

# COMMAND ----------

dbutils.fs.head("/mnt/datalake/taxisource/TaxiZones.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val configs = Map(
# MAGIC   "fs.azure.account.key.sanjayadbstorage2.blob.core.windows.net" -> "NqGCv5J8NWcK4hoPzz6W7qJ2OknCwcsaoKAR1WYwYOeOIyFtu27HibBtg0RRVgsRYEj2d/R25aC9+AStcEVGJA=="
# MAGIC )
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://taxisource@sanjayadbstorage2.blob.core.windows.net/",
# MAGIC   mountPoint = "/mnt/storage",
# MAGIC   extraConfigs = configs
# MAGIC )

# COMMAND ----------


