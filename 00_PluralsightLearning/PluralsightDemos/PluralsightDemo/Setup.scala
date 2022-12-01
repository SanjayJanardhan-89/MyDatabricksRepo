// Databricks notebook source
val configs = Map(
  "fs.adl.oauth2.access.token.provider.type" -> "ClientCredential",
  "fs.adl.oauth2.client.id" -> "<client id>",
  "fs.adl.oauth2.credential" -> "<client secret>",
  "fs.adl.oauth2.refresh.url" -> "https://login.microsoftonline.com/<directory id>/oauth2/token"
)

dbutils.fs.mount(
  source = "adl://<data lake name>.azuredatalakestore.net",
  mountPoint = "/mnt/datalake",
  extraConfigs = configs
)

// COMMAND ----------

val configs = Map(
  "fs.azure.account.key.<storage name>.blob.core.windows.net" -> "<access key>"
)

dbutils.fs.mount(
  source = "wasbs://<container name>@<storage name>.blob.core.windows.net/",
  mountPoint = "/mnt/storage",
  extraConfigs = configs
)

// COMMAND ----------


