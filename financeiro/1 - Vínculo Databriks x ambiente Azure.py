# Databricks notebook source
# DBTITLE 1,Criar uma pasta no Databriks para o vínculo
# MAGIC %fs mkdirs /mnt/financeiro

# COMMAND ----------

# DBTITLE 1,Configuração
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "c40b6ecf-8a55-4dce-bc87-b7e1c95fca6c",
          "fs.azure.account.oauth2.client.secret":"8mc8Q~WL1DjoZE415z64YPBICtWvFDIMAPsiEbTT",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3044fcb5-2d35-4636-9770-6c0604cbd639/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://financeiro@datalakearcuria.dfs.core.windows.net/",
  mount_point = "/mnt/financeiro",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Conferindo pastas criadas
# MAGIC %fs ls /mnt/financeiro

# COMMAND ----------

# DBTITLE 1,Conferindo arquivos das pastas
# MAGIC %fs ls dbfs:/mnt/financeiro/Bronze/
