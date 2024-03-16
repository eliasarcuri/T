# Databricks notebook source
# DBTITLE 1,Conferindo arquivos das pastas
# MAGIC %fs ls dbfs:/mnt/financeiro/bronze/

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo
df_recebimento = spark.read.parquet("dbfs:/mnt/financeiro/bronze/data_69e5bcf1-fe68-44ec-907c-10cb65a5b19f_ad35162a-9ea3-41a5-972f-d723e846d4d9.parquet")
display(df_recebimento)
#dbfs:/mnt/financeiro/bronze/RECEBIMENTO.parquet

# COMMAND ----------

# DBTITLE 1,Print (visualização das colunas para seleção)
print(df_recebimento.columns)

# COMMAND ----------

# DBTITLE 1,Tratando os nulos com for e fillna
colunas = ['NUMERO_NF', 'DATA_RECEBIMENTO', 'DATA_RECEBIMENTO_EFETUADO', 'VALOR_PARCELA', 'VALOR_PARCELA_RECEBIDA', 'NUM_PARCELA', 'STATUS', 'DataCarga']

for trocanulos in colunas:
    df_recebimento = df_recebimento\
        .fillna('Sem dados', subset=[trocanulos])
display(df_recebimento)

# COMMAND ----------

# DBTITLE 1,Consultando esquema
df_recebimento.printSchema()

# COMMAND ----------

# DBTITLE 1,Convertendo tipo de dados string para int
colunasint = ['NUMERO_NF', 'VALOR_PARCELA_RECEBIDA']

for csi in colunasint:
    df_recebimento = df_recebimento\
        .withColumn(csi,df_recebimento[csi].cast("int"))
display(df_recebimento)

# COMMAND ----------

# DBTITLE 1,Conferindo esquema alterado
df_recebimento.printSchema()

# COMMAND ----------

# DBTITLE 1,Salvar na camada Silver criando a pasta e o nome do arquivo
df_recebimento.write.mode("overwrite").parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet")

# COMMAND ----------

# DBTITLE 1,Ver arquivo na camada Silver
# MAGIC %fs ls dbfs:/mnt/financeiro/Silver

# COMMAND ----------

# DBTITLE 1,Lendo arquivo parquet na Silver
display(spark.read.parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet/"))
