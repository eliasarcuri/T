# Databricks notebook source
# DBTITLE 1,Conferindo arquivos das pastas
# MAGIC %fs ls dbfs:/mnt/financeiro/Bronze/

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo
df_recebimento = spark.read.parquet("dbfs:/mnt/financeiro/Bronze/RECEBIMENTO.parquet")
display(df_recebimento)

# COMMAND ----------

# DBTITLE 1,Print (visualização das colunas para seleção)
print(df_recebimento.columns)

# COMMAND ----------

# DBTITLE 1,Tratando os nulos com for e fillna
colunas = ['NUMERO_NF', 'VALOR_PARCELA_RECEBIDA', 'DATA_RECEBIMENTO', 'DATA_RECEBIMENTO_EFETUADO', 'DATA_PROCESSAMENTO']

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

# DBTITLE 1,Salvar na camada Silver
df_recebimento.write.mode("overwrite").parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet")

# COMMAND ----------

# DBTITLE 1,Ver arquivo na camada Silver
# MAGIC %fs ls dbfs:/mnt/financeiro/Silver

# COMMAND ----------

# DBTITLE 1,Lendo arquivo parquet na Silver
display(spark.read.parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet/"))
