# Databricks notebook source
# DBTITLE 1,Conferindo arquivos da camada Bronze
files = dbutils.fs.ls("dbfs:/mnt/financeiro/bronze/")
sorted_files = sorted(files, key=lambda file: file.modificationTime, reverse=True)
display(sorted_files)


# COMMAND ----------

# DBTITLE 1,Ler somente o arquivo mais recente
# Listar arquivos no diretório
arquivos = dbutils.fs.ls("/mnt/financeiro/bronze/")

# Filtrar apenas arquivos Parquet
arquivos_parquet = [arquivo for arquivo in arquivos if arquivo.name.endswith(".parquet")]

# Encontrar o arquivo mais recente com base no timestamp de modificação
arquivo_mais_recente = max(arquivos_parquet, key=lambda x: x.modificationTime)

# Caminho completo do arquivo mais recente
caminho_arquivo_mais_recente = arquivo_mais_recente.path 
df_modelo2 = spark.read.parquet(caminho_arquivo_mais_recente) #df dados novos

display(df_modelo2)

# COMMAND ----------

# DBTITLE 1,Print (visualização das colunas para seleção)
#Buscando o nome das colunas para inserir no tratamento dos nulos
print(df_modelo2.columns)

# COMMAND ----------

# DBTITLE 1,Tratando os nulos com for e fillna
colunas = ['NUMERO_NF', 'DATA_RECEBIMENTO', 'DATA_RECEBIMENTO_EFETUADO', 'VALOR_PARCELA', 'VALOR_PARCELA_RECEBIDA', 'NUM_PARCELA', 'STATUS', 'DataCarga']

for trocanulos in colunas:
    df_modelo2 = df_modelo2\
        .fillna('Sem dados', subset=[trocanulos])
display(df_modelo2)

# COMMAND ----------

# DBTITLE 1,Consultando esquema
df_modelo2.printSchema()

# COMMAND ----------

# DBTITLE 1,Convertendo tipo de dado string para decimal
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

colunasdec = ['VALOR_PARCELA', 'VALOR_PARCELA_RECEBIDA']

for coluna in colunasdec:
    df_modelo2 = df_modelo2\
        .withColumn(coluna, col(coluna).cast(DecimalType(10, 2)))


# COMMAND ----------

# DBTITLE 1,Convertendo tipo de dados string para int
colunasint = ['NUMERO_NF','NUM_PARCELA']

for csi in colunasint:
    df_modelo2 = df_modelo2\
        .withColumn(csi,df_modelo2[csi].cast("int"))
display(df_modelo2)

# COMMAND ----------

# DBTITLE 1,string para data
from pyspark.sql.functions import col

# Lista de colunas a serem convertidas para tipo de data
colunasdata = ['DATA_RECEBIMENTO', 'DATA_RECEBIMENTO_EFETUADO']

# Iterar sobre as colunas e converter para tipo de data
for for_data in colunasdata:
    df_modelo2 = df_modelo2.withColumn(for_data, col(for_data).cast("Date"))

# COMMAND ----------

display(df_modelo2)

# COMMAND ----------

# DBTITLE 1,Conferindo esquema alterado
df_modelo2.printSchema()

# COMMAND ----------

# DBTITLE 1,Insert no dataframe df_novo
#Dados que já estão armazenados na Silver
df_modelo1 = spark.read.parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet/")

#Dados novos (df_modelo2) + Dados já armazenados (df_modelo1)
df_novo = df_modelo1.union(df_modelo2).orderBy('DataCarga')


# COMMAND ----------

# DBTITLE 1,Camada Silver sem os novos dados
display(df_modelo1)

# COMMAND ----------

# DBTITLE 1,Conferência do Insert
display(df_novo)

# COMMAND ----------

# DBTITLE 1,Insert na camada Silver
#Subscrevendo o arquivo na camada Silver
df_novo.write.mode("overwrite").parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet")

# COMMAND ----------

# DBTITLE 1,Ver arquivo na camada Silver
# MAGIC %fs ls dbfs:/mnt/financeiro/Silver

# COMMAND ----------

# DBTITLE 1,Lendo Informações atualizadas na Silver
df = spark.read.parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet/")
df_sorted = df.sort(df['DataCarga'].desc())

display(df_sorted)
