# Databricks notebook source
# DBTITLE 1,Conferindo arquivos da pasta Silver
# MAGIC %fs ls dbfs:/mnt/financeiro/Silver/

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo
df_recebimento = spark.read.parquet("dbfs:/mnt/financeiro/Silver/recebimento_silver.parquet/")
display(df_recebimento)

# COMMAND ----------

# DBTITLE 1,Print (visualização das colunas para seleção)
print(df_recebimento.columns)

# COMMAND ----------

# DBTITLE 1,Inserir coluna com data do carregamento
from pyspark.sql.functions import current_timestamp, date_format, from_utc_timestamp
df_recebimento = df_recebimento.withColumn("CARREGAMENTO_GOLD",\
    date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss"))

display(df_recebimento)

# COMMAND ----------

# DBTITLE 1,Consultando esquema
df_recebimento.printSchema()

# COMMAND ----------

# DBTITLE 1,Verificando caminho
# MAGIC %fs ls /mnt/financeiro/

# COMMAND ----------

# DBTITLE 1,Indicar que a coluna NUMERO_NF é not null para poder particionar
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

df_recebimento = df_recebimento.withColumn("NUMERO_NF", col("NUMERO_NF").isNotNull())

# COMMAND ----------

df_recebimento = df_recebimento.withColumn("NUMERO_NF", col("NUMERO_NF").cast("int"))

# COMMAND ----------

df_recebimento.printSchema()

# COMMAND ----------

df_recebimento.write \
    .mode("overwrite") \
    .partitionBy("NUMERO_NF") \
    .parquet("dbfs:/mnt/financeiro/Gold/recebimento_gold_particionado_a.parquet")

# COMMAND ----------

# DBTITLE 1,Salvar na camada Gold
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("A").getOrCreate()

df_recebimento.write\
              .mode("overwrite")\
              .partitionBy("NUMERO_NF")\
              .parquet("dbfs:/mnt/financeiro/Gold/recebimento_gold_particionado_b.parquet")

# COMMAND ----------

df_recebimento.write \
    .mode("overwrite") \
    .partitionBy("NUMERO_NF") \
    .parquet("dbfs:/mnt/financeiro/Gold/recebimento_gold_particionado.parquet")
