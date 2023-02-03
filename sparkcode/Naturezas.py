# Importar bibliotecas
print("Importando bibliotecas...")
from pyspark.sql import SparkSession

# Criar sessão Spark
print("Criando sessão Spark...")
spark = SparkSession.builder.master("local[*]").appName("Tratando Dados Gov").config("spark.sql.legacy.timeParserPolicy", "LEGACY").config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ETL Naturezas
spark = SparkSession.builder.appName('naturezas').getOrCreate()
print("Lendo arquivo CSV Naturezas do S3...")
naturezas = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Naturezas*", inferSchema=True, sep=";", encoding='latin1')

naturezasColNames = ['codigo', 'descricao']
for index, colName in enumerate(naturezasColNames):
    naturezas = naturezas.withColumnRenamed(f'_c{index}', colName)

print("Escrevendo conjunto de dados cnpj como uma tabela parquet em trusted...")
naturezas.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Naturezas")

print("Escrevendo conjunto de dados cnpj como uma tabela parquet em refined...")
naturezas.write.format('parquet').mode("overwrite").save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Naturezas")

spark.stop()