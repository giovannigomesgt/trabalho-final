# Importar bibliotecas
print("Importando bibliotecas...")
from pyspark.sql import SparkSession

# Variables
raw = 's3://256240406578-datalake-hml-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-hml-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-hml-refined/dados_publicos_cnpj'


# Criar sessão Spark
print("Criando sessão Spark...")
spark = SparkSession.builder.appName("Tratando Dados Gov").config("spark.sql.legacy.timeParserPolicy", "LEGACY").config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ETL Naturezas
spark = SparkSession.builder.appName('naturezas').getOrCreate()
print("Lendo arquivo CSV Naturezas do S3...")
naturezas = spark.read.csv(f"{raw}/Naturezas*", inferSchema=True, sep=";", encoding='latin1')

naturezasColNames = ['codigo', 'descricao']
for index, colName in enumerate(naturezasColNames):
    naturezas = naturezas.withColumnRenamed(f'_c{index}', colName)

print("Escrevendo conjunto de dados cnpj como uma tabela parquet em trusted...")
naturezas.write.format("parquet").mode("overwrite").save(f"{trusted}/Naturezas")

print("Escrevendo conjunto de dados cnpj como uma tabela parquet em refined...")
naturezas.write.format('parquet').mode("overwrite").save(f"{refined}/Naturezas")

spark.stop()