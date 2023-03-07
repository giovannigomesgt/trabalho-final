from pyspark.sql import SparkSession

# Variables
raw = 's3://256240406578-datalake-dev-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-dev-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-dev-refined/dados_publicos_cnpj'


spark = SparkSession.builder.master('local[*]').appName("Tratando dados Gov").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Qualificacoes CSV file from S3...")
qualificacoes = spark.read.csv(f"{raw}/Qualificacoes*",
                               inferSchema=True, sep=";", encoding='latin1')

qualificacoes = qualificacoes.selectExpr("_c0 as codigo", "_c1 as descricao")

print("Writing cnpj dataset as a parquet table on trusted...")

qualificacoes.write.format("parquet").mode("overwrite").save(f"{trusted}/Qualificacoes")

print("Writing cnpj dataset as a parquet table on refined...")

qualificacoes.write.format('parquet').mode("overwrite").save(f"{refined}/Qualificacoes")

spark.stop()