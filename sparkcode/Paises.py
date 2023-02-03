from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName("Tratando dados Gov").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Paises CSV file from S3...")
paises = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Paises*", inferSchema=True, sep=";", encoding='latin1')

paisColNames = ['codigo', 'descricao']
for index, colName in enumerate(paisColNames):
    paises = paises.withColumnRenamed(f'_c{index}', colName)

print("Writing cnpj dataset as a parquet table on trusted...")
paises.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Paises")

print("Writing cnpj dataset as a parquet table on refined...")
paises.write.format('parquet').mode("overwrite").save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Paises")

spark.stop()