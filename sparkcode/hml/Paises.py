from pyspark.sql import SparkSession

# Variables
raw = 's3://256240406578-datalake-hml-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-hml-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-hml-refined/dados_publicos_cnpj'


spark = SparkSession.builder.appName("Tratando dados Gov").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Paises CSV file from S3...")
paises = spark.read.csv(f"{raw}/Paises*", inferSchema=True, sep=";", encoding='latin1')

paisColNames = ['codigo', 'descricao']
for index, colName in enumerate(paisColNames):
    paises = paises.withColumnRenamed(f'_c{index}', colName)

print("Writing cnpj dataset as a parquet table on trusted...")
paises.write.format("parquet").mode("overwrite").save(f"{trusted}/Paises")

print("Writing cnpj dataset as a parquet table on refined...")
paises.write.format('parquet').mode("overwrite").save(f"{refined}/Paises")

spark.stop()