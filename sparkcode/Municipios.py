# Importing libraries
print("Importing libraries...")
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Creating a Spark session
print("Creating SparkSession...")
spark = SparkSession.builder.master("local[*]").appName("Tratando dados Gov")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ETL for Municipios
print("Reading Municipios CSV file from S3...")
municipios = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Municipios*",
                           inferSchema=True, sep=";", encoding='latin1')
municipios = municipios.withColumnRenamed("_c0", "codigo").withColumnRenamed("_c1", "descricao")

# Writing to Parquet in S3
print("Writing cnpj dataset as a parquet table on trusted...")
municipios.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Municipios")

print("Writing cnpj dataset as a parquet table on refined...")
municipios.write.format("parquet").mode("overwrite").save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Municipios")

spark.stop()