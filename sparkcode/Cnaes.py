# Importing Libraries
print("Importing libraries...")
from pyspark.sql import SparkSession

# Creating SparkSession
print("Creating SparkSession...")
spark = SparkSession.builder.master('local[*]') \
    .appName("Processing Gov Data") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ETL CNAEs
print("Reading CNAEs CSV file from S3...")
cnaes = spark.read \
    .csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Cnaes*", 
         inferSchema=True, sep=";", encoding='latin1')
cnaes = cnaes.withColumnRenamed("_c0", "codigo") \
             .withColumnRenamed("_c1", "descricao")

# Write to Parquet
print("Writing CNAEs dataset as a parquet table on trusted...")
cnaes.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Cnaes")

print("Writing CNAEs dataset as a parquet table on refined...")
cnaes.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Cnaes")

spark.stop()