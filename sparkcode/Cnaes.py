# Importing Libraries
print("Importing libraries...")
from pyspark.sql import SparkSession

# Variables
raw = 's3://256240406578-datalake-dev-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-dev-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-dev-refined/dados_publicos_cnpj'


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
    .csv(f"{raw}/Cnaes*", 
         inferSchema=True, sep=";", encoding='latin1')
cnaes = cnaes.withColumnRenamed("_c0", "codigo") \
             .withColumnRenamed("_c1", "descricao")

# Write to Parquet
print("Writing CNAEs dataset as a parquet table on trusted...")
cnaes.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{trusted}/Cnaes")

print("Writing CNAEs dataset as a parquet table on refined...")
cnaes.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{refined}/Cnaes")

spark.stop()