# Importing Libraries
print("Importing libraries...")
from pyspark.sql import SparkSession

# Variables
raw = 's3://256240406578-datalake-dev-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-dev-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-dev-refined/dados_publicos_cnpj'

# Creating Spark Session
print("Creating SparkSession...")
spark = SparkSession.builder \
                    .appName("Tratando dados Gov")\
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
                    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Reading CSV file
print("Reading Motivos CSV file from S3...")
motivos = spark.read.csv(f"{raw}/Motivos*",
                        inferSchema=True, sep=";", encoding='latin1')

# Renaming columns
motivosColNames = ['codigo', 'descricao']
for index, colName in enumerate(motivosColNames):
    motivos = motivos.withColumnRenamed(f'_c{index}', colName)

# Writing as parquet table in trusted and refined
print("Writing cnpj dataset as a parquet table on trusted...")
motivos.write.format("parquet").mode("overwrite").save(f"{trusted}/Motivos")
print("Writing cnpj dataset as a parquet table on refined...")
motivos.write.format("parquet").mode("overwrite").save(f"{refined}/Motivos")

# Stopping Spark Session
spark.stop()