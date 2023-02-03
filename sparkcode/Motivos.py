# Importing Libraries
print("Importing libraries...")
from pyspark.sql import SparkSession

# Creating Spark Session
print("Creating SparkSession...")
spark = SparkSession.builder.master('local[*]')\
                    .appName("Tratando dados Gov")\
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
                    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Reading CSV file
print("Reading Motivos CSV file from S3...")
motivos = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Motivos*",
                        inferSchema=True, sep=";", encoding='latin1')

# Renaming columns
motivosColNames = ['codigo', 'descricao']
for index, colName in enumerate(motivosColNames):
    motivos = motivos.withColumnRenamed(f'_c{index}', colName)

# Writing as parquet table in trusted and refined
print("Writing cnpj dataset as a parquet table on trusted...")
motivos.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Motivos")
print("Writing cnpj dataset as a parquet table on refined...")
motivos.write.format("parquet").mode("overwrite").save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Motivos")

# Stopping Spark Session
spark.stop()