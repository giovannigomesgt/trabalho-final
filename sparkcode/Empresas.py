# Importing Libraries
print("Importing libraries...")
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import SparkSession, functions as f

# Creating SparkSession
print("Creating SparkSession...")
spark = SparkSession.builder.master("local[*]")\
                      .appName("Tratando dados Gov")\
                      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
                      .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Reading Empresas CSV from S3
print("Reading Empresas CSV file from S3...")
empresas = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Empresas*",
                        inferSchema=True, sep=";", encoding='latin1')

# Renaming Columns
empresasColNames = ['cnpj_basico',
                    'razao_social_nome_empresarial',
                    'natureza_juridica',
                    'qualificacao_do_responsavel',
                    'capital_social_da_empresa',
                    'porte_da_empresa',
                    'ente_federativo_responsavel']
for index, colName in enumerate(empresasColNames):
    empresas = empresas.withColumnRenamed(f'_c{index}', colName)

# Writing cnpj dataset as a parquet table on Trusted
print("Writing cnpj dataset as a parquet table on Trusted...")
empresas.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Empresas")

# Formatting capital_social_da_empresa Column
empresas = empresas.withColumn("capital_social_da_empresa", f.regexp_replace("capital_social_da_empresa", ',', '.'))
empresas = empresas.withColumn("capital_social_da_empresa", empresas["capital_social_da_empresa"].cast(DoubleType()))

# Creating Temp View and Selecting Data
empresas.createOrReplaceTempView("empresas")
empresas = spark.sql("""
  SELECT cnpj_basico,
         razao_social_nome_empresarial,
         natureza_juridica,
         qualificacao_do_responsavel,
         capital_social_da_empresa,
         CASE   
             WHEN porte_da_empresa = 00 THEN 'Nao Informado'   
             WHEN porte_da_empresa = 01 THEN 'Micro Empresa'
             WHEN porte_da_empresa = 03 THEN 'Empresa de Pequeno Porte'
             WHEN porte_da_empresa = 05 THEN 'Demais'
  END as porte_da_empresa,
  ente_federativo_responsavel
  FROM empresas
""")

print("Writing cnpj dataset as a parquet table on Refined...")
(
    empresas
    .write
    .format('parquet')
    .mode("overwrite")
    .save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Empresas")
)
spark.stop()