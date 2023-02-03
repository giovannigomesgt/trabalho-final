from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName("Tratando dados Gov").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Qualificacoes CSV file from S3...")
qualificacoes = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Qualificacoes*",
                               inferSchema=True, sep=";", encoding='latin1')

qualificacoes = qualificacoes.selectExpr("_c0 as codigo", "_c1 as descricao")

print("Writing cnpj dataset as a parquet table on trusted...")

qualificacoes.write.format("parquet").mode("overwrite").save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Qualificacoes")

print("Writing cnpj dataset as a parquet table on refined...")

qualificacoes.write.format('parquet').mode("overwrite").save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Qualificacoes")

spark.stop()