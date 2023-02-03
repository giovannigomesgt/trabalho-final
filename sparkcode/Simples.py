from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('simples').getOrCreate()

simples = spark.read.csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Simples*",
         inferSchema=True, sep=";", encoding='latin1')

col_names = ['cnpj_basico', 'opcao_pelo_simples', 'data_de_opcao_pelo_simples',            'data_de_exclusao_do_simples', 'opcao_pelo_mei', 'data_de_opcao_pelo_mei', 'data_de_exclusao_do_mei']

simples = simples.toDF(*col_names)

date_cols = ['data_de_opcao_pelo_simples', 'data_de_exclusao_do_simples', 'data_de_opcao_pelo_mei', 'data_de_exclusao_do_mei']
for col in date_cols:
    simples = simples.withColumn(col, f.to_date(simples[col].cast(StringType()), 'yyyyMMdd'))

simples = simples.withColumn('opcao_pelo_simples', 
                 f.when(simples['opcao_pelo_simples'] == 'S', 'Sim')
                 .when(simples['opcao_pelo_simples'] == 'N', 'Nao')
                 .otherwise('Outros'))

simples = simples.withColumn('opcao_pelo_mei', 
                 f.when(simples['opcao_pelo_mei'] == 'S', 'Sim')
                 .when(simples['opcao_pelo_mei'] == 'N', 'Nao'))

simples.write.format("parquet").mode("overwrite")\
    .save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Simples")

simples.write.format('parquet').mode("overwrite")\
    .save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Simples")

spark.stop()