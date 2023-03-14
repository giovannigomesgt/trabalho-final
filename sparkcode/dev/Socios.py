from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Variables
raw = 's3://256240406578-datalake-dev-raw/dados_publicos_cnpj'
trusted = 's3://256240406578-datalake-dev-trusted/dados_publicos_cnpj'
refined = 's3://256240406578-datalake-dev-refined/dados_publicos_cnpj'


spark = SparkSession.builder.appName("Tratando dados Gov").config("spark.sql.legacy.timeParserPolicy", "LEGACY").config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Socios CSV file from S3...")
socios = spark.read.csv(f"{raw}/Socios*",
         inferSchema=True, sep=";", encoding='latin1')

colnames = ['cnpj_basico',
                  'identificador_de_socio',
                  'nome_do_socio_ou_razao_social',
                  'cnpj_ou_cpf_do_socio',
                  'qualificacao_do_socio',
                  'data_de_entrada_sociedade',
                  'pais',
                  'representante_legal',
                  'nome_do_representante',
                  'qualificacao_do_representante_legal',
                  'faixa_etaria']

for i, colname in enumerate(colnames):
    socios = socios.withColumnRenamed(f'_c{i}', colname)

    print("Writing cnpj dataset as a parquet table on trusted...")
socios.write.format("parquet").mode("overwrite").save(f"{trusted}/Socios")

socios = socios.withColumn('data_de_entrada_sociedade', f.to_date(socios['data_de_entrada_sociedade'].cast(StringType()), 'yyyyMMdd'))
socios.createOrReplaceTempView('socios')

socios = spark.sql("""
  SELECT cnpj_basico,
         CASE WHEN identificador_de_socio = 1 THEN 'Pessoa Juridica'   
              WHEN identificador_de_socio = 2 THEN 'Pessoa Fisica'
              WHEN identificador_de_socio = 3 THEN 'Estrangeiro' END as identificador_de_socio,
         nome_do_socio_ou_razao_social,
         cnpj_ou_cpf_do_socio,
         qualificacao_do_socio,
         data_de_entrada_sociedade,
         pais,
         representante_legal,
         nome_do_representante,
         qualificacao_do_representante_legal,
         CASE WHEN faixa_etaria = 1 THEN 'Entre 0 a 12 Anos'   
              WHEN faixa_etaria = 2 THEN 'Entre 13 a 20 Anos'
              WHEN faixa_etaria = 3 THEN 'Entre 21 a 30 Anos'
              WHEN faixa_etaria = 4 THEN 'Entre 31 a 40 Anos'
              WHEN faixa_etaria = 5 THEN 'Entre 41 a 50 Anos'
              WHEN faixa_etaria = 6 THEN 'Entre 51 a 60 Anos'
              WHEN faixa_etaria = 7 THEN 'Entre 61 a 70 Anos'
              WHEN faixa_etaria = 8 THEN 'Entre 71 a 80 Anos'
              WHEN faixa_etaria = 9 THEN 'Mais de 80 Anos'
              WHEN faixa_etaria = 0 THEN 'Nao se Aplica'
        END as faixa_etaria
    FROM socios
""")

print("Writing cnpj dataset as a parquet table on refined...")
socios.write.format('parquet').mode("overwrite").save(f"{refined}/Socios")

spark.stop()