# Importando bibliotecas
print("Importing libraries...")
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, functions as f

# Criando sessão Spark
print("Creating SparkSession...")
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Tratando dados Gov") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ETL Estabelecimentos
print("Reading Estabelecimentos CSV file from S3...")
estabelecimentos = spark \
    .read \
    .csv("s3://dadosgov-raw-256240406578/dados_publicos_cnpj/Estabelecimentos*",
         inferSchema=True, sep=";", encoding='latin1')

col_names = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
             'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
             'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais',
             'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
             'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep',
             'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
             'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_da_situacao_especial']

for i, col_name in enumerate(col_names):
    estabelecimentos = estabelecimentos.withColumnRenamed(f'_c{i}', col_name)

print("Writing cnpj dataset as a parquet table on trusted...")
estabelecimentos \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3://dadosgov-trusted-256240406578/dados_publicos_cnpj/Estabelecimentos")

# Convertendo colunas de data
estabelecimentos = estabelecimentos\
    .withColumn(
        'data_situacao_cadastral',
        f.to_date(estabelecimentos['data_situacao_cadastral'].cast(
            StringType()), 'yyyyMMdd')
    )\
    .withColumn(
        'data_de_inicio_atividade',
        f.to_date(estabelecimentos['data_de_inicio_atividade'].cast(
            StringType()), 'yyyyMMdd')
    )\
    .withColumn(
        'data_da_situacao_especial',
        f.to_date(estabelecimentos['data_da_situacao_especial'].cast(
            StringType()), 'yyyyMMdd')
    )

estabelecimentos.createOrReplaceTempView('estabelecimentos')

estabelecimentos = spark.sql("""
  SELECT
  cnpj_basico,
  lpad(cnpj_ordem,4,'0') as cnpj_ordem,
  lpad(cnpj_dv,2,'0') as cnpj_dv,
  CONCAT(cnpj_basico, lpad(cnpj_ordem,4,'0'), lpad(cnpj_dv,2,'0')) AS cnpj_completo,
  CASE   
          WHEN identificador_matriz_filial = 1 THEN 'Matriz'   
          WHEN identificador_matriz_filial = 2 THEN 'Filial'
  END as identificador_matriz_filial,
  nome_fantasia,
  CASE   
          WHEN situacao_cadastral = 1 THEN 'Nula'   
          WHEN situacao_cadastral = 2 THEN 'Ativa'
          WHEN situacao_cadastral = 3 THEN 'Suspensa'
          WHEN situacao_cadastral = 4 THEN 'Inapta'
          WHEN situacao_cadastral = 8 THEN 'Baixada'
  END as situacao_cadastral,
  data_situacao_cadastral,
  motivo_situacao_cadastral,
  nome_da_cidade_no_exterior,
  pais,
  data_de_inicio_atividade,
  cnae_fiscal_principal,
  cnae_fiscal_secundaria,
  tipo_de_logradouro,
  logradouro,
  numero,
  complemento,
  bairro,
  cep,
  uf,
  municipio,
  ddd_1,
  telefone_1,
  ddd_2,
  telefone_2,
  ddd_do_fax,
  fax,
  correio_eletronico,
  situacao_especial,
  data_da_situacao_especial
  FROM estabelecimentos
""")

print("Writing cnpj dataset as a parquet table on Refined...")
(
    estabelecimentos
    .write
    .format('parquet')
    .mode("overwrite")
    .save("s3://dadosgov-refined-256240406578/dados_publicos_cnpj/Estabelecimentos")
)

spark.stop()