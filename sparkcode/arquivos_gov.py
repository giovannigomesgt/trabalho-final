from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Tratando dados Gov") \
    .getOrCreate()

print('EMPRESAS')
spark = SparkSession.builder.appName('empresas').getOrCreate()

print('Lendo Arquivos')
empresas = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Empresas/",
        inferSchema=True, sep=";", encoding='latin1')
)
print('Nomeando colunas')
empresasColNames = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica', 'qualificacao_do_responsavel', 'capital_social_da_empresa', 'porte_da_empresa', 'ente_federativo_responsavel']
for index, colName in enumerate(empresasColNames):
  empresas = empresas.withColumnRenamed(f'_c{index}', colName)

empresas.show()
empresas = empresas.withColumn('capital_social_da_empresa',f.regexp_replace('capital_social_da_empresa',',','.'))
empresas = empresas.withColumn('capital_social_da_empresa', empresas['capital_social_da_empresa'].cast(DoubleType()))

empresas.limit(5).show()
print('Criando View Empresa')
empresas.createOrReplaceTempView('empresas')

empresas = spark.sql("""
  SELECT
  cnpj_basico,
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

print('Salvando no S3')
(
    empresas
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/empresas")
)

print('ESTABELECIMENTOS')
spark = SparkSession.builder.appName('estabelecimentos').getOrCreate()

estabelecimentos = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Estabelecimentos/",
         inferSchema=True, sep=";", encoding='latin1')
)
print('Nomeando Colunas')
estabsColNames = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_da_situacao_especial']
for index, colName in enumerate(estabsColNames):
  estabelecimentos = estabelecimentos.withColumnRenamed(f'_c{index}', colName)

estabelecimentos.show()

print('Formatando Colunas')
estabelecimentos = estabelecimentos\
  .withColumn(
      'data_situacao_cadastral',
      f.to_date(estabelecimentos['data_situacao_cadastral'].cast(StringType()), 'yyyyMMdd')
  )\
  .withColumn(
      'data_de_inicio_atividade',
      f.to_date(estabelecimentos['data_de_inicio_atividade'].cast(StringType()), 'yyyyMMdd')
  )\
  .withColumn(
      'data_da_situacao_especial',
      f.to_date(estabelecimentos['data_da_situacao_especial'].cast(StringType()), 'yyyyMMdd')
  )

estabelecimentos.limit(5).show()

print('Criando View para Estabelecimentos')
estabelecimentos.createOrReplaceTempView('estabelecimentos')

estabelecimentos = spark.sql("""
  SELECT
  cnpj_basico,
  cnpj_ordem,
  cnpj_dv,
  CASE   
          WHEN identificador_matriz_filial = 1 THEN 'Matriz'   
          WHEN identificador_matriz_filial = 2 THEN 'Filial'
  END as identificador_matriz_filial,
  nome_fantasia,
  situacao_cadastral,
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

print('Salvando no S3')
(
    estabelecimentos
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Estabelecimentos/")
)

print('Simples')

spark = SparkSession.builder.appName('simples').getOrCreate()

print('Lendo arquivos')
simples = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Simples/",
    inferSchema=True, sep=";", encoding='latin1')
)
simples.show()

print('Nomeando Colunas')
simpleColNames = ['cnpj_basico', 'opcao_pelo_simples', 'data_de_opcao_pelo_simples', 'data_de_exclusao_do_simples', 'opcao_pelo_mei', 'data_de_opcao_pelo_mei', 'data_de_exclusao_do_mei']

for index, colName in enumerate(simpleColNames):
  simples = simples.withColumnRenamed(f'_c{index}', colName)

print('Formatando Colunas')
simples = simples\
  .withColumn(
      'data_de_opcao_pelo_simples',
      f.to_date(simples['data_de_opcao_pelo_simples'].cast(StringType()), 'yyyyMMdd')
  )\
  .withColumn(
      'data_de_exclusao_do_simples',
      f.to_date(simples['data_de_exclusao_do_simples'].cast(StringType()), 'yyyyMMdd')
  )\
  .withColumn(
      'data_de_opcao_pelo_mei',
      f.to_date(simples['data_de_opcao_pelo_mei'].cast(StringType()), 'yyyyMMdd')
  )\
  .withColumn(
      'data_de_exclusao_do_mei',
      f.to_date(simples['data_de_exclusao_do_mei'].cast(StringType()), 'yyyyMMdd')
  )

simples.show()
(
    simples
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Simples/")
)

print('SOCIOS')

spark = SparkSession.builder.appName('socios').getOrCreate()

print('Lendo Arquivos')
socios = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Socios/",
      inferSchema=True, sep=";", encoding='latin1')
)
print('Renomeando Colunas')
sociosColNames = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio', 'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']

for index, colName in enumerate(sociosColNames):
  socios = socios.withColumnRenamed(f'_c{index}', colName)

socios = socios\
  .withColumn(
      'data_de_entrada_sociedade',
      f.to_date(socios['data_de_entrada_sociedade'].cast(StringType()), 'yyyyMMdd')
  )

socios.show()

print('Criando View para Socios')
socios.createOrReplaceTempView('socios')

socios = spark.sql("""
  SELECT
  cnpj_basico,
  CASE   
          WHEN identificador_de_socio = 1 THEN 'Pessoa Jurídica'   
          WHEN identificador_de_socio = 2 THEN 'Pessoa Física'
          WHEN identificador_de_socio = 3 THEN 'Estrangeiro'
  END as identificador_de_socio,
  nome_do_socio_ou_razao_social,
  cnpj_ou_cpf_do_socio,
  qualificacao_do_socio,
  data_de_entrada_sociedade,
  pais,
  representante_legal,
  nome_do_representante,
  qualificacao_do_representante_legal,
  CASE   
          WHEN faixa_etaria = 1 THEN 'entre 0 a 12 anos'   
          WHEN faixa_etaria = 2 THEN 'entre 13 a 20 anos'
          WHEN faixa_etaria = 3 THEN 'entre 21 a 30 anos'
          WHEN faixa_etaria = 4 THEN 'entre 31 a 40 anos'
          WHEN faixa_etaria = 5 THEN 'entre 41 a 50 anos'
          WHEN faixa_etaria = 6 THEN 'entre 51 a 60 anos'
          WHEN faixa_etaria = 7 THEN 'entre 61 a 70 anos'
          WHEN faixa_etaria = 8 THEN 'entre 71 a 80 anos'
          WHEN faixa_etaria = 9 THEN 'mais de 80 anos'
          WHEN faixa_etaria = 0 THEN 'não se aplica'
  END as faixa_etaria
  FROM socios
""")

print('Salvando Arquivos no S3')
(
    socios
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Socios/")
)

print('PAISES')
spark = SparkSession.builder.appName('paises').getOrCreate()

print('Lendo Arquivos')
paises = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Paises/",
         inferSchema=True, sep=";", encoding='latin1')
)
paises.show()

paisColNames = ['codigo', 'descricao']
for index, colName in enumerate(paisColNames):
  paises = paises.withColumnRenamed(f'_c{index}', colName)

paises.show()
print('Salvando no S3')
(
    paises
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Paises/")
)

print('MUNICIPIOS')

spark = SparkSession.builder.appName('municipios').getOrCreate()
print('Lendo Arquivos')
municipios = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Municipios/",
    inferSchema=True, sep=";", encoding='latin1')
)
municipios.show()

municipiosColNames = ['codigo', 'descricao']
for index, colName in enumerate(municipiosColNames):
  municipios = municipios.withColumnRenamed(f'_c{index}', colName)

municipios.show()

print('Salvando no S3')
(
    municipios
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Municipios/")
)

print('QUALIFICACOES')

spark = SparkSession.builder.appName('qualificacoes').getOrCreate()

qualificacoes = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Qualificacoes/",
    inferSchema=True, sep=";", encoding='latin1')
)
qualificacoes.show()

qualificacoesColNames = ['codigo', 'descricao']
for index, colName in enumerate(qualificacoesColNames):
  qualificacoes = qualificacoes.withColumnRenamed(f'_c{index}', colName)

qualificacoes.show()

print('Salvando no S3')
(
    qualificacoes
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Qualificacoes/")
)

print('NATUREZAS')

spark = SparkSession.builder.appName('naturezas').getOrCreate()
print('Lendo Arquivos')
naturezas = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Naturezas/",
         inferSchema=True, sep=";", encoding='latin1')
)
naturezas.show()

naturezasColNames = ['codigo', 'descricao']
for index, colName in enumerate(naturezasColNames):
  naturezas = naturezas.withColumnRenamed(f'_c{index}', colName)

naturezas.show()

print('Salvando no S3')
(
    naturezas
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Naturezas/")
)

print('CNAEs')
spark = SparkSession.builder.appName('CNAEs').getOrCreate()

cnaes = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Cnaes/",
    inferSchema=True, sep=";", encoding='latin1')
)
cnaes.show()

cnaesColNames = ['codigo', 'descricao']
for index, colName in enumerate(cnaesColNames):
  cnaes = cnaes.withColumnRenamed(f'_c{index}', colName)

cnaes.show()

print('Escrevendo no S3')
(
    cnaes
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/cnaes/")
)

print('Motivos')

spark = SparkSession.builder.appName('motivos').getOrCreate()
print('Lendo Arquivos')
motivos = (
    spark
    .read
    .csv("s3://dadosgov-256240406578/v2/dados_publicos_cnpj/Motivos/",
    inferSchema=True, sep=";", encoding='latin1')
)
motivos.show()

motivosColNames = ['codigo', 'descricao']
for index, colName in enumerate(motivosColNames):
  motivos = motivos.withColumnRenamed(f'_c{index}', colName)

motivos.show()

print('Salvando no S3')
(
    motivos
    .write
    .format('parquet')
    .mode("append")
    .save("s3://dadosgov-256240406578/PARQUETs/Motivos/")
)