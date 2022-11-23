from pyspark.sql import functions as f
from pyspark.sql import SparkSession

print('Iniciando uma SparkSession')

spark = SparkSession.builder.appName('filmes').getOrCreate()

print("Reading CSV file from S3...")

filmes = (
  spark
 .read
 .csv('s3://microdados-256240406578/CSVs/EDITED-imdb_top250_movies.csv', header=True, sep=";", inferSchema=True )    
)

print('Imprimindo Schema')

filmes.printSchema()

print('Renomeando Colunas')

filmes = (
    filmes
    .withColumnRenamed(filmes.columns[0],'id')
    .withColumnRenamed('Num','num')
    .withColumnRenamed('Title','title')
    .withColumnRenamed('Year','year')
    .withColumnRenamed('Released','released')
    .withColumnRenamed('Runtime','runtime')
    .withColumnRenamed('Genre','genre')
    .withColumnRenamed('Director','director')
    .withColumnRenamed('Writer','writer')
    .withColumnRenamed('Actors','actors')
    .withColumnRenamed('Plot','plot')
    .withColumnRenamed('Language','language')
    .withColumnRenamed('Country','country')
    .withColumnRenamed('Awards','awards')
    .withColumnRenamed('Metascore','metascore')
    .withColumnRenamed('imdbRating','imdb_rating')
    .withColumnRenamed('imdbVotes','imdb_votes')
    .withColumnRenamed('imdbID','imdb_id')
    .withColumnRenamed('Type','type')
    .withColumnRenamed('DVD','dvd')
    .withColumnRenamed('BoxOffice','box_office')
    .withColumnRenamed('Production','production')
    .withColumnRenamed('Website','website')
)

print('Formatando Colunas')

filmes = filmes.withColumn("imdb_rating",filmes.imdb_rating.cast('double'))
filmes = filmes.withColumn("metascore",filmes.metascore.cast('double'))
filmes = filmes.withColumn("imdb_votes",filmes.imdb_votes.cast('double'))

print('Imprimindo Tabela')

filmes.show()

print('Criando uma View para tabela')

filmes.createOrReplaceTempView('filmes')

print('Criando primeiro indicador')

# 1 - Quantidade de filmes por diretor
indicador1 = spark.sql("""
  SELECT
  director,
  COUNT(title) as Qtd_Filmes
  FROM filmes
  GROUP BY director
  ORDER BY COUNT(title) DESC
""")
indicador1.show()

# 2 - média imdb_rating por diretor
indicador2 = spark.sql("""
  SELECT
  director,
  mean(imdb_rating) as media_imdb_rating
  FROM filmes
  GROUP BY director
  ORDER BY mean(imdb_rating) DESC
""")
indicador2.show()

# 3 - Tempo do maior filme por diretor
indicador3 = spark.sql("""
  SELECT
  director,
  max(runtime) as maior_filme_min
  FROM filmes
  GROUP BY director
  ORDER BY max(runtime) DESC
""")
indicador3.show()


print('Escrevendo arquivos no formato parquet')
#Transformando em Parquet
(
    indicador1
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador1")
)
print('Indicador 1 escrito com Sucesso!')
#Transformando em Parquet
(
    indicador2
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador2")
)
print('Indicador 2 escrito com Sucesso!')

#Transformando em Parquet
(
    indicador3
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador3")
)
print('Indicador 3 escrito com Sucesso!')


df1 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador1')
)

df2 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador2')
)

df3 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador3')
)

df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')
df3.createOrReplaceTempView('df3')

print('Juntando tabelas')

tabelafinal = spark.sql("""
   SELECT
   df1.*,
   df2.media_imdb_rating,
   df3.maior_filme_min
   FROM df1
   INNER JOIN df2
   ON df1.director = df2.director
   INNER JOIN df3
   ON df1.director = df3.director
""")

tabelafinal.show()

#Transformando em Parquet
(
    tabelafinal
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs/tabelafinal")
)

