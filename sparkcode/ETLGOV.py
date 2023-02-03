# ### IMPORTANDO BIBLIOTECAS
print("Importing libraries...")
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as f



print("Creating SparkSession...")
# ### CRIANDO UMA SECAO SPARK
spark = (
    SparkSession.builder.master('local[*]')
    .appName("Tratando dados Gov")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

