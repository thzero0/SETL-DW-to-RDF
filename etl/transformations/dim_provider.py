from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from utils import clean_text

output_path = "../clean_data/dim_provider.parquet"

def create_dim_provider():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("../raw_data/data_provider.csv", header=True, inferSchema=True)

    dim_provider = df.select(
        sf.col("Estação").alias("estacao"),
        sf.col("Sigla da rede").alias("rede"),

        # corrigindo vírgula → ponto antes do cast
        sf.regexp_replace("Lat",  ",", ".").cast("double").alias("latitude"),
        sf.regexp_replace("Long", ",", ".").cast("double").alias("longitude"),
        sf.regexp_replace("Alt",  ",", ".").cast("double").alias("altitude"),

        sf.col("Localidade").alias("localidade"),
        sf.col("Estado").alias("estado"),
    ).dropDuplicates()


    # Clean text of localidade string
    clean_text_udf = udf(clean_text)
    dim_provider = dim_provider.withColumn("localidade", clean_text_udf(col("localidade")))

    window = Window.orderBy("estacao")
    dim_provider = dim_provider.withColumn("sk_provider", row_number().over(window))

    null_row = spark.createDataFrame([(-1,)], ["sk_provider"]) \
        .withColumn("estacao", lit("Unknown")) \
        .withColumn("rede", lit("Unknown")) \
        .withColumn("latitude", lit(-1)) \
        .withColumn("longitude", lit(-1)) \
        .withColumn("altitude", lit(-1)) \
        .withColumn("localidade", lit("Unknown")) \
        .withColumn("estado", lit("Unknown"))
    dim_provider = dim_provider.select("sk_provider", "estacao", "rede", "latitude", "longitude", "altitude", "localidade", "estado")
    dim_provider = dim_provider.unionByName(null_row).orderBy("sk_provider")

    dim_provider.write.parquet(output_path, mode="overwrite")

    return dim_provider

create_dim_provider()