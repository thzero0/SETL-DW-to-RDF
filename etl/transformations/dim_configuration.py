from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from utils import clean_text

output_path = "../clean_data/dim_configuration.parquet"

def create_dim_configuration():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("../raw_data/data_provider.csv", header=True, inferSchema=True)

    dim_configuration = df.select(
        col("Sensor").alias("sensor"),
        col("Tipo BB SP").alias("tipo_resposta_sensor"),
        col("S/N Sensor").alias("num_serie_sensor"),
        col("DAS").alias("das"),
        col("S/N DAS").alias("num_serie_das"),
    ).dropDuplicates()

    # Clean text of sensor string 
    clean_text_udf = udf(clean_text)
    dim_configuration = dim_configuration.withColumn("sensor", clean_text_udf(col("sensor")))

    window = Window.orderBy("sensor")
    dim_configuration = dim_configuration.withColumn("sk_configuration", row_number().over(window))

    null_row = spark.createDataFrame([(-1,)], ["sk_configuration"]) \
        .withColumn("sensor", lit(None)) \
        .withColumn("tipo_resposta_sensor", lit(None)) \
        .withColumn("num_serie_sensor", lit(None)) \
        .withColumn("das", lit(None)) \
        .withColumn("num_serie_das", lit(None))
    dim_configuration = dim_configuration.select("sk_configuration", "sensor", "tipo_resposta_sensor", "num_serie_sensor", "das", "num_serie_das")
    dim_configuration = dim_configuration.unionByName(null_row).orderBy("sk_configuration")


    dim_configuration.write.parquet(output_path, mode="overwrite")

    return dim_configuration

create_dim_configuration()