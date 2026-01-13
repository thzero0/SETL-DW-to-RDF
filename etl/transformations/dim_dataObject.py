from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from utils import clean_text

output_path = "../clean_data/dim_dataObject.parquet"

def create_dim_dataObject():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("../raw_data/metadatalake.csv", header=True, inferSchema=True)

    dim_dataObject = df.select( 
        col("ID").alias("source_id"),
        col("filename").alias("title"), 
        col("channel").alias("channel"),
    )

    window = Window.orderBy("source_id")
    dim_dataObject = dim_dataObject.withColumn("sk_dataObject", row_number().over(window))

    null_row = spark.createDataFrame([(-1,)], ["sk_dataObject"]) \
        .withColumn("source_id", lit(-1)) \
        .withColumn("title", lit("Unknown")) \
        .withColumn("channel", lit("Unknown"))
    dim_dataObject = dim_dataObject.select("sk_dataObject", "source_id", "title", "channel")
    dim_dataObject = dim_dataObject.unionByName(null_row).orderBy("sk_dataObject")

    dim_dataObject.write.parquet(output_path, mode="overwrite")

    return dim_dataObject

if __name__ == "__main__":
    create_dim_dataObject()