from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

metadata_path = "../raw_data/metadatalake.csv"
providers_path = "../raw_data/data_provider.csv"
output_path = "../clean_data/dim_date.parquet"

def create_dim_date():
    spark = SparkSession.builder.getOrCreate()
    df_metadata = spark.read.csv(metadata_path, header=True, inferSchema=True)
    df_providers = spark.read.csv(providers_path, header=True, inferSchema=True)

    metadata_end_dates = df_metadata.select( 
        sf.date_format(col("endtime"), "yyyy-MM-dd").alias("date"),
        sf.year(col("endtime")).alias("year"),
        sf.month(col("endtime")).alias("month"),
        sf.dayofmonth(col("endtime")).alias("day"),
    )
    metadata_start_dates = df_metadata.select( 
        sf.date_format(col("starttime"), "yyyy-MM-dd").alias("date"),
        sf.year(col("starttime")).alias("year"),
        sf.month(col("starttime")).alias("month"),
        sf.dayofmonth(col("starttime")).alias("day"),
    )
    metadata_dates = metadata_end_dates.union(metadata_start_dates).distinct().orderBy("date")

    # Data Início Greg,Data Fim Greg
    providers_start_date = df_providers.select(
        sf.to_date(col("Data Início Greg").cast("string"), "yyyyMMdd").alias("date"),
        sf.year(sf.to_date(col("Data Início Greg").cast("string"), "yyyyMMdd")).alias("year"),
        sf.month(sf.to_date(col("Data Início Greg").cast("string"), "yyyyMMdd")).alias("month"),
        sf.dayofmonth(sf.to_date(col("Data Início Greg").cast("string"), "yyyyMMdd")).alias("day")
    )

    providers_end_date = df_providers.select(
        sf.to_date(col("Data Fim Greg").cast("string"), "yyyyMMdd").alias("date"),
        sf.year(sf.to_date(col("Data Fim Greg").cast("string"), "yyyyMMdd")).alias("year"),
        sf.month(sf.to_date(col("Data Fim Greg").cast("string"), "yyyyMMdd")).alias("month"),
        sf.dayofmonth(sf.to_date(col("Data Fim Greg").cast("string"), "yyyyMMdd")).alias("day")
    )


    dim_date = metadata_dates.union(providers_start_date).union(providers_end_date).distinct().filter(col("date").isNotNull()).orderBy("date")

    window = Window.orderBy("date")
    dim_date = dim_date.withColumn("sk_date", row_number().over(window))
    null_row = spark.createDataFrame([(-1,)], ["sk_date"]) \
        .withColumn("date", lit("0001-01-01")) \
        .withColumn("year", lit(-1)) \
        .withColumn("month", lit(-1)) \
        .withColumn("day", lit(-1))

    dim_date = dim_date.select("sk_date", "date", "year", "month", "day")

    dim_date = dim_date.unionByName(null_row).orderBy("sk_date")

    dim_date.write.parquet(output_path, mode="overwrite")

    return dim_date

create_dim_date()