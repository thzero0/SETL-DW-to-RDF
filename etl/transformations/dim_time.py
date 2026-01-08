from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unidecode

metadata_path = "../raw_data/metadata/"

def create_dim_time():
    spark = SparkSession.builder.getOrCreate()
    df_metadata = spark.read.csv(metadata_path, header=True, inferSchema=True)

    # print head of the dataframe
    df_metadata.show(5)

create_dim_time()