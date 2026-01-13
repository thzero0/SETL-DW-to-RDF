from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, to_timestamp, date_format
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

metadata_path = "../raw_data/metadatalake.csv"
output_path = "../clean_data/dim_time.parquet"

def create_dim_time():
    # 1. Configura o Spark para usar UTC (IGNORA o fuso horário do seu PC)
    spark = SparkSession.builder \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    # 2. Lê os dados
    df_metadata = spark.read.csv(metadata_path, header=True, inferSchema=False)

    # 3. Converte para Timestamp (agora ele respeita o 17h como 17h)
    # O formato "yyyy-MM-dd HH:mm:ss.SSSSSSXXX" cobre o seu padrão com timezone
    df_metadata = df_metadata.withColumn("starttime_ts", col("starttime").cast("timestamp")) \
                             .withColumn("endtime_ts", col("endtime").cast("timestamp"))

    # 4. Extrai a string HH:mm:ss diretamente do Timestamp
    metadata_end_times = df_metadata.select( 
        sf.date_format(col("endtime_ts"), "HH:mm:ss").alias("timestamp"),
        sf.hour(col("endtime_ts")).alias("hour"),
        sf.minute(col("endtime_ts")).alias("minute"),
        sf.second(col("endtime_ts")).alias("second"),
    ).filter(col("timestamp").isNotNull())

    metadata_start_times = df_metadata.select( 
        sf.date_format(col("starttime_ts"), "HH:mm:ss").alias("timestamp"),
        sf.hour(col("starttime_ts")).alias("hour"),
        sf.minute(col("starttime_ts")).alias("minute"),
        sf.second(col("starttime_ts")).alias("second"),
    ).filter(col("timestamp").isNotNull())

    # 5. União, Distinct e Ordenação
    dim_time = metadata_end_times.union(metadata_start_times).distinct().orderBy("timestamp")

    # 6. Criação do SK
    window = Window.orderBy("timestamp")
    dim_time = dim_time.withColumn("sk_time", row_number().over(window))
    
    # Linha para "Desconhecido"
    null_row = spark.createDataFrame([(-1,)], ["sk_time"]) \
        .withColumn("timestamp", lit("Unknown")) \
        .withColumn("hour", lit(-1)) \
        .withColumn("minute", lit(-1)) \
        .withColumn("second", lit(-1))

    dim_time = dim_time.select("sk_time", "timestamp", "hour", "minute", "second")
    dim_time = dim_time.unionByName(null_row).orderBy("sk_time")

    dim_time.write.parquet(output_path, mode="overwrite")
    
    print("Dimensão Time gerada com sucesso em UTC!")
    return dim_time

if __name__ == "__main__":
    create_dim_time()