import os
import shutil
from pyspark.sql import SparkSession

def parquet_to_csv_batch(input_dir, output_dir):
    """
    Converte todos os arquivos .parquet dentro de input_dir em arquivos .csv no output_dir.
    Cada .parquet vira um .csv único.
    """
    output_tmp = "../clean_data/tmp_csv"
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(input_dir)
    df.coalesce(1).write.csv(output_tmp, mode="overwrite", header=True)
    for tmp_file in os.listdir(output_tmp):
        if tmp_file.startswith("part-") and tmp_file.endswith(".csv"):
            shutil.move(os.path.join(output_tmp, tmp_file), output_dir)
            break
    shutil.rmtree(output_tmp)


dim_date = "../clean_data/dim_date"
dim_time = "../clean_data/dim_time"
dim_licence = "../clean_data/dim_licence"
dim_permissions = "../clean_data/dim_permissions"
dim_status = "../clean_data/dim_status"
dim_configuration = "../clean_data/dim_configuration"
dim_provider = "../clean_data/dim_provider"



#parquet_to_csv_batch(f"{dim_time}.parquet", f"{dim_time}.csv")
#parquet_to_csv_batch(f"{dim_date}.parquet", f"{dim_date}.csv")
parquet_to_csv_batch(f"{dim_licence}.parquet", f"{dim_licence}.csv")
#parquet_to_csv_batch(f"{dim_permissions}.parquet", f"{dim_permissions}.csv")
#parquet_to_csv_batch(f"{dim_status}.parquet", f"{dim_status}.csv")
#parquet_to_csv_batch(f"{dim_configuration}.parquet", f"{dim_configuration}.csv")
#parquet_to_csv_batch(f"{dim_provider}.parquet", f"{dim_provider}.csv")