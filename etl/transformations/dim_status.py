from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as sf

output_path = "../clean_data/dim_status.parquet"

def create_dim_status():
    spark = SparkSession.builder.getOrCreate()

    # -----------------------------
    # 1. Criando os dummies
    # -----------------------------
    dummies = [
        {"status": "active"},
        {"status": "deprecated"},
    ]

    df_status = spark.createDataFrame(dummies)

    # -----------------------------
    # 2. Surrogate Key (SK)
    # -----------------------------
    window = Window.orderBy("status")

    df_status = df_status.withColumn(
        "sk_status",
        row_number().over(window)
    )

    # -----------------------------
    # 3. Reordenar colunas
    # -----------------------------
    df_status = df_status.select(
        "sk_status",
        "status"
    )

    # -----------------------------
    # 4. Salvar parquet
    # -----------------------------
    df_status.write.parquet(output_path, mode="overwrite")

    return df_status

if __name__ == "__main__":
    create_dim_status()
