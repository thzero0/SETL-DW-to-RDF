from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as sf

output_path = "../clean_data/dim_licence.parquet"

def create_dim_licence():
    spark = SparkSession.builder.getOrCreate()

    # -----------------------------
    # 1. Criando dummies
    # -----------------------------
    dummies = [
        {
            "licence_type": "Creative Commons BY 2.0 Brasil",
            "description": "Permite copiar, distribuir, exibir e executar a obra e criar trabalhos derivados, desde que seja dado credito ao autor original.",
            "access_link": "https://creativecommons.org/licenses/by/2.0/br/"
        },
        {
            "licence_type": "MIT License",
            "description": "Permite uso, copia, modificacao, fusao, publicacao, distribuicao, sublicenciamento e/ou venda do software, desde que mantido aviso.",
            "access_link": "https://opensource.org/licenses/MIT"
        }
    ]

    df_licenca = spark.createDataFrame(dummies)

    # -----------------------------
    # 2. Gerando Surrogate Key (SK)
    # -----------------------------
    window = Window.orderBy("licence_type")

    df_licenca = df_licenca.withColumn(
        "sk_licence",
        row_number().over(window)
    )

    # -----------------------------
    # 3. Reordenar colunas (opcional)
    # -----------------------------
    df_licenca = df_licenca.select(
        "sk_licence", "licence_type", "description", "access_link"
    )

    # -----------------------------
    # 4. Salvar em Parquet
    # -----------------------------
    df_licenca.write.parquet(output_path, mode="overwrite")

    return df_licenca

if __name__ == "__main__":
    create_dim_licence()
