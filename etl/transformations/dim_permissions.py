from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as sf

output_path = "../clean_data/dim_permissions.parquet"

def create_dim_permissions():
    spark = SparkSession.builder.getOrCreate()

    # -----------------------------
    # 1. Criando os dummies
    # -----------------------------
    dummies = [
        {
            "required_role": "reader",
            "requires_anonymization": "yes"
        },
        {
            "required_role": "admin",
            "requires_anonymization": "no"
        },
        {
            "required_role": "reader",
            "requires_anonymization": "no"
        }
    ]

    df_permissoes = spark.createDataFrame(dummies)

    # -----------------------------
    # 2. Remover duplicatas
    # -----------------------------
    df_permissoes = df_permissoes.dropDuplicates()

    # -----------------------------
    # 3. Criar Surrogate Key (SK)
    # -----------------------------
    window = Window.orderBy("required_role", "requires_anonymization")

    df_permissoes = df_permissoes.withColumn(
        "sk_permissions",
        row_number().over(window)
    )

    # -----------------------------
    # 4. Reordenar colunas
    # -----------------------------
    df_permissoes = df_permissoes.select(
        "sk_permissions",
        "required_role",
        "requires_anonymization"
    )

    # -----------------------------
    # 5. Salvar parquet
    # -----------------------------
    df_permissoes.write.parquet(output_path, mode="overwrite")

    return df_permissoes

if __name__ == "__main__":
    create_dim_permissions()
