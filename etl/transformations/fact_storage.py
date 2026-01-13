import polars as pl
from dim_configuration import create_dim_configuration
from dim_date import create_dim_date 
from dim_licence import create_dim_licence
from dim_permissions import create_dim_permissions
from dim_provider import create_dim_provider
from dim_status import create_dim_status
from dim_time import create_dim_time
from dim_dataObject import create_dim_dataObject
from parquet2csv import parquet_to_csv_batch
from utils import *
from utils import clean_text
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, initcap, lower, regexp_replace, split, when, lit, row_number, substring
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from utils import clean_text

dim_date_path = "../clean_data/dim_date"
dim_time_path = "../clean_data/dim_time"
dim_licence_path = "../clean_data/dim_licence"
dim_permissions_path = "../clean_data/dim_permissions"
dim_status_path = "../clean_data/dim_status"
dim_configuration_path = "../clean_data/dim_configuration"
dim_provider_path = "../clean_data/dim_provider"


if __name__ == "__main__":

    # -------------------------
    #  LOAD DIMENSIONS
    # -------------------------
    dim_configuration = pl.DataFrame(create_dim_configuration().toPandas())
    dim_provider = pl.DataFrame(create_dim_provider().toPandas())
    dim_date = pl.DataFrame(create_dim_date().toPandas())
    dim_time = pl.DataFrame(create_dim_time().toPandas())
    dim_licence = pl.DataFrame(create_dim_licence().toPandas())
    dim_permissions = pl.DataFrame(create_dim_permissions().toPandas())
    dim_status = pl.DataFrame(create_dim_status().toPandas())
    dim_dataObject = pl.DataFrame(create_dim_dataObject().toPandas())

    dims = {
        "dim_configuration": dim_configuration,
        "dim_provider": dim_provider,
        "dim_date": dim_date,
        "dim_time": dim_time,
        "dim_licence": dim_licence,
        "dim_permissions": dim_permissions,
        "dim_status": dim_status,
        "dim_dataObject": dim_dataObject,
    }

    # -------------------------
    #  LOAD RAW DATA
    # -------------------------
    lake_path = "../raw_data/metadatalake.csv"
    provider_path = "../raw_data/data_provider.csv"

    df_lake = pl.read_csv(lake_path)
    df_provider = pl.read_csv(provider_path)

    # -------------------------
    #  JOIN WITH CONFIGURATION
    # -------------------------
    df_provider = df_provider.with_columns([
        pl.col("Sensor").map_elements(clean_text).alias("sensor_clean"),
    ])

    df_provider = df_provider.join(
        dim_configuration.select([
            "sk_configuration",
            "sensor",
            "tipo_resposta_sensor",
            "num_serie_sensor",
            "das",
            "num_serie_das"
        ]),
        left_on=[
            "sensor_clean",
            "Tipo BB SP",
            "S/N Sensor",
            "DAS",
            "S/N DAS"
        ],
        right_on=[
            "sensor",
            "tipo_resposta_sensor",
            "num_serie_sensor",
            "das",
            "num_serie_das"
        ],
        how="left"
    )

    # -------------------------
    #  JOIN COM PROVIDER E FILTRO
    # -------------------------

    df_provider = df_provider.with_columns([
        pl.col("Data Início Greg").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("data_inicio"),
        pl.col("Data Fim Greg").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("data_fim")
    ])

    df_provider = df_provider.filter(
        ~(
            pl.col("Data Fim Greg").is_null() & 
            (pl.col("Ativa") == "Não")
        )
    )

    # PARSE datetime
    df_lake = df_lake.with_columns([
        pl.col("starttime").str.strptime(pl.Datetime(time_zone="UTC"),format="%Y-%m-%d %H:%M:%S%.f%:z").alias("start_dt"),

        pl.col("endtime").str.strptime(pl.Datetime(time_zone="UTC"),format="%Y-%m-%d %H:%M:%S%.f%:z").alias("end_dt"),
    ])

    # Join entre lake e provider
    df_lake = df_lake.join(
        df_provider,
        left_on=["station", "network"],
        right_on=["Estação", "Sigla da rede"],
        how="inner"
    )

    # Filtro para pegar apenas os dados dentro do intervalo de datas
    df_lake = df_lake.filter(
        (
            (pl.col("start_dt").dt.date() >= pl.col("data_inicio")) &
            (pl.col("start_dt").dt.date() <= pl.col("data_fim"))
        ) |
        (
            (pl.col("start_dt").dt.date() >= pl.col("data_inicio")) &
            (pl.col("Ativa") == "Sim")
        )
    )


    # Correção das strings de latitude, longitude e altitude
    df_lake = df_lake.with_columns([
        pl.col("Lat").str.replace(",", ".").cast(pl.Float64),
        pl.col("Long").str.replace(",", ".").cast(pl.Float64),
        pl.col("Alt").str.replace(",", ".").cast(pl.Float64)
    ])

    df_lake = df_lake.join(
        dim_provider.select(["sk_provider", "estacao", "rede", "latitude", "longitude"]),
        left_on=["station", "network", "Lat", "Long"],
        right_on=["estacao", "rede", "latitude", "longitude"],
        how="left"
    )


    # -------------------------
    #  JOIN COM DATE E TIME
    # -------------------------
    df_lake = df_lake.with_columns([
        # Extrai a data como string (YYYY-MM-DD)
        pl.col("start_dt").cast(pl.Date).alias("start_date"),
        pl.col("end_dt").cast(pl.Date).alias("end_date"),
        
        # Extrai o horário como string (HH:MM:SS)
        pl.col("start_dt").dt.strftime("%H:%M:%S").alias("start_time"),
        pl.col("end_dt").dt.strftime("%H:%M:%S").alias("end_time"),
    ])


    # JOIN DATE
    df_lake = df_lake.join(
        dim_date.select(["sk_date", "date"]).rename({"sk_date": "sk_start_date"}),
        left_on="start_date",
        right_on="date",
        how="left"
    )

    df_lake = df_lake.join(
        dim_date.select(["sk_date", "date"]).rename({"sk_date": "sk_end_date"}),
        left_on="end_date",
        right_on="date",
        how="left"
    )

    df_lake = df_lake.join(
        dim_time.select(["sk_time", "timestamp"]).rename({"sk_time": "sk_start_time"}),
        left_on="start_time",
        right_on="timestamp",
        how="left"
    )

    df_lake = df_lake.join(
        dim_time.select(["sk_time", "timestamp"]).rename({"sk_time": "sk_end_time"}),
        left_on="end_time",
        right_on="timestamp",
        how="left"
    )

    # -------------------------
    #  JOIN COM LICENCE
    # -------------------------
    df_lake = df_lake.with_columns(
        pl.when(pl.arange(0, df_lake.height) < (df_lake.height - 50))
        .then(pl.lit("Creative Commons BY 2.0 Brasil"))
        .otherwise(pl.lit("MIT License"))
        .alias("licence_type")
    )

    df_lake = df_lake.join(
        dim_licence.select(["sk_licence", "licence_type"]),
        left_on="licence_type",
        right_on="licence_type",
        how="left"
    )

    # -------------------------
    #  JOIN COM PERMISSIONS
    # -------------------------
    df_lake = df_lake.with_columns([
        pl.when(pl.arange(0, df_lake.height) < 6000)
        .then(pl.lit("reader"))
        .when(pl.arange(0, df_lake.height) < 12000)
        .then(pl.lit("reader"))
        .otherwise(pl.lit("admin"))
        .alias("required_role"),

        pl.when(pl.arange(0, df_lake.height) < 6000)
        .then(pl.lit("no"))
        .when(pl.arange(0, df_lake.height) < 12000)
        .then(pl.lit("yes"))
        .otherwise(pl.lit("no"))
        .alias("requires_anonymization")
    ])

    df_lake = df_lake.join(
        dim_permissions.select(["sk_permissions", "required_role", "requires_anonymization"]),
        left_on=["required_role", "requires_anonymization"],
        right_on=["required_role", "requires_anonymization"],
        how="left"
    )

    # -------------------------
    #  JOIN COM STATUS
    # -------------------------
    df_lake = df_lake.with_columns(
        pl.when(pl.arange(0, df_lake.height) < (df_lake.height - 10))
        .then(pl.lit("active"))
        .otherwise(pl.lit("deprecated"))
        .alias("status")
    )

    df_lake = df_lake.join(
        dim_status.select(["sk_status", "status"]),
        left_on="status",
        right_on="status",
        how="left"
    )

    # -------------------------
    #  JOIN COM DATA OBJECT
    # -------------------------
    df_lake = df_lake.join(
        dim_dataObject.select(["sk_dataObject", "source_id"]),
        left_on="ID",
        right_on="source_id",
        how="left"
    )


    # FACT STORAGE
    fact_storage = df_lake.select([
        pl.col("sk_configuration"),
        pl.col("sk_provider"),
        pl.col("sk_start_date"),
        pl.col("sk_end_date"),
        pl.col("sk_start_time"),
        pl.col("sk_end_time"),
        pl.col("sk_licence"),
        pl.col("sk_permissions"),
        pl.col("sk_status"),
        pl.col("sk_dataObject"),
        (pl.col("npts") * 32).cast(pl.Int32).alias("size"),
        pl.col("npts").alias("number_of_points"),
        pl.col("delta"),
        pl.col("sampling_rate").alias("sampling_rate"),
        pl.col("calib").alias("calibration"),
    ])

    fact_storage.write_csv("../clean_data/fact_storage.csv")

    parquet_to_csv_batch(f"{dim_date_path}.parquet", f"{dim_date_path}.csv")
    parquet_to_csv_batch(f"{dim_time_path}.parquet", f"{dim_time_path}.csv")
    parquet_to_csv_batch(f"{dim_licence_path}.parquet", f"{dim_licence_path}.csv")
    parquet_to_csv_batch(f"{dim_permissions_path}.parquet", f"{dim_permissions_path}.csv")
    parquet_to_csv_batch(f"{dim_status_path}.parquet", f"{dim_status_path}.csv")
    parquet_to_csv_batch(f"{dim_configuration_path}.parquet", f"{dim_configuration_path}.csv")
    parquet_to_csv_batch(f"{dim_provider_path}.parquet", f"{dim_provider_path}.csv")
    parquet_to_csv_batch(f"../clean_data/dim_dataObject.parquet", f"../clean_data/dim_dataObject.csv")
