import polars as pl
from utils import hash_columns

def create_dim_configuracao():
    
    # 1. Loading files
    lake_path = '../raw/data_provider.csv'
    df_lake = pl.read_csv(lake_path)

    # 2. Filtering necessary columns    
    df_lake = df_lake.select(
            pl.col("Estação").alias("estacao"),
            pl.col("Nome da Rede").alias("rede"),
            pl.col("Lat"),
            pl.col("Long"),
            pl.col("Alt"),
            pl.col("Sensor").alias("sensor"),
            pl.col("Tipo BB SP").alias("tipo_resposta_sensor"),
            pl.col("S/N Sensor").alias("num_serie_sensor"),
            pl.col("DAS").alias("das"),
            pl.col("S/N DAS").alias("num_serie_das"),
            pl.col("Data Início Greg").alias("data_inicio"),
            pl.col("Data Fim Greg").alias("data_fim"),
            pl.col("Ativa").alias("ativa")
        ).unique()

    # 3. Treating data
    df_dim_configuracao = (
        df_lake.with_columns([
            pl.col("data_inicio").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"),
            pl.col("data_fim").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"),
        ])
    )

    df_dim_configuracao = (
        df_dim_configuracao.with_columns([
            pl.when(pl.col("Lat") == "???").then(None)
            .otherwise(pl.col("Lat").str.replace(",", ".")).cast(pl.Float64).alias("latitude"),

            pl.when(pl.col("Long") == "???").then(None)
            .otherwise(pl.col("Long").str.replace(",", ".")).cast(pl.Float64).alias("longitude"),

            pl.when(pl.col("Alt") == "???").then(None)
            .otherwise(pl.col("Alt").str.replace(",", ".")).cast(pl.Float64).alias("altitude"),
        ])
    )


    # 4. Performing final filters and hashses
    df_dim_configuracao = df_dim_configuracao.with_columns(
        pl.Series(
            f"id_configuracao",  
            hash_columns(
                df_dim_configuracao["estacao"], df_dim_configuracao["rede"], df_dim_configuracao["latitude"], df_dim_configuracao["longitude"], 
                df_dim_configuracao["altitude"], df_dim_configuracao["data_inicio"], df_dim_configuracao["data_fim"]
            )
        ),
        pl.Series(
            f"id_provedor",  
            hash_columns(
                df_dim_configuracao["estacao"], df_dim_configuracao["rede"], df_dim_configuracao["longitude"], 
                df_dim_configuracao["altitude"]
            )
        )
    ).drop(pl.col("Lat"), pl.col("Long"), pl.col("Alt"), pl.col("latitude"), pl.col("longitude"), pl.col("altitude"))

    # 5. Saving dimension as parquet file
    df_dim_configuracao.write_parquet(f"../dw/dim_Configuracao.parquet")
    return df_dim_configuracao

if __name__ == '__main__':
    create_dim_configuracao()