import polars as pl
from utils import hash_columns

def create_dim_provedor():
    
    # 1. Loading files
    lake_path = '../raw/data_provider.csv'
    df_lake = pl.read_csv(lake_path)

    # 2. Filtering necessary columns    
    df_lake = df_lake.select(
            pl.col("Estação"),
            pl.col("Nome da Rede"),
            pl.col("Lat"),
            pl.col("Long"),
            pl.col("Alt"),
            pl.col("Localidade").alias("Localidade_raw"),
            pl.col("Estado")
        ).unique()

    # 3. Treating data
    df_dim_provider = (
        df_lake.with_columns([
            pl.when(pl.col("Localidade_raw").str.contains(","))
            .then(pl.col("Localidade_raw").str.split(by=",").list.first().str.strip_chars())
            .otherwise(None)
            .alias("localidade"),

            pl.when(pl.col("Localidade_raw").str.contains(","))
            .then(pl.col("Localidade_raw").str.split(by=",").list.slice(1).list.join(",").str.strip_chars())
            .otherwise(pl.col("Localidade_raw"))
            .alias("cidade")
        ])
    )

    df_dim_provider = (
        df_dim_provider.with_columns([
            pl.col("Estação").alias("estacao"),
            pl.col("Nome da Rede").alias("rede"),

            pl.when(pl.col("Lat") == "???")
            .then(None)
            .otherwise(pl.col("Lat").str.replace(",", "."))
            .cast(pl.Float64)
            .alias("latitude"),

            pl.when(pl.col("Long") == "???")
            .then(None)
            .otherwise(pl.col("Long").str.replace(",", "."))
            .cast(pl.Float64)
            .alias("longitude"),

            pl.when(pl.col("Alt") == "???")
            .then(None)
            .otherwise(pl.col("Alt").str.replace(",", "."))
            .cast(pl.Float64)
            .alias("altitude"),

            pl.lit("Brasil").alias("pais")
        ])
    ).drop([pl.col("Localidade_raw"), pl.col("Estação"), pl.col("Nome da Rede"), pl.col("Lat"), pl.col("Long"), pl.col("Alt")])

    # 4. Performing final filters and hashses
    df_dim_provider = df_dim_provider.with_columns(
        pl.Series(
            f"id_provedor",  
            hash_columns(
                df_dim_provider["estacao"], df_dim_provider["rede"], df_dim_provider["latitude"], df_dim_provider["longitude"],  df_dim_provider["altitude"]
            )
        )
    )

    # 5. Saving dimension as parquet file
    df_dim_provider.write_parquet(f"../dw/dim_Provedor.parquet")
    return df_dim_provider

if __name__ == '__main__':
    create_dim_provedor()