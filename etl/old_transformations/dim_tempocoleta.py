import polars as pl
from utils import hash_columns

def create_dim_tempocoleta():
    
    dummies = ['2025-08-10 23:59:59.995000+00:00', '2025-08-15 12:30:33.995000+00:00', '2025-07-01 20:50:00.995000+00:00']

    # 4. Performing final filters and hashses
    df_tempo_coleta = pl.DataFrame({
        "timestamp_str": dummies
    })

    df_tempo_coleta = (
        df_tempo_coleta.with_columns([pl.col("timestamp_str").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z").alias("timestamp_str")])
            .with_columns([
                pl.col("timestamp_str").alias("timestamp"),
                pl.col("timestamp_str").dt.hour().alias("hora"),
                pl.col("timestamp_str").dt.minute().alias("minuto"),
                pl.col("timestamp_str").dt.second().alias("segundo")
            ])
    )

    # 4. Performing final filters and hashses
    df_tempo_coleta = df_tempo_coleta.with_columns(
        pl.Series(
            f"id_tempo_coleta",  
            hash_columns(
                df_tempo_coleta["timestamp"]
            )
        )
    ).drop([pl.col("timestamp_str")])

    # 5. Saving dimension as parquet file
    df_tempo_coleta.write_parquet(f"../dw/dim_TempoColeta.parquet")
    return df_tempo_coleta

if __name__ == '__main__':
    create_dim_tempocoleta()