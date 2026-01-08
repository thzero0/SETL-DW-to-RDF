import polars as pl
from utils import hash_columns

def create_dim_datacoleta():
    
    dummies = ['2025-08-10 23:59:59.995000+00:00', '2025-08-15 12:30:33.995000+00:00', '2025-07-01 20:50:00.995000+00:00']

    df_data_coleta = pl.DataFrame({
        "timestamp_str": dummies
    })

    # 3. Treating data
    df_data_coleta = (
        df_data_coleta.with_columns([pl.col("timestamp_str").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z").alias("timestamp_str")])
            .with_columns([
                pl.col("timestamp_str").cast(pl.Date).alias("data"),
                pl.col("timestamp_str").dt.day().alias("dia"),
                pl.col("timestamp_str").dt.month().alias("mes"),
                pl.col("timestamp_str").dt.year().alias("ano")
            ])
    )

    # 4. Performing final filters and hashses
    df_data_coleta = df_data_coleta.with_columns(
        pl.Series(
            f"id_data_coleta",  
            hash_columns(
                df_data_coleta["data"]
            )
        )
    ).drop([pl.col("timestamp_str")])

    # 5. Saving dimension as parquet file
    df_data_coleta.write_parquet(f"../dw/dim_DataColeta.parquet")
    return df_data_coleta

if __name__ == '__main__':
    create_dim_datacoleta()