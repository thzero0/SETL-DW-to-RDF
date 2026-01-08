import polars as pl
from utils import hash_columns

def create_dim_datamedicao(flag):
    
    # 1. Loading files
    desired_col = None
    dim = None
    lake_path = '../raw/metadatalake.csv'
    df_lake = pl.read_csv(lake_path)

    # 2. Filtering necessary columns
    if flag == 'start':
        desired_col = "starttime"
        dim = "Inicio"
    elif flag == 'end':
        desired_col = "endtime"
        dim = "Fim"
    else:
        print("Invalid flag")
        return None
    
    df_lake = df_lake.select(
            pl.col(desired_col)
        ).unique()

    # 3. Treating data
    df_dim_data = (
        df_lake.with_columns([pl.col(desired_col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z").alias(f"{desired_col}_dt")])
            .with_columns([
                pl.col(f"{desired_col}_dt").cast(pl.Date).alias("data"),
                pl.col(f"{desired_col}_dt").dt.day().alias("dia"),
                pl.col(f"{desired_col}_dt").dt.month().alias("mes"),
                pl.col(f"{desired_col}_dt").dt.year().alias("ano")
            ])
    ).drop([pl.col(desired_col), pl.col(f"{desired_col}_dt")]).unique()

    # 4. Performing final filters and hashses
    df_dim_data = df_dim_data.with_columns(
        pl.Series(
            f"id_data_{dim.lower()}_medicao",  
            hash_columns(
                df_dim_data["data"]
            )
        )
    )

    # 5. Saving dimension as parquet file
    df_dim_data.write_parquet(f"../dw/dim_Data{dim}Medicao.parquet")
    return df_dim_data

if __name__ == '__main__':
    flag = "end"
    create_dim_datamedicao(flag)