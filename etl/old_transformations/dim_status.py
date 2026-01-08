import polars as pl
from utils import hash_columns

def create_dim_status():
    
    dummies = ['ativo', 'apagado']

    # 4. Performing final filters and hashses
    df_status = pl.DataFrame({
        "status": dummies
    })

    # 4. Performing final filters and hashses
    df_status = df_status.with_columns(
        pl.Series(
            f"id_status",  
            hash_columns(
                df_status["status"]
            )
        )
    )

    # 5. Saving dimension as parquet file
    df_status.write_parquet(f"../dw/dim_Status.parquet")
    return df_status

if __name__ == '__main__':
    create_dim_status()