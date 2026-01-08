import polars as pl
from utils import hash_columns

def create_dim_permissoes():
    
    dummies = {
        "papel_requerido": [
            "leitor",
            "admin",
            "leitor"
        ],
        "anonimizacao_necessaria": [
            "sim",
            "nao",
            "nao"
        ]
    }

    # 4. Performing final filters and hashses
    df_permissoes = pl.DataFrame(dummies)

    # 4. Performing final filters and hashses
    df_permissoes = df_permissoes.with_columns(
        pl.Series(
            f"id_permissoes",  
            hash_columns(
                df_permissoes["papel_requerido"], df_permissoes["anonimizacao_necessaria"]
            )
        )
    )

    # 5. Saving dimension as parquet file
    df_permissoes.write_parquet(f"../dw/dim_Permissoes.parquet")
    return df_permissoes

if __name__ == '__main__':
    create_dim_permissoes()