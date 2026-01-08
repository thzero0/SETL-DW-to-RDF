import polars as pl
from utils import hash_columns

def create_dim_licenca():
    
    dummies = {
        "tipo": [
            "Creative Commons BY 2.0 Brasil",
            "MIT License"
        ],
        "descricao": [
            "Permite copiar, distribuir, exibir e executar a obra e criar trabalhos derivados, desde que seja dado crédito ao autor original.",
            "Permite uso, cópia, modificação, fusão, publicação, distribuição, sublicenciamento e/ou venda do software, desde que mantido aviso."
        ],
        "link_acesso": [
            "https://creativecommons.org/licenses/by/2.0/br/",
            "https://opensource.org/licenses/MIT"
        ]
    }

    # 4. Performing final filters and hashses
    df_licenca = pl.DataFrame(dummies)

    # 4. Performing final filters and hashses
    df_licenca = df_licenca.with_columns(
        pl.Series(
            f"id_licenca",  
            hash_columns(
                df_licenca["tipo"]
            )
        )
    )

    # 5. Saving dimension as parquet file
    df_licenca.write_parquet(f"../dw/dim_Licenca.parquet")
    return df_licenca

if __name__ == '__main__':
    create_dim_licenca()