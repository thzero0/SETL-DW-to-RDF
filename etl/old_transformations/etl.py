import polars as pl
import dim_configuracao, dim_datamedicao, dim_datacoleta, dim_licenca, dim_permissoes, dim_provedor, dim_status, dim_tempocoleta, dim_tempomedicao
from utils import *

if __name__ == '__main__':
    
    dim_configuracao = dim_configuracao.create_dim_configuracao()

    dim_provedor = dim_provedor.create_dim_provedor()

    dim_datainiciomedicao = dim_datamedicao.create_dim_datamedicao("start")
    dim_datafimmedicao = dim_datamedicao.create_dim_datamedicao("end")

    dim_tempoiniciomedicao = dim_tempomedicao.create_dim_tempomedicao("start")
    dim_tempofimmedicao = dim_tempomedicao.create_dim_tempomedicao("end")

    dim_tempocoleta = dim_tempocoleta.create_dim_tempocoleta()
    
    dim_datacoleta = dim_datacoleta.create_dim_datacoleta()

    dim_status = dim_status.create_dim_status()
    
    dim_licenca = dim_licenca.create_dim_licenca()
    
    dim_permissoes = dim_permissoes.create_dim_permissoes()
    
    dims = {
        "dim_configuracao": dim_configuracao,
        "dim_provedor": dim_provedor,
        "dim_datainiciomedicao": dim_datainiciomedicao,
        "dim_datafimmedicao": dim_datafimmedicao,
        "dim_tempoiniciomedicao": dim_tempoiniciomedicao,
        "dim_tempofimmedicao": dim_tempofimmedicao,
        "dim_tempocoleta": dim_tempocoleta,
        "dim_datacoleta": dim_datacoleta,
        "dim_status": dim_status,
        "dim_licenca": dim_licenca,
        "dim_permissoes": dim_permissoes,
    }

    # Loop para checar colunas com valores nulos e contar quantos
    for name, df in dims.items():
        null_info = {}
        for col in df.columns:
            null_count = df.select(pl.col(col).is_null().sum()).item()
            if null_count > 0:
                null_info[col] = null_count

        if null_info:
            print(f"\n{name} possui valores nulos:")
            for col, count in null_info.items():
                print(f"  - {col}: {count} valores nulos")
        else:
            print(f"\n{name} não possui valores nulos.")


    # Creating fact table
    lake_path = '../raw/metadatalake.csv'
    provider_path = '../raw/data_provider.csv'
    df_lake = pl.read_csv(lake_path)
    df_provider = pl.read_csv(provider_path)

    df_lake = df_lake.with_columns([
        pl.col("starttime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z").alias("starttime_dt"),
        pl.col("endtime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z").alias("endtime_dt")
    ])
    df_lake = df_lake.with_columns([
        pl.col("starttime_dt").cast(pl.Date).alias("start_date"),
        pl.col("endtime_dt").cast(pl.Date).alias("end_date")
    ])

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

    df_merged = df_lake.join(
        df_provider,
        left_on=["station", "network"],
        right_on=["Estação", "Sigla da rede"],
        how="inner"
    ).filter(
        (
            (pl.col("starttime_dt").dt.date() >= pl.col("data_inicio")) &
            (pl.col("starttime_dt").dt.date() <= pl.col("data_fim"))
        ) |
        (
            (pl.col("starttime_dt").dt.date() >= pl.col("data_inicio")) &
            (pl.col("Ativa") == "Sim")
        )
    )

    df_merged = (
        df_merged.with_columns([
            pl.when(pl.col("Lat") == "???").then(None)
            .otherwise(pl.col("Lat").str.replace(",", ".")).cast(pl.Float64).alias("latitude"),

            pl.when(pl.col("Long") == "???").then(None)
            .otherwise(pl.col("Long").str.replace(",", ".")).cast(pl.Float64).alias("longitude"),

            pl.when(pl.col("Alt") == "???").then(None)
            .otherwise(pl.col("Alt").str.replace(",", ".")).cast(pl.Float64).alias("altitude")
        ])
    )

    # Adicionando coluna de status
    df_merged = df_merged.with_columns(
        pl.when(pl.arange(0, df_merged.height) < (df_merged.height - 10))
        .then(pl.lit("ativo"))
        .otherwise(pl.lit("apagado"))
        .alias("status")
    )

    # Adicionando coluna de licenca
    df_merged = df_merged.with_columns(
        pl.when(pl.arange(0, df_merged.height) < (df_merged.height - 50))
        .then(pl.lit("Creative Commons BY 2.0 Brasil"))
        .otherwise(pl.lit("MIT License"))
        .alias("tipo_licenca")
    )

    # Adicionando coluna de permissao
    df_merged = df_merged.with_columns([
        pl.when(pl.arange(0, df_merged.height) < 6000)
        .then(pl.lit("leitor"))
        .when(pl.arange(0, df_merged.height) < 12000)
        .then(pl.lit("leitor"))
        .otherwise(pl.lit("admin"))
        .alias("papel_requerido"),

        pl.when(pl.arange(0, df_merged.height) < 6000)
        .then(pl.lit("nao"))
        .when(pl.arange(0, df_merged.height) < 12000)
        .then(pl.lit("sim"))
        .otherwise(pl.lit("nao"))
        .alias("anonimizacao_necessaria")
    ])

    # Criando a coluna data_coleta
    df_merged = df_merged.with_columns(
        pl.when(pl.arange(0, df_merged.height) < 6000)
        .then(pl.lit("2025-08-10").str.strptime(pl.Date, "%Y-%m-%d"))
        .when(pl.arange(0, df_merged.height) < 12000)
        .then(pl.lit("2025-08-15").str.strptime(pl.Date, "%Y-%m-%d"))
        .otherwise(pl.lit("2025-07-01").str.strptime(pl.Date, "%Y-%m-%d"))
        .alias("data_coleta")
    )

    # Criando a coluna tempo_coleta
    df_merged = df_merged.with_columns(
        pl.when(pl.arange(0, df_merged.height) < 6000)
        .then(pl.lit("2025-08-10 23:59:59.995000+00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z"))
        .when(pl.arange(0, df_merged.height) < 12000)
        .then(pl.lit("2025-08-15 12:30:33.995000+00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z"))
        .otherwise(pl.lit("2025-07-01 20:50:00.995000+00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f%z"))
        .alias("tempo_coleta")
    )

    print(df_merged)

    # Getting fks
    df_merged = df_merged.with_columns(
        pl.Series(f"id_tempo_inicio_medicao", hash_columns(df_merged["starttime_dt"])),
        pl.Series(f"id_tempo_fim_medicao", hash_columns(df_merged["endtime_dt"])),
        pl.Series(f"id_data_inicio_medicao", hash_columns(df_merged["start_date"])),
        pl.Series(f"id_data_fim_medicao", hash_columns(df_merged["end_date"])),
        pl.Series(
            f"id_provedor",  
            hash_columns(
                df_merged["station"], df_merged["Nome da Rede"], df_merged["latitude"], 
                df_merged["longitude"],  df_merged["altitude"]
            )
        ),
        pl.Series(
            f"id_configuracao",  
            hash_columns(
                df_merged["station"], df_merged["Nome da Rede"], df_merged["latitude"], 
                df_merged["longitude"],  df_merged["altitude"], df_merged["data_inicio"], df_merged["data_fim"]
            )
        ),
        pl.Series(f"id_status", hash_columns(df_merged["status"])),
        pl.Series(f"id_licenca", hash_columns(df_merged["tipo_licenca"])),
        pl.Series(f"id_permissoes", hash_columns(df_merged["papel_requerido"], df_merged["anonimizacao_necessaria"])),
        pl.Series(f"id_data_coleta", hash_columns(df_merged["data_coleta"])),
        pl.Series(f"id_tempo_coleta", hash_columns(df_merged["tempo_coleta"]))
    )

    # Performing final selects
    fact_measurement = df_merged.select(
        pl.col("id_tempo_inicio_medicao"),
        pl.col("id_tempo_fim_medicao"),
        pl.col("id_data_inicio_medicao"),
        pl.col("id_data_fim_medicao"),
        pl.col("id_provedor"),
        pl.col("id_configuracao"),
        pl.col("id_status"),
        pl.col("id_licenca"),
        pl.col("id_permissoes"),
        pl.col("id_data_coleta"),
        pl.col("id_tempo_coleta"),
        (pl.col("npts") * 32).cast(pl.Int32).alias("tamanho"),
        pl.col("npts").alias("numero_de_pontos"),
        pl.col("delta"),
        pl.col("sampling_rate").alias("taxa_de_amostragem"),
        pl.col("calib").alias("calibracao"),
        pl.col("filename").alias("id_objeto_dados")
    )

    print(fact_measurement)

    fact_measurement.write_parquet("../dw/fact_Medicao.parquet")