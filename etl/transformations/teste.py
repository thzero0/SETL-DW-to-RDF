import polars as pl
from dim_configuration import create_dim_configuration
from dim_date import create_dim_date 
from dim_licence import create_dim_licence
from dim_permissions import create_dim_permissions
from dim_provider import create_dim_provider
from dim_status import create_dim_status
from dim_time import create_dim_time
from utils import *

if __name__ == "__main__":

    # -------------------------
    #  LOAD DIMENSIONS
    # -------------------------
    dim_configuration = create_dim_configuration()
    dim_provider = create_dim_provider()
    dim_date = create_dim_date()
    dim_time = create_dim_time()
    dim_licence = create_dim_licence()
    dim_permissions = create_dim_permissions()
    dim_status = create_dim_status()

    dims = {
        "dim_configuration": dim_configuration,
        "dim_provider": dim_provider,
        "dim_date": dim_date,
        "dim_time": dim_time,
        "dim_licence": dim_licence,
        "dim_permissions": dim_permissions,
        "dim_status": dim_status,
    }

    # -------------------------
    #  NULL VALIDATION
    # -------------------------
    for name, df in dims.items():
        nulls = {col: df[col].is_null().sum() for col in df.columns if df[col].is_null().sum() > 0}
        print(f"\n{name} null values:", nulls if nulls else "No nulls")

    # -------------------------
    #  LOAD RAW DATA
    # -------------------------
    lake_path = "../raw_data/metadatalake.csv"
    provider_path = "../raw_data/data_provider.csv"

    df_lake = pl.read_csv(lake_path)
    df_provider = pl.read_csv(provider_path)

    # -------------------------
    #  JOIN RAW DATA
    # -------------------------
    df_raw = df_lake.join(
        df_provider,
        left_on="provider_id",
        right_on="provider_id",
        how="left"
    )

    # -------------------------
    #  CREATE FACT TABLE
    # -------------------------
    fact = df_raw.select([
        "provider_id",
        "configuration",
        "date",
        "time",
        "licence",
        "permissions",
        "status",
    ])

    # -------------------------
    #  JOIN WITH DIMENSIONS
    # -------------------------
    fact = (
        fact
        # Join com dim_provider
        .join(dim_provider, on="provider_id", how="left")
        
        # Join com dim_configuration
        .join(dim_configuration, on="configuration", how="left")

        # Join com dim_date
        .join(dim_date, on="date", how="left")

        # Join com dim_time
        .join(dim_time, on="time", how="left")

        # Join com dim_licence
        .join(dim_licence, on="licence", how="left")

        # Join com dim_permissions
        .join(dim_permissions, on="permissions", how="left")

        # Join com dim_status
        .join(dim_status, on="status", how="left")
    )

    # -------------------------
    #  FINAL NULL CHECK
    # -------------------------
    fact_nulls = {
        col: fact[col].is_null().sum()
        for col in fact.columns
        if fact[col].is_null().sum() > 0
    }

    print("\nFact table null values:", fact_nulls if fact_nulls else "No nulls")

    # -------------------------
    #  SAVE OUTPUT
    # -------------------------
    fact.write_csv("../warehouse/fact_table.csv")

    print("\nETL Finalizado com sucesso!")
