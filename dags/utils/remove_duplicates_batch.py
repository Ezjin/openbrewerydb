import os
import pyarrow.dataset as ds
import pyarrow as pa

def remove_duplicates_batch(date: str, silver_path_fact: str, log):
    """
    Deduplica apenas o batch informado.
    Mantém a versão mais completa de cada registro com base em identity_cols.
    """

    batch_path = os.path.join(silver_path_fact, f"batch={date}")
    log.info(f"Iniciando deduplicação no batch {date}: {batch_path}")

    dataset = ds.dataset(batch_path, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()

    if df.empty:
        log.warning(f"Nenhum dado encontrado para o batch {date}.")
        return

    identity_cols = ["name", "country", "state", "city", "brewery_type"]

    # Completude = número de colunas não nulas fora das chaves de identidade
    cols_to_check = [c for c in df.columns if c not in identity_cols]
    df["completeness"] = df[cols_to_check].notna().sum(axis=1)

    df = (df
          .sort_values(by = identity_cols + ["completeness"],
                       ascending = [True] * len(identity_cols) + [False])
          .drop_duplicates(subset = identity_cols, keep = "first")
          .drop(columns = ["completeness"])
    )

    # Regrava sobrescrevendo só o batch atual

    ds.write_dataset(
        pa.Table.from_pandas(df),
        base_dir=batch_path,                  
        format="parquet",                     
        partitioning=["country", "state", "part"],  
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore"
    )


    log.info(f"Deduplicação concluída para batch {date}: {len(df)} registros finais.")
