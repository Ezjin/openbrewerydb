import pyarrow.dataset as ds
import pyarrow as pa
import os
import pandas as pd

def gold_pipeline(
    silver_path: str,
    gold_path: str,
    log,
    batch_size: int = 65536
):
    """
    Lê o dataset Hive-style da Silver Layer, agrega a quantidade de registros por 
    (country, state, city, brewery_type) e salva parquet agregado na Gold Layer.

    Args:
        silver_path: caminho da Silver Layer (Hive-style parquet)
        gold_path: caminho do parquet final da Gold Layer
        batch_size: número de linhas por batch
    """

    dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")

    # Lista para acumular dataframes
    dfs = []

    # Cria scanner
    scanner = dataset.scanner(batch_size=batch_size)

    # Processa cada batch
    for batch in scanner.to_batches():
        table = pa.Table.from_batches([batch])
        df_batch = table.to_pandas()
        dfs.append(df_batch)

    # Concatena todos os batches
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["country","state","city","brewery_type"])

    # Agrega contagem por grupo
    if not df.empty:
        df_count = df.groupby(["country","state","city","brewery_type"], dropna=False).size().reset_index(name="count")
        df_count = df_count.sort_values("count", ascending=False, ignore_index=True)
    else:
        df_count = pd.DataFrame(columns=["country","state","city","brewery_type","count"])

    # Salva parquet
    os.makedirs(gold_path, exist_ok=True)
    filepath = os.path.join(gold_path, "total.parquet")
    df_count.to_parquet(filepath, index=False, engine="pyarrow")

    log.info(f"Gold dataset salvo em {filepath}")
    return gold_path
