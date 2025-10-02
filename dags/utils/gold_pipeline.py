import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.compute as pc
from collections import defaultdict
import pyarrow.parquet as pq
import os
import pandas as pd

def gold_pipeline(
    silver_path: str,
    gold_path: str,
    log,
    batch_size: int = 65536,
    dedupe_subset: list = ["name","country","state","city","brewery_type"]
):
    """
    Lê o dataset Hive-style da Silver Layer, deduplica nomes globalmente por grupo 
    (country, state, city, brewery_type) e salva parquet agregado na Gold Layer.

    Args:
        silver_path: caminho da Silver Layer (Hive-style parquet)
        gold_path: caminho do parquet final da Gold Layer
        batch_size: número de linhas por batch
        dedupe_subset: colunas usadas para deduplicação
    """

    # Inicializa o dataset Hive-style
    dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")

    # Dicionário para contagem global
    counts = defaultdict(int)
    # Dicionário para nomes únicos já vistos por grupo
    group_seen = defaultdict(set)

    # Cria scanner
    scanner = dataset.scanner(columns=dedupe_subset, batch_size=batch_size)

    # Processa cada batch
    for batch in scanner.to_batches():
        table = pa.Table.from_batches([batch])

        # Itera pelas linhas do batch
        for record in table.to_pylist():
            key = (record["country"], record["state"], record["city"], record["brewery_type"])
            name = record["name"]
            if name not in group_seen[key]:
                counts[key] += 1
                group_seen[key].add(name)

    # Converte resultado final para Table
    if counts:
        df = pd.DataFrame(
            [(k[0], k[1], k[2], k[3], v) for k, v in counts.items()],
            columns=["country","state","city","brewery_type","count"]
        ).sort_values("count", ascending=False, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["country","state","city","brewery_type","count"])

    # Garante que diretório existe
    os.makedirs(gold_path, exist_ok=True)

    # Salva parquet
    filepath = os.path.join(gold_path, "total.parquet")
    df.to_parquet(filepath, index=False, engine="pyarrow")

    return gold_path
