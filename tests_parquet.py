import pandas as pd

df = pd.read_parquet("./data_lake_mock/gold/batch=2025-10-02/total.parquet")


df[(df["city"].isna()) & (~df["country"].isna())]

dim_country = pd.read_parquet("./data_lake_mock/silver/dim/dim_country.parquet")

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pandas as pd
from collections import defaultdict
import pyarrow as pa


silver_path = "./data_lake_mock/silver/batch=2025-10-02/country=united_states"

# cria dataset recursivo
dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")

# Dicionário para contagem global
counts = defaultdict(int)
# Dicionário para nomes únicos já vistos por grupo
group_seen = defaultdict(set)

# Cria scanner
scanner = dataset.scanner(columns=["name", "state","city","brewery_type"], batch_size=65000)

# Processa cada batch
for batch in scanner.to_batches():
    table = pa.Table.from_batches([batch])

    # Itera pelas linhas do batch
    for record in table.to_pylist():
        key = (record["state"], record["city"], record["brewery_type"])
        name = record["name"]
        if name not in group_seen[key]:
            counts[key] += 1
            group_seen[key].add(name)

# Converte resultado final para Table
if counts:
    df = pd.DataFrame(
        [(k[0], k[1], k[2], v) for k, v in counts.items()],
        columns=["state","city","brewery_type","count"]
    ).sort_values("count", ascending=False, ignore_index=True)
else:
    df = pd.DataFrame(columns=["country","state","city","brewery_type","count"])


df[(df["state"].isna())]