from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa

from collections import defaultdict

silver_path = "data_lake_mock/silver/"

# Lê o dataset Hive-style
dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")

# Inicializa um dicionário para contar
counts = defaultdict(int)

# Cria um scanner que lê em batches (padrão 64k linhas por batch)
scanner = dataset.scanner(batch_size=65536)

for batch in scanner.to_batches():
    # Converte para Table
    table = pa.Table.from_batches([batch])
    
    # Converte para Pandas apenas o batch
    df_batch = table.to_pandas()
    
    # Remove duplicados no batch
    df_batch = df_batch.drop_duplicates(subset=["name", "country", "state", "city", "brewery_type"])
    
    # Conta agregando no dicionário
    for row in df_batch.itertuples(index=False):
        key = (row.country, row.state, row.city, row.brewery_type)
        counts[key] += 1

# Converte resultado final para DataFrame
import pandas as pd
result = pd.DataFrame(
    [(k[0], k[1], k[2], k[3], v) for k, v in counts.items()],
    columns=["country", "state", "city", "brewery_type", "count"]
)

print(result)

result.sort_values("count", ascending=False)
