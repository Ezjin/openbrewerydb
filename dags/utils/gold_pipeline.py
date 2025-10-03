import os
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.required_columns import require_columns


def gold_pipeline(
    silver_path: str,
    gold_path: str,
    batch_size: int = 65_536,
) -> str:
    """
    Agrega contagem por (country, state, city, brewery_type) a partir da Silver Layer (Hive-style)
    e grava um único parquet 'total.parquet' na Gold Layer.

    Args:
        silver_path: Caminho base da Silver (parquet particionado Hive).
        gold_path: Diretório de saída da Gold.
        batch_size: Número de linhas por lote ao varrer o dataset.

    Returns:
        Caminho do diretório gold_path.

    Raises:
        AirflowFailException: Em falhas de leitura/validação/escrita.
    """
    log = LoggingMixin().log
    keys = ["country", "state", "city", "brewery_type"]
    log.info("Início gold_pipeline silver=%s gold=%s batch_size=%s", silver_path, gold_path, batch_size)

    try:
        if not os.path.isdir(silver_path):
            log.warning("Silver path não existe: %s", silver_path)
            # Gera arquivo vazio com schema esperado
            os.makedirs(gold_path, exist_ok=True)
            empty = pd.DataFrame(columns=keys + ["count"])
            empty.to_parquet(os.path.join(gold_path, "total.parquet"), index=False, engine="pyarrow")
            return gold_path

        dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")

        # Acumulador incremental: MultiIndex -> count
        agg_series = None
        scanner = dataset.scanner(batch_size=batch_size)

        n_rows = 0
        n_batches = 0

        for batch in scanner.to_batches():
            n_batches += 1
            table = pa.Table.from_batches([batch])
            if table.num_rows == 0:
                continue

            df_batch = table.to_pandas()
            n_rows += len(df_batch)

            # valida chaves no primeiro batch útil
            if agg_series is None:
                require_columns(df_batch, keys, "silver_batch")

            # groupby do lote atual
            gb = (
                df_batch
                .groupby(keys, dropna=False)
                .size()
            )

            # agrega incrementalmente (soma por índice)
            if agg_series is None:
                agg_series = gb
            else:
                # alinhar índices e somar
                agg_series = agg_series.add(gb, fill_value=0)

        if agg_series is None or len(agg_series) == 0:
            log.warning("Nenhum dado agregado encontrado. (batches=%s rows=%s)", n_batches, n_rows)
            os.makedirs(gold_path, exist_ok=True)
            empty = pd.DataFrame(columns=keys + ["count"])
            empty.to_parquet(os.path.join(gold_path, "total.parquet"), index=False, engine="pyarrow")
            return gold_path

        # Finaliza dataframe ordenado
        df_count = agg_series.astype("int64").reset_index()
        df_count = df_count.rename(columns={0: "count"})
        df_count = df_count.sort_values("count", ascending=False, ignore_index=True)

        # Salva parquet
        os.makedirs(gold_path, exist_ok=True)
        filepath = os.path.join(gold_path, "total.parquet")
        df_count.to_parquet(filepath, index=False, engine="pyarrow")

        log.info(
            "Gold gravado: %s (groups=%s total_rows=%s batches=%s)",
            filepath, len(df_count), n_rows, n_batches
        )
        return gold_path

    except AirflowFailException:
        raise
    except Exception as e:
        log.exception("Erro inesperado na gold_pipeline")
        raise AirflowFailException(f"gold_pipeline falhou: {e}") from e
