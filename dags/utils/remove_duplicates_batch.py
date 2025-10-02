import os
from typing import Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin


def _require_columns(df: pd.DataFrame, cols: Sequence[str], ctx: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AirflowFailException(f"Colunas ausentes em {ctx}: {missing}")


def remove_duplicates_batch(date: str, silver_path_fact: str) -> None:
    """
    Deduplica apenas o batch informado (batch=<date>) dentro de `silver_path_fact`.
    Mantém a versão mais completa de cada registro com base em `identity_cols`,
    medindo completude pelo número de campos não nulos fora das chaves.

    Args:
        date: Identificador do batch (ex.: '2025-09-27').
        silver_path_fact: Diretório base da fato silver (particionado Hive).

    Raises:
        AirflowFailException: Em falhas de leitura, validação ou escrita.
    """
    log = LoggingMixin().log
    batch_path = os.path.join(silver_path_fact, f"batch={date}")
    log.info("Deduplicação do batch=%s em %s", date, batch_path)

    try:
        if not os.path.isdir(batch_path):
            log.warning("Path do batch não existe: %s", batch_path)
            return

        dataset = ds.dataset(batch_path, format="parquet", partitioning="hive")
        table = dataset.to_table()  # todo o batch
        if table.num_rows == 0:
            log.warning("Nenhum dado encontrado para batch=%s", date)
            return

        df = table.to_pandas()
        if df.empty:
            log.warning("DataFrame vazio após conversão; batch=%s", date)
            return

        identity_cols = ["name", "country", "state", "city", "brewery_type"]
        _require_columns(df, identity_cols, f"batch={date}")

        # Colunas não-chave para medir completude
        cols_to_check = [c for c in df.columns if c not in identity_cols]
        if not cols_to_check:
            log.warning("Sem colunas não-chave para medir completude; apenas drop_duplicates.")
            df_sorted = df.drop_duplicates(subset=identity_cols, keep="first")
        else:
            # Completude = contagem de não-nulos nas não-chaves
            df = df.copy()
            df["__completeness__"] = df[cols_to_check].notna().sum(axis=1)
            df_sorted = (
                df.sort_values(by=identity_cols + ["__completeness__"],
                               ascending=[True] * len(identity_cols) + [False])
                  .drop_duplicates(subset=identity_cols, keep="first")
                  .drop(columns="__completeness__")
            )

        n_before = len(df)
        n_after = len(df_sorted)
        log.info("Dedup batch=%s: antes=%s depois=%s removidos=%s",
                 date, n_before, n_after, n_before - n_after)

        # Escrita: regrava SOMENTE o batch atual
        # Observação: `overwrite_or_ignore` não remove arquivos antigos conflitantes.
        # Para garantir limpeza total, removemos o diretório do batch antes.
        try:
            # Limpa diretório do batch para evitar coexistência de arquivos antigos
            for root, dirs, files in os.walk(batch_path, topdown=False):
                for f in files:
                    os.remove(os.path.join(root, f))
                for d in dirs:
                    os.rmdir(os.path.join(root, d))
        except Exception:
            log.exception("Falha ao limpar diretório do batch antes da escrita: %s", batch_path)
            raise AirflowFailException("Não foi possível limpar o diretório do batch antes da escrita.")

        ds.write_dataset(
            data=pa.Table.from_pandas(df_sorted, preserve_index=False),
            base_dir=batch_path,                      # escreve dentro de batch=<date>
            format="parquet",
            partitioning=["country", "state", "part"],  # mantém a hierarquia sob o batch
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
        )

        log.info("Deduplicação concluída para batch=%s; registros finais=%s", date, n_after)

    except AirflowFailException:
        raise
    except Exception as e:
        log.exception("Erro inesperado na deduplicação do batch=%s", date)
        raise AirflowFailException(f"remove_duplicates_batch falhou: {e}") from e
