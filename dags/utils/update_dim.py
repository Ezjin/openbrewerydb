import pandas as pd
from pathlib import Path
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Optional, Callable

def update_dim(
        df: pd.DataFrame,
        original_col: str,
        normalized_col: Optional[str],
        filepath: str,
        normalizer: Optional[Callable[[str], str]] = None) -> None:
    """
    Atualiza uma dimensão parquet 'de-para' (original -> normalizado).
    Mantém um arquivo com DUAS colunas: original_col e f"{original_col}_norm".

    Regras:
      - Se `normalized_col` não for informado, usa `normalizer(original)`.
      - Dedup por `original_col`; se houver conflito de mapeamento, o valor novo vence.

    Args:
        df: DataFrame contendo a coluna original (e opcionalmente a normalizada).
        original_col: Nome da coluna original (ex.: "country").
        normalized_col: Nome da coluna normalizada (ex.: "country_norm"). Pode ser None.
        filepath: Caminho do parquet da dimensão (persistido como mapping).
        normalizer: Função para normalizar (ex.: normalize_name). Obrigatória se normalized_col=None.
    """
    log = LoggingMixin().log

    if df is None or df.empty:
        raise AirflowFailException("update_dim: df vazio.")

    if original_col not in df.columns:
        raise AirflowFailException(f"update_dim: coluna '{original_col}' ausente.")

    target_norm_col = f"{original_col}_norm"

    work = df[[original_col]].copy()
    work[original_col] = work[original_col].astype("string")

    # obter coluna normalizada
    if normalized_col and normalized_col in df.columns:
        work[target_norm_col] = df[normalized_col].astype("string")
    else:
        if normalizer is None:
            raise AirflowFailException("update_dim: informe `normalized_col` ou `normalizer`.")
        work[target_norm_col] = work[original_col].apply(lambda x: None if pd.isna(x) else normalizer(str(x)))

    # limpar
    work = work.dropna(subset=[original_col, target_norm_col]).drop_duplicates(subset=[original_col])

    # carregar existente (se houver)
    p = Path(filepath)
    if p.exists():
        old = pd.read_parquet(p)
        # garantir mesmas colunas
        missing = [c for c in [original_col, target_norm_col] if c not in old.columns]
        if missing:
            log.warning("update_dim: arquivo existente sem colunas %s; será reescrito.", missing)
            combined = work
        else:
            # conflitos: mesmo original mapeado para outro normalizado → mantém o NOVO
            merged = pd.concat([old[[original_col, target_norm_col]], work], ignore_index=True)
            merged = merged.dropna(subset=[original_col, target_norm_col])
            merged = merged.drop_duplicates(subset=[original_col], keep="last")
            combined = merged.sort_values(by=[original_col]).reset_index(drop=True)
    else:
        combined = work.sort_values(by=[original_col]).reset_index(drop=True)

    combined.to_parquet(filepath, index=False)
    log.info("update_dim: %s linhas salvas em %s", len(combined), filepath)
