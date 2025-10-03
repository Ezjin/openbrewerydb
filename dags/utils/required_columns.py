from airflow.exceptions import AirflowFailException
from typing import Iterable
import pandas as pd


def require_columns(df: pd.DataFrame, cols: Iterable[str], ctx: str) -> None:
    """Valida a presença de colunas exigidas."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AirflowFailException(f"Colunas ausentes em {ctx}: {missing}")