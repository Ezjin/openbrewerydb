import re
import unicodedata
import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def normalize_name(value: str | None) -> str | None:
    """
    Normaliza uma string para formato padronizado:
    - strip + lowercase
    - remove acentos/diacríticos
    - substitui caracteres não alfanuméricos por '_'
    - reduz múltiplos '_' a um só
    - remove '_' no início/fim

    Args:
        value: String de entrada.

    Returns:
        String normalizada ou None se entrada for None.
    """
    if value is None:
        return None

    try:
        value = value.strip().lower()
        value = unicodedata.normalize("NFKD", value)
        value = "".join(c for c in value if not unicodedata.combining(c))
        value = re.sub(r"[^a-z0-9]+", "_", value)
        value = re.sub(r"_+", "_", value)
        value = value.strip("_")
        return value
    except Exception:
        log.exception("Erro ao normalizar valor: %s", value)
        raise AirflowFailException(f"normalize_name falhou para '{value}'")


def normalize_brewery_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas de um DataFrame de breweries:
    - Converte colunas textuais para string dtype
    - Converte latitude/longitude para numérico (coerce)

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame com dtypes normalizados.

    Raises:
        AirflowFailException: Se df for vazio ou None.
    """
    if df is None or df.empty:
        log.error("DataFrame vazio recebido em normalize_brewery_df")
        raise AirflowFailException("DataFrame vazio em normalize_brewery_df")

    text_cols = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "state", "street",
        "website_url", "phone"
    ]

    num_cols = ["latitude", "longitude"]

    try:
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype("string")

        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        log.info(
            "normalize_brewery_df concluído: rows=%s cols=%s",
            len(df), len(df.columns)
        )
        return df
    except Exception:
        log.exception("Erro ao normalizar DataFrame breweries")
        raise AirflowFailException("normalize_brewery_df falhou")
