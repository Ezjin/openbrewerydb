import re
import unicodedata
import pandas as pd

def normalize_name(value: str) -> str:

        if value is None:
            return None

        # strip + lowercase
        value = value.strip().lower()

        # remover acentos/diacríticos
        value = unicodedata.normalize("NFKD", value)
        value = "".join(c for c in value if not unicodedata.combining(c))

        # substituir qualquer caractere que não seja letra ou número por "_"
        value = re.sub(r"[^a-z0-9]+", "_", value)

        # remover underscores consecutivos
        value = re.sub(r"_+", "_", value)

        # remover underscores no início/fim
        value = value.strip("_")

        return value

def normalize_brewery_df(df):
    # Colunas de texto
    text_cols = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "state", "street", "website_url",
        "phone"
    ]

    num_cols = [
         "latitude", "longitude"
         ]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Colunas numéricas
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df