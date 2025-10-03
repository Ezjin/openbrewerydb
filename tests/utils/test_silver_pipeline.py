# tests/utils/test_silver_pipeline.py
from pathlib import Path
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pytest

from airflow.exceptions import AirflowFailException
from dags.utils.silver_pipeline import silver_pipeline


# =======================
# Helpers
# =======================

def _write_parquet(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path))


def _read_fact_df(fact_base: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(fact_base), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


def _make_dims(
    dir_dim: Path,
    country_map: dict[str, str],
    state_map: dict[str, str],
    city_map: dict[str, str],
    brewery_type_map: dict[str, str],
):
    df_country = pd.DataFrame(
        {"country": list(country_map.keys()), "country_norm": list(country_map.values())}
    )
    df_state = pd.DataFrame(
        {"state": list(state_map.keys()), "state_norm": list(state_map.values())}
    )
    df_city = pd.DataFrame(
        {"city": list(city_map.keys()), "city_norm": list(city_map.values())}
    )
    df_brewery_type = pd.DataFrame(
        {
            "brewery_type": list(brewery_type_map.keys()),
            "brewery_type_norm": list(brewery_type_map.values()),
        }
    )
    _write_parquet(dir_dim / "dim_country.parquet", df_country)
    _write_parquet(dir_dim / "dim_state.parquet", df_state)
    _write_parquet(dir_dim / "dim_city.parquet", df_city)
    _write_parquet(dir_dim / "dim_brewery_type.parquet", df_brewery_type)


# =======================
# Tests
# =======================

def test_silver_pipeline_sucesso(tmp_path, capsys):
    fact = tmp_path / "silver_fact"
    dim = tmp_path / "dims"

    # Dimensões normalizam para formas "canonical"
    _make_dims(
        dim,
        country_map={"United States": "US", "Brasil": "BR"},
        state_map={"California": "CA", "São Paulo": "SP"},
        city_map={"San Francisco": "SF", "Campinas": "Campinas"},
        brewery_type_map={"micro": "micro", "brewpub": "brewpub", "large": "large"},
    )

    # Raw com valores "não normalizados"
    df_raw = pd.DataFrame(
        [
            {
                "country": "United States",
                "state": "California",
                "city": "San Francisco",
                "name": "A",
                "brewery_type": "micro",
                "phone": "111",
            },
            {
                "country": "Brasil",
                "state": "São Paulo",
                "city": "Campinas",
                "name": "B",
                "brewery_type": "brewpub",
                "phone": "222",
            },
            # outro do mesmo país/estado; valida que particiona por estado também
            {
                "country": "United States",
                "state": "California",
                "city": "San Francisco",
                "name": "C",
                "brewery_type": "large",
                "phone": None,
            },
        ]
    )

    batch = "2025-09-27"
    part = 3
    silver_pipeline(df_raw, str(fact), str(dim), batch, part=part)

    # Verifica diretórios Hive
    assert (fact / f"batch={batch}").exists()
    assert (fact / f"batch={batch}" / "country=US").exists()
    assert (fact / f"batch={batch}" / "country=BR").exists()
    # pelo menos um state dentro dos países acim
