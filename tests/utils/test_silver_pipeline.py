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


def _make_dims(dir_dim: Path,
               country_map: dict[str, str],
               state_map: dict[str, str],
               city_map: dict[str, str]):
    df_country = pd.DataFrame({"country": list(country_map.keys()),
                               "country_norm": list(country_map.values())})
    df_state   = pd.DataFrame({"state": list(state_map.keys()),
                               "state_norm": list(state_map.values())})
    df_city    = pd.DataFrame({"city": list(city_map.keys()),
                               "city_norm": list(city_map.values())})
    _write_parquet(dir_dim / "dim_country.parquet", df_country)
    _write_parquet(dir_dim / "dim_state.parquet",   df_state)
    _write_parquet(dir_dim / "dim_city.parquet",    df_city)


# =======================
# Tests
# =======================

def test_silver_pipeline_sucesso(tmp_path, capsys):
    fact = tmp_path / "silver_fact"
    dim  = tmp_path / "dims"

    # Dimensões normalizam para formas "canonical"
    _make_dims(
        dim,
        country_map={"United States": "US", "Brasil": "BR"},
        state_map={"California": "CA", "São Paulo": "SP"},
        city_map={"San Francisco": "SF", "Campinas": "Campinas"},
    )

    # Raw com valores "não normalizados"
    df_raw = pd.DataFrame([
        {"country": "United States", "state": "California", "city": "San Francisco",
         "name": "A", "brewery_type": "micro", "phone": "111"},
        {"country": "Brasil", "state": "São Paulo", "city": "Campinas",
         "name": "B", "brewery_type": "brewpub", "phone": "222"},
        # outro do mesmo país/estado; valida que particiona por estado também
        {"country": "United States", "state": "California", "city": "San Francisco",
         "name": "C", "brewery_type": "large", "phone": None},
    ])

    batch = "2025-09-27"
    part  = 3
    silver_pipeline(df_raw, str(fact), str(dim), batch, part=part)

    # Verifica diretórios Hive
    assert (fact / f"batch={batch}").exists()
    assert (fact / f"batch={batch}" / "country=US").exists()
    assert (fact / f"batch={batch}" / "country=BR").exists()
    # pelo menos um state dentro dos países acima
    assert list((fact / f"batch={batch}" / "country=US").iterdir())  # tem state=...
    assert list((fact / f"batch={batch}" / "country=BR").iterdir())

    # Leitura do dataset
    out_df = _read_fact_df(fact)

    # Colunas de partição aparecem como colunas
    for col in ["batch", "country", "state", "part"]:
        assert col in out_df.columns

    # Normalização aplicada
    assert set(out_df["country"].unique()) == {"US", "BR"}
    assert set(out_df[out_df["country"] == "US"]["state"].unique()) == {"CA"}
    assert set(out_df[out_df["country"] == "BR"]["state"].unique()) == {"SP"}
    assert set(out_df["batch"].unique()) == {batch}
    assert set(out_df["part"].astype(str).unique()) == {str(part)}  

    # Conteúdo: 3 linhas totais
    assert len(out_df) == 3
    # exemplo de checagem de preservação de campo não-chave
    assert "phone" in out_df.columns

    logs = capsys.readouterr().out
    assert "Início silver_pipeline" in logs
    assert "Países a escrever:" in logs
    assert f"Silver salvo em {fact}/batch={batch}" in logs


def test_silver_pipeline_df_raw_vazio_ou_none(tmp_path):
    fact = tmp_path / "silver_fact"
    dim  = tmp_path / "dims"
    with pytest.raises(AirflowFailException) as exc:
        silver_pipeline(pd.DataFrame(), str(fact), str(dim), "2025-09-27", part=1)
    assert "df_raw vazio ou None." in str(exc.value)

    with pytest.raises(AirflowFailException) as exc:
        silver_pipeline(None, str(fact), str(dim), "2025-09-27", part=1)
    assert "df_raw vazio ou None." in str(exc.value)


def test_silver_pipeline_df_raw_campos_obrigatorios(tmp_path):
    fact = tmp_path / "silver_fact"
    dim  = tmp_path / "dims"

    df_raw = pd.DataFrame([
        # faltam 'city' e 'brewery_type'
        {"country": "United States", "state": "California", "name": "A"},
    ])

    with pytest.raises(AirflowFailException) as exc:
        silver_pipeline(df_raw, str(fact), str(dim), "2025-09-27", part=1)

    assert "Colunas ausentes em df_raw" in str(exc.value)


def test_silver_pipeline_dimensoes_faltando_levanta(tmp_path, capsys):
    fact = tmp_path / "silver_fact"
    dim  = tmp_path / "dims"

    # Não criamos arquivos de dimensão -> pd.read_parquet vai falhar
    df_raw = pd.DataFrame([
        {"country": "United States", "state": "California", "city": "San Francisco",
         "name": "A", "brewery_type": "micro"},
    ])

    with pytest.raises(AirflowFailException) as exc:
        silver_pipeline(df_raw, str(fact), str(dim), "2025-09-27", part=1)

    assert "Erro ao ler dimensões:" in str(exc.value)
    logs = capsys.readouterr().out
    assert "Falha ao ler dimensões" in logs


def test_silver_pipeline_descarta_todos_apos_dropna(tmp_path, capsys):
    fact = tmp_path / "silver_fact"
    dim  = tmp_path / "dims"

    # Dimensões que NÃO cobrem o raw (norms ficarão NaN)
    _make_dims(
        dim,
        country_map={"X": "X"},    # não cobre 'United States'
        state_map={"Y": "Y"},
        city_map={"Z": "Z"},
    )

    df_raw = pd.DataFrame([
        {"country": "United States", "state": "California", "city": "San Francisco",
         "name": "A", "brewery_type": "micro"},
        {"country": "United States", "state": "California", "city": "San Francisco",
         "name": "B", "brewery_type": "brewpub"},
    ])

    with pytest.raises(AirflowFailException) as exc:
        silver_pipeline(df_raw, str(fact), str(dim), "2025-09-27", part=1)

    assert "Todos os registros foram descartados após dropna()." in str(exc.value)
    logs = capsys.readouterr().out
    assert "Valores sem normalização:" in logs
