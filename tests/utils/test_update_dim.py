# tests/utils/test_update_dim.py
from pathlib import Path
import pandas as pd
import pytest

from airflow.exceptions import AirflowFailException
from dags.utils.update_dim import update_dim  


def _read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p)


def test_update_dim_com_coluna_normalizada_sucesso(tmp_path, capsys):
    p = tmp_path / "dim_country.parquet"
    df = pd.DataFrame({
        "country": ["Brasil", "United States", "Brasil"],   # dup na mesma chamada
        "country_norm": ["BR", "US", "BR_ALT"]              # a função mantém o PRIMEIRO (BR)
    })

    update_dim(df, original_col="country", normalized_col="country_norm", filepath=str(p))

    assert p.exists()
    out = _read_parquet(p)
    # Mantém apenas duas chaves (Brasil/United States)
    assert set(out.columns) == {"country", "country_norm"}
    assert set(out["country"]) == {"Brasil", "United States"}
    # Como o dedup interno usa keep="first", vence "BR", não "BR_ALT"
    assert out.loc[out["country"] == "Brasil", "country_norm"].iloc[0] == "BR"

    logs = capsys.readouterr().out
    assert "update_dim: 2 linhas salvas em" in logs


def test_update_dim_merge_sobrescreve_existente(tmp_path, capsys):
    p = tmp_path / "dim_state.parquet"
    # arquivo existente
    old = pd.DataFrame({"state": ["California", "São Paulo"], "state_norm": ["CA_OLD", "SP"]})
    old.to_parquet(p, index=False)

    # novo df com conflito na mesma chave -> novo deve vencer
    df = pd.DataFrame({"state": ["California"], "state_norm": ["CA"]})
    update_dim(df, original_col="state", normalized_col="state_norm", filepath=str(p))

    out = _read_parquet(p)
    assert set(out.columns) == {"state", "state_norm"}
    # "California" deve estar com "CA" (novo valor), "São Paulo" preservado
    assert out.loc[out["state"] == "California", "state_norm"].iloc[0] == "CA"
    assert out.loc[out["state"] == "São Paulo", "state_norm"].iloc[0] == "SP"

    logs = capsys.readouterr().out
    assert "update_dim:" in logs and "linhas salvas em" in logs


def test_update_dim_sem_coluna_norm_usa_normalizer(tmp_path, capsys):
    p = tmp_path / "dim_city.parquet"
    df = pd.DataFrame({"city": ["Campinas", None, "São Paulo"]})

    def normalizer(s: str) -> str:
        return s.strip().lower().replace(" ", "_")

    update_dim(df, original_col="city", normalized_col=None, filepath=str(p), normalizer=normalizer)

    out = _read_parquet(p)
    assert set(out.columns) == {"city", "city_norm"}
    # linha com None é descartada
    assert set(out["city"]) == {"Campinas", "São Paulo"}
    assert out.loc[out["city"] == "Campinas", "city_norm"].iloc[0] == "campinas"
    assert out.loc[out["city"] == "São Paulo", "city_norm"].iloc[0] == "são_paulo"

    logs = capsys.readouterr().out
    assert "update_dim: 2 linhas salvas em" in logs


def test_update_dim_df_vazio_ou_none(tmp_path):
    p = tmp_path / "dim_any.parquet"
    with pytest.raises(AirflowFailException):
        update_dim(pd.DataFrame(), original_col="x", normalized_col="x_norm", filepath=str(p))
    with pytest.raises(AirflowFailException):
        update_dim(None, original_col="x", normalized_col="x_norm", filepath=str(p))


def test_update_dim_sem_coluna_original(tmp_path):
    p = tmp_path / "dim_country.parquet"
    df = pd.DataFrame({"wrong": ["A"]})
    with pytest.raises(AirflowFailException):
        update_dim(df, original_col="country", normalized_col="country_norm", filepath=str(p))


def test_update_dim_sem_normalizer_quando_norm_col_none(tmp_path):
    p = tmp_path / "dim_city.parquet"
    df = pd.DataFrame({"city": ["X"]})
    with pytest.raises(AirflowFailException):
        update_dim(df, original_col="city", normalized_col=None, filepath=str(p))


def test_update_dim_reescreve_quando_schema_antigo_invalido(tmp_path, capsys):
    p = tmp_path / "dim_state.parquet"
    # arquivo existente com schema inválido (sem as colunas exigidas)
    bad = pd.DataFrame({"foo": ["x"], "bar": ["y"]})
    bad.to_parquet(p, index=False)

    df = pd.DataFrame({"state": ["California"], "state_norm": ["CA"]})
    update_dim(df, original_col="state", normalized_col="state_norm", filepath=str(p))

    out = _read_parquet(p)
    assert set(out.columns) == {"state", "state_norm"}
    assert set(out["state"]) == {"California"}

    logs = capsys.readouterr().out
    assert "update_dim: arquivo existente sem colunas" in logs
    assert "será reescrito" in logs
