# tests/utils/test_normalization.py
import types
import unicodedata as real_unicodedata
import pandas as pd
import pytest

from airflow.exceptions import AirflowFailException
from dags.utils.normalization import normalize_name, normalize_brewery_df


# -----------------------------
# normalize_name
# -----------------------------

def test_normalize_name_basico():
    assert normalize_name(" Cervejaria São Paulo ") == "cervejaria_sao_paulo"
    assert normalize_name("123@!#Teste") == "123_teste"
    assert normalize_name("ÁÉÍÓÚ") == "aeiou"


def test_normalize_name_none():
    assert normalize_name(None) is None


def test_normalize_name_multiplos_underscores():
    # deve reduzir vários "_" para apenas um
    assert normalize_name("A---B__C") == "a_b_c"


def test_normalize_name_inicio_fim_underscore():
    assert normalize_name("___Hello___") == "hello"


def test_normalize_name_erro(monkeypatch, capsys):
    # NÃO patchar real unicodedata.normalize global — isso quebra o pytest.
    # Em vez disso, substituímos o NOME 'unicodedata' apenas dentro do módulo alvo.
    def fake_normalize(*_, **__):
        raise RuntimeError("boom")

    import dags.utils.normalization as mod
    fake_uni = types.SimpleNamespace(
        normalize=fake_normalize,
        combining=real_unicodedata.combining  # manter 'combining' real
    )
    # substitui o objeto unicodedata usado PELO MÓDULO (sem afetar o global)
    monkeypatch.setattr(mod, "unicodedata", fake_uni, raising=True)

    with pytest.raises(AirflowFailException):
        normalize_name("abc")

    logs = capsys.readouterr().out
    assert "Erro ao normalizar valor" in logs


# -----------------------------
# normalize_brewery_df
# -----------------------------

def test_normalize_brewery_df_sucesso(capsys):
    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["Cervejaria A", "Brewery B"],
        "latitude": ["-23.5", None],
        "longitude": ["-46.6", "invalid"]
    })

    out = normalize_brewery_df(df.copy())

    # text cols viram string dtype
    assert str(out["id"].dtype) == "string"
    assert str(out["name"].dtype) == "string"

    # latitude/longitude viram numéricos, com coerção (invalid -> NaN)
    assert pd.api.types.is_float_dtype(out["latitude"])
    assert pd.isna(out.loc[1, "longitude"])

    logs = capsys.readouterr().out
    assert "normalize_brewery_df concluído" in logs


def test_normalize_brewery_df_vazio_ou_none(capsys):
    with pytest.raises(AirflowFailException):
        normalize_brewery_df(pd.DataFrame())

    with pytest.raises(AirflowFailException):
        normalize_brewery_df(None)

    logs = capsys.readouterr().out
    assert "DataFrame vazio recebido em normalize_brewery_df" in logs


def test_normalize_brewery_df_erro(monkeypatch, capsys):
    # Forçar erro em pd.to_numeric (usado para latitude/longitude)
    df = pd.DataFrame({
        "id": [1, 2],
        "latitude": [1.0, 2.0],   # garante que o caminho de to_numeric é exercitado
    })

    def fake_to_numeric(*_, **__):
        raise RuntimeError("boom")

    monkeypatch.setattr(pd, "to_numeric", fake_to_numeric, raising=True)

    with pytest.raises(AirflowFailException):
        normalize_brewery_df(df)

    logs = capsys.readouterr().out
    assert "Erro ao normalizar DataFrame breweries" in logs
