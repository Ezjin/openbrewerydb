# tests/utils/test_gold_pipeline.py
import os
from pathlib import Path

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.exceptions import AirflowFailException
from dags.utils.gold_pipeline import gold_pipeline


# ------------ Helpers ------------

def _write_parquet_rows(path: Path, df: pd.DataFrame):
    """
    Grava um parquet simples (sem partições) no caminho exato `path`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path))


def _read_parquet_df(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")


# ------------ Tests ------------

def test_silver_inexistente(tmp_path, capsys):
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    assert not silver.exists()

    out_dir = gold_pipeline(str(silver), str(gold), batch_size=8)

    # retorna gold_path
    assert out_dir == str(gold)

    # arquivo gerado
    total_path = gold / "total.parquet"
    assert total_path.exists()

    # conteúdo vazio com colunas esperadas
    df = _read_parquet_df(total_path)
    assert list(df.columns) == ["country", "state", "city", "brewery_type", "count"]
    assert df.empty

    # logs (stdout dos handlers do Airflow)
    logs = capsys.readouterr().out
    assert f"Silver path não existe: {silver}" in logs


def test_agrega_de_particoes(tmp_path, capsys):
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # Cria arquivos parquet dentro de diretórios Hive-style:
    # country=US/state=CA/city=SF/brewery_type=micro -> 2 arquivos (total 3 linhas)
    _write_parquet_rows(
        silver / "country=US" / "state=CA" / "city=SF" / "brewery_type=micro" / "part-0001.parquet",
        pd.DataFrame({"dummy": [1, 2]})
    )
    _write_parquet_rows(
        silver / "country=US" / "state=CA" / "city=SF" / "brewery_type=micro" / "part-0002.parquet",
        pd.DataFrame({"dummy": [3]})
    )
    # country=US/state=CA/city=LA/brewery_type=brewpub -> 1 arquivo (2 linhas)
    _write_parquet_rows(
        silver / "country=US" / "state=CA" / "city=LA" / "brewery_type=brewpub" / "data.parquet",
        pd.DataFrame({"dummy": [9, 9]})
    )
    # country=BR/state=SP/city=Campinas/brewery_type=micro -> 1 arquivo (1 linha)
    _write_parquet_rows(
        silver / "country=BR" / "state=SP" / "city=Campinas" / "brewery_type=micro" / "data.parquet",
        pd.DataFrame({"dummy": [42]})
    )

    out_dir = gold_pipeline(str(silver), str(gold), batch_size=2)
    assert out_dir == str(gold)

    total_path = gold / "total.parquet"
    assert total_path.exists()

    df = _read_parquet_df(total_path)
    # colunas e ordenação por count desc
    assert list(df.columns) == ["country", "state", "city", "brewery_type", "count"]

    # Esperado:
    # (US, CA, SF, micro) -> 3
    # (US, CA, LA, brewpub) -> 2
    # (BR, SP, Campinas, micro) -> 1
    expected = pd.DataFrame(
        [
            ["US", "CA", "SF", "micro", 3],
            ["US", "CA", "LA", "brewpub", 2],
            ["BR", "SP", "Campinas", "micro", 1],
        ],
        columns=["country", "state", "city", "brewery_type", "count"]
    )
    # verificação por merge, ignorando ordem de linhas mas exigindo igualdade de conteúdo
    merged = df.merge(expected, on=["country", "state", "city", "brewery_type", "count"], how="outer", indicator=True)
    assert (merged["_merge"] == "both").all()
    # checar que está ordenado por count desc (1a linha deve ser 3)
    assert df.loc[0, "count"] >= df.loc[1, "count"] >= df.loc[2, "count"]

    logs = capsys.readouterr().out
    assert "Início gold_pipeline" in logs
    assert "Gold gravado:" in logs
    assert "(groups=3" in logs  # 3 grupos


def test_erro_colunas_ausentes(tmp_path):
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # Cria um arquivo parquet SEM diretórios-partição hive e SEM as colunas esperadas
    _write_parquet_rows(
        silver / "data.parquet",
        pd.DataFrame({"foo": [1, 2, 3]})
    )

    with pytest.raises(AirflowFailException) as exc:
        gold_pipeline(str(silver), str(gold), batch_size=16)

    # A mensagem vem de _require_columns()
    assert "Colunas ausentes em silver_batch" in str(exc.value)
    # Não há assert de logs aqui porque a exceção é propagada (sem log.exception)


def test_dataset_sem_dados(tmp_path, capsys):
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    # Dataset existe, mas com 0 linhas no parquet
    # Cria um parquet vazio (sem linhas) — as colunas podem até existir, mas sem rows não agrega.
    empty_table = pa.table({"dummy": pa.array([], type=pa.int32())})
    (silver).mkdir(parents=True, exist_ok=True)
    pq.write_table(empty_table, str(silver / "empty.parquet"))

    out_dir = gold_pipeline(str(silver), str(gold), batch_size=8)
    assert out_dir == str(gold)

    total_path = gold / "total.parquet"
    assert total_path.exists()

    df = _read_parquet_df(total_path)
    assert list(df.columns) == ["country", "state", "city", "brewery_type", "count"]
    assert df.empty

    logs = capsys.readouterr().out
    assert "Nenhum dado agregado encontrado. (batches=" in logs
