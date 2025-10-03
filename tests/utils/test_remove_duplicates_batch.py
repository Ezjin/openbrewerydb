# tests/utils/test_remove_duplicates_batch.py
from pathlib import Path
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pytest

from airflow.exceptions import AirflowFailException
from dags.utils.remove_duplicates_batch import remove_duplicates_batch  # ajuste se necessário


# =======================
# Helpers
# =======================

def _write_parquet(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path))


def _read_batch_df(batch_path: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(batch_path), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


# =======================
# Tests
# =======================

def test_batch_inexistente_apenas_log(tmp_path, capsys):
    silver_base = tmp_path / "silver_fact"
    date = "2025-09-27"
    # Não criamos batch=..., função deve apenas logar e retornar sem erro
    remove_duplicates_batch(date, str(silver_base))

    captured = capsys.readouterr().out
    assert f"Path do batch não existe: {silver_base / f'batch={date}'}" in captured


def test_batch_vazio_sem_linhas_log_warning(tmp_path, capsys):
    silver_base = tmp_path / "silver_fact"
    date = "2025-09-27"
    batch_dir = silver_base / f"batch={date}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    # parquet vazio (0 linhas)
    empty_tbl = pa.table({"dummy": pa.array([], type=pa.int32())})
    pq.write_table(empty_tbl, str(batch_dir / "empty.parquet"))

    remove_duplicates_batch(date, str(silver_base))

    captured = capsys.readouterr().out
    assert f"Nenhum dado encontrado para batch={date}" in captured


def test_colunas_identidade_ausentes_levanta(tmp_path):
    silver_base = tmp_path / "silver_fact"
    date = "2025-09-27"
    batch_dir = silver_base / f"batch={date}"

    # Falta 'city' (uma das identity cols)
    df = pd.DataFrame({
        "name": ["A"],
        "country": ["US"],
        "state": ["CA"],
        # "city": ["SF"],   # <- ausente para provocar erro
        "brewery_type": ["micro"],
        "part": [0],
        "address_1": ["x"]
    })
    _write_parquet(batch_dir / "data.parquet", df)

    with pytest.raises(AirflowFailException) as exc:
        remove_duplicates_batch(date, str(silver_base))

    assert "Colunas ausentes em batch=2025-09-27" in str(exc.value)


def test_deduplica_escolhendo_mais_completo(tmp_path, capsys):
    silver_base = tmp_path / "silver_fact"
    date = "2025-09-27"
    batch_dir = silver_base / f"batch={date}"

    # Duas linhas com MESMA identidade; uma mais "completa" nas não-chaves
    ident = dict(name="Brew", country="US", state="CA", city="SF", brewery_type="micro")
    df = pd.DataFrame([
        {**ident, "part": 0, "address_1": "Main st", "phone": "123", "website_url": "http://a"},
        {**ident, "part": 1, "address_1": None,      "phone": None,  "website_url": None},
        # Outra identidade, única
        {"name": "Other", "country": "BR", "state": "SP", "city": "Campinas", "brewery_type": "brewpub",
         "part": 0, "address_1": "Rua X", "phone": None, "website_url": None},
    ])

    _write_parquet(batch_dir / "chunk.parquet", df)

    remove_duplicates_batch(date, str(silver_base))

    # Le o resultado do próprio batch (a função reescreve partições country/state/part)
    out_df = _read_batch_df(batch_dir)

    # Devem existir apenas 2 registros (1 por identidade)
    # (A segunda linha duplicada da mesma identidade deve ser descartada)
    assert len(out_df) == 2

    # Confirma que a linha mantida para a identidade 'Brew' é a mais completa (tem phone/website/address_1)
    kept = out_df[(out_df["name"] == "Brew") & (out_df["city"] == "SF")]
    assert len(kept) == 1
    kept_row = kept.iloc[0]
    assert kept_row.get("phone") == "123"
    assert kept_row.get("website_url") == "http://a"
    assert kept_row.get("address_1") == "Main st"

    # Logs básicos
    logs = capsys.readouterr().out
    assert f"Deduplicação do batch={date}" in logs
    assert f"Dedup batch={date}: antes=3 depois=2 removidos=1" in logs
    assert f"Deduplicação concluída para batch={date}; registros finais=2" in logs


def test_sem_colunas_nao_chave_usa_drop_duplicates(tmp_path, capsys):
    silver_base = tmp_path / "silver_fact"
    date = "2025-09-27"
    batch_dir = silver_base / f"batch={date}"

    # Apenas colunas de identidade + 'part' (nenhuma não-chave)
    df = pd.DataFrame([
        {"name": "A", "country": "US", "state": "CA", "city": "SF", "brewery_type": "micro", "part": 0},
        {"name": "A", "country": "US", "state": "CA", "city": "SF", "brewery_type": "micro", "part": 1},  
        {"name": "B", "country": "US", "state": "CA", "city": "LA", "brewery_type": "brewpub", "part": 0},
    ])
    _write_parquet(batch_dir / "data.parquet", df)

    remove_duplicates_batch(date, str(silver_base))

    out_df = _read_batch_df(batch_dir)
    # Deve restar 2 identidades
    assert len(out_df) == 2

    captured = capsys.readouterr().out
    assert f"Deduplicação do batch={date}" in captured
    assert f"Dedup batch={date}: antes=3 depois=2 removidos=1" in captured
    assert f"Deduplicação concluída para batch={date}; registros finais=2" in captured

