# tests/utils/test_save_api_data.py
import json
import builtins
import pytest
from pathlib import Path
from datetime import datetime as _Datetime

from dags.utils.save_api_data import save_api_data  


class _FixedDateTime(_Datetime):
    """Subclasse de datetime para fixar today()."""
    @classmethod
    def today(cls):
        # YYYY-MM-DD fixo para o teste
        return cls(2025, 1, 2)


def test_save_api_data_sucesso(tmp_path, monkeypatch, capsys):
    # fixa a data "hoje" no módulo sob teste
    import dags.utils.save_api_data as mod_under_test
    monkeypatch.setattr(mod_under_test, "datetime", _FixedDateTime, raising=True)

    base_path = tmp_path
    page = 7
    data = [{"id": 1, "name": "Cervejaria São Paulo"}]  # inclui acento para validar ensure_ascii=False

    # executa
    out_path = save_api_data(data, str(base_path), page)

    # verifica caminho esperado (year/month/day + arquivo com page zero-padded)
    expected = (
        base_path
        / "year=2025"
        / "month=01"
        / "day=02"
        / "breweries_page_007.json"
    )
    assert Path(out_path) == expected
    assert expected.exists()

    # conteúdo salvo é JSON igual ao dado
    with expected.open("r", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved == data

    # logs emitidos pelo LoggingMixin (stdout)
    captured = capsys.readouterr().out
    assert f"Página {page} salva em {expected}" in captured
    assert "(tipo=list, tamanho=1)" in captured
    # também deve registrar o GET não; aqui só save, então nada de GET.


def test_save_api_data_erro_ao_salvar(tmp_path, monkeypatch, capsys):
    # fixa a data "hoje" no módulo sob teste
    import dags.utils.save_api_data as mod_under_test
    monkeypatch.setattr(mod_under_test, "datetime", _FixedDateTime, raising=True)

    base_path = tmp_path
    page = 7
    data = {"k": "v"}

    # mocka open para falhar somente durante a escrita
    real_open = builtins.open

    def fake_open(*args, **kwargs):
        # se for o arquivo de destino do save, provoca erro
        target_dir = (
            Path(base_path)
            / "year=2025" / "month=01" / "day=02"
        )
        # como save_api_data chama os.makedirs antes, podemos usar startswith
        if args and isinstance(args[0], (str, Path)) and str(args[0]).startswith(str(target_dir)):
            raise OSError("disk full")
        return real_open(*args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open, raising=True)

    with pytest.raises(OSError) as exc:
        save_api_data(data, str(base_path), page)

    assert "disk full" in str(exc.value)

    captured = capsys.readouterr().out
    # log.exception do Airflow inclui a mensagem
    # Verifica prefixo + caminho estimado
    expected_file = (
        Path(base_path) / "year=2025" / "month=01" / "day=02" / "breweries_page_007.json"
    )
    assert f"Erro ao salvar página {page} em {expected_file}" in captured
