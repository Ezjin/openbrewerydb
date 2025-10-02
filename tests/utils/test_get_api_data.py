# tests/utils/test_get_api_data.py
import pytest
import requests
from dags.utils.get_api_data import get_api_data  # ajuste se o caminho for diferente

class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="", raise_http=False):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        # content para o log "bytes"
        self.content = text.encode("utf-8") if text else (b"{}" if json_data is not None else b"")
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


def test_sucesso(monkeypatch, capsys):
    called = {"args": None, "kwargs": None}

    def fake_get(*args, **kwargs):
        called["args"] = args
        called["kwargs"] = kwargs
        # resposta OK com JSON
        return _FakeResponse(status_code=200, json_data={"hello": "world"}, text='{"hello":"world"}')

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/x"
    out = get_api_data(url)

    # retorno correto
    assert out == {"hello": "world"}

    # garante que passamos timeout=30
    assert called["kwargs"]["timeout"] == 30

    # captura o que os handlers do Airflow imprimiram (stdout)
    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"HTTP 200 em {url}" in captured
    assert "JSON parse ok (17 bytes)" in captured


def test_http_error(monkeypatch, capsys):
    def fake_get(*_, **__):
        # dispara HTTPError em raise_for_status
        return _FakeResponse(
            status_code=404,
            json_data={"err": "x"},
            text="not found",
            raise_http=True
        )

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/missing"
    with pytest.raises(ValueError) as exc:
        get_api_data(url)

    # checa a exceção retornada
    assert f"Falha HTTP ao acessar {url}" in str(exc.value)

    # captura o stdout onde o logger do Airflow escreve
    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"HTTPError em {url} status=404" in captured
    assert "body_preview='not found'" in captured

def test_timeout(monkeypatch, capsys):
    def fake_get(*_, **__):
        raise requests.exceptions.Timeout("boom")

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/slow"
    with pytest.raises(ValueError) as exc:
        get_api_data(url)

    assert f"Erro de rede/timeout ao acessar {url}" in str(exc.value)

    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"Erro de rede/timeout em {url}: boom" in captured


def test_connection_error(monkeypatch, capsys):
    def fake_get(*_, **__):
        raise requests.exceptions.ConnectionError("no route")

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/down"
    with pytest.raises(ValueError) as exc:
        get_api_data(url)

    assert f"Erro de rede/timeout ao acessar {url}" in str(exc.value)

    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"Erro de rede/timeout em {url}: no route" in captured


def test_json_invalido(monkeypatch, capsys):
    # response.ok, mas .json() levanta ValueError
    def fake_get(*_, **__):
        return _FakeResponse(
            status_code=200,
            json_data=ValueError("invalid json"),
            text="<!doctype html> not json"
        )

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/bad-json"
    with pytest.raises(ValueError) as exc:
        get_api_data(url)

    # comportamento atual da função: reempacota em "Falha inesperada..."
    assert f"Falha inesperada ao acessar {url}" in str(exc.value)

    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"HTTP 200 em {url}" in captured
    assert f"JSON inválido ao acessar {url}: preview='<!doctype html> not json'" in captured
    assert f"Erro inesperado em {url}" in captured


def test_erro_inesperado(monkeypatch, capsys):
    # simula exceção não prevista (ex.: RuntimeError)
    def fake_get(*_, **__):
        raise RuntimeError("something odd")

    monkeypatch.setattr(requests, "get", fake_get)

    url = "https://api.example.com/weird"
    with pytest.raises(ValueError) as exc:
        get_api_data(url)

    assert f"Falha inesperada ao acessar {url}" in str(exc.value)

    captured = capsys.readouterr().out
    assert f"GET {url}" in captured
    assert f"Erro inesperado em {url}" in captured
