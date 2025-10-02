import json
import os
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def save_api_data(data: dict | list, base_path: str, page: int) -> str:
    """
    Salva dados JSON retornados da API em partições por data (year/month/day).
    Usa logger padrão do Airflow para registrar eventos.

    Args:
        data: Dados retornados da API (dict ou list).
        base_path: Diretório base onde salvar os dados.
        page: Número da página (para compor o nome do arquivo).

    Returns:
        Caminho completo do arquivo salvo.

    Raises:
        OSError: Se ocorrer erro ao criar diretórios ou salvar arquivo.
    """
    log = LoggingMixin().log

    # Cria partições por data
    today = datetime.today()
    path = os.path.join(
        base_path,
        f"year={today.year}",
        f"month={today.month:02d}",
        f"day={today.day:02d}"
    )
    os.makedirs(path, exist_ok=True)

    filename = os.path.join(path, f"breweries_page_{page:03d}.json")

    try:
        with open(filename, "w", encoding="utf-8") as f:
            # `ensure_ascii=False` mantém acentos, `indent=2` é opcional
            json.dump(data, f, ensure_ascii=False)
        log.info(
            "Página %s salva em %s (tipo=%s, tamanho=%s)",
            page,
            filename,
            type(data).__name__,
            len(data) if hasattr(data, "__len__") else "N/A"
        )
    except Exception:
        log.exception("Erro ao salvar página %s em %s", page, filename)
        raise

    return filename
