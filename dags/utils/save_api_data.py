import json
import os
from datetime import datetime


def save_api_data(data, base_path, page, log):
    """
    Salva o JSON retornado da API em disco, criando partições por data.
    
    Args:
        data (list/dict): Dados retornados da API.
        base_path (str): Diretório base onde salvar os dados.
        page (int): Número da página (para nome do arquivo).
    
    Returns:
        str: Caminho completo do arquivo salvo.
    """
    # Cria diretórios por ano/mês/dia
    today = datetime.today()
    path = os.path.join(
        base_path,
        f"year={today.year}",
        f"month={today.month:02d}",
        f"day={today.day:02d}"
    )
    os.makedirs(path, exist_ok=True)

    # Salva JSON
    filename = os.path.join(path, f"breweries_page_{page:03d}.json")
    try:
        with open(filename, "w") as f:
            json.dump(data, f)
        log.info(f"Página {page} salva em {filename} ({len(data)} itens)")
    except Exception as e:
        log.error(f"Erro ao salvar página {page} em {filename}: {e}")
        raise

    return filename