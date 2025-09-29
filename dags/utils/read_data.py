import json


def read_data(filepath, log):
    """
    Lê um arquivo JSON e retorna seu conteúdo como um objeto Python (dict ou list).

    Args:
        filepath (str): Caminho do arquivo JSON.
        log (structlog): Criar com LoggingMixin().log.

    Returns:
        dict | list: Conteúdo do JSON.
    """
    if filepath.endswith(".json"):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                conteudo = json.load(f)
            return conteudo
        except FileNotFoundError:
            log.error(f"Arquivo não encontrado: {filepath}")
            return None
        except json.JSONDecodeError as e:
            log.error(f"Erro ao decodificar JSON: {e}")
            return None
    else:
        log.error(f"Arquivo com extensão errada: {filepath}")