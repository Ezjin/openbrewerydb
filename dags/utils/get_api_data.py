import requests
from airflow.utils.log.logging_mixin import LoggingMixin

def get_api_data(link: str) -> dict:
    """
    Faz GET em `link` e retorna o JSON.
    Loga no padrão do Airflow (UI/handlers configurados).

    Args:
        link: URL do endpoint.

    Returns:
        dict: JSON da resposta.

    Raises:
        ValueError: Em erro HTTP, rede/timeout ou JSON inválido.
    """
    log = LoggingMixin().log
    log.info("GET %s", link)

    response = None
    try:
        response = requests.get(link, timeout=30)
        response.raise_for_status()
        log.info("HTTP %s em %s", response.status_code, link)

        try:
            payload = response.json()
            # opcional: tamanho/shape sem vazar conteúdo
            log.info("JSON parse ok (%s bytes)", len(response.content))
            return payload
        except ValueError as e:
            # JSON inválido
            preview = (response.text or "")[:200]
            log.error("JSON inválido ao acessar %s: preview='%s'", link, preview)
            raise ValueError(f"Resposta não-JSON em {link}") from e

    except requests.exceptions.HTTPError as e:
        body_preview = (getattr(response, "text", "") or "")[:200] if response is not None else "N/A"
        status = getattr(response, "status_code", "N/A")
        log.error("HTTPError em %s status=%s body_preview='%s'", link, status, body_preview)
        raise ValueError(f"Falha HTTP ao acessar {link}") from e

    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        log.error("Erro de rede/timeout em %s: %s", link, e)
        raise ValueError(f"Erro de rede/timeout ao acessar {link}") from e

    except Exception as e:
        log.exception("Erro inesperado em %s", link)
        raise ValueError(f"Falha inesperada ao acessar {link}") from e
