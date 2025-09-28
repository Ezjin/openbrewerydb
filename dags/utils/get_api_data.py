import requests

def get_api_data(link, log):
    try:
        log.info(f"Iniciando get em: {link}")
        response = requests.get(link)
        response.raise_for_status()
        log.info(f"Resposta recebida: {response.status_code}")
        return response.json()
    except Exception as e:
        log.error(f"Erro ao acessar {link}: {e} status: {response.status_code}")
        raise ValueError(f"Falha ao acessar API")