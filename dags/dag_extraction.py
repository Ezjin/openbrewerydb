from airflow.sdk import dag
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from requests.exceptions import RequestException
import time
import math
import os
from datetime import datetime
from utils.get_api_data import get_api_data
from utils.save_api_data import save_api_data

log = LoggingMixin().log

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
META_URL = "https://api.openbrewerydb.org/v1/breweries/meta"
RAW_PATH = "data_lake_mock/raw/"  # ajuste para seu path
PER_PAGE = 100

# -------------------------------------------------------------
# DAG
# -------------------------------------------------------------

@dag(
    schedule = "@daily",
    start_date = datetime(2025, 9, 27),
    description = "Extração dos dados da API https://www.openbrewerydb.org/ ",
    tags=["extracao", "brewery"]
)
def extracao_dados_brewery():
    
    @task(retries=3, retry_delay=60)
    def get_total_pages(per_page: int = PER_PAGE, log=log) -> int:
        """Busca o total de itens da API e calcula o total de páginas."""
        try:
            meta = get_api_data(META_URL, log)
            total_items = meta["total"]
            log.info(f"Total de itens na API: {total_items}")
        except Exception as e:
            log.error(f"Erro ao buscar meta da API: {e}")
            raise

        total_pages = math.ceil(total_items / per_page)
        log.info(f"Serão feitas {total_pages} requisições (per_page={per_page})")
        return total_pages
        
    @task(retries=3, retry_delay=60)
    def get_api_task(total_pages: int, per_page = PER_PAGE, log=log):
        """ Salva a consulta pagina a pagina """
        today = datetime.today()
        path = os.path.join(
            RAW_PATH,
            f"year={today.year}",
            f"month={today.month:02d}",
            f"day={today.day:02d}"
        )

        for page in (range(1, total_pages + 1)):
            url = f"{BASE_URL}?page={page}&per_page={per_page}"
            log.info(f"Buscando página {page}/{total_pages}: {url}")
            
            success = False
            attempts = 0
            
            while not success and attempts < 3:
                try:
                    data = get_api_data(url, log)
                    save_api_data(data, RAW_PATH, page, log)
                    success = True
                    
                except RequestException as e:
                    attempts += 1
                    log.warning(f"Erro ao buscar página {page}: {e}. Tentativa {attempts}/3")
                    time.sleep(5)
                except Exception as e:
                    log.error(f"Erro inesperado na página {page}: {e}")
                    raise

        log.info(f"Todas as páginas processadas. Total de páginas: {total_pages}")
# -----------------------------------------------------------
# Fluxo
# -----------------------------------------------------------

    total_pages = get_total_pages()
    get_data = get_api_task(total_pages)


    total_pages >> get_data 


extracao_dados_brewery()