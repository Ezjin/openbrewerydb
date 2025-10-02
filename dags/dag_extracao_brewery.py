from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import math

from utils.get_api_data import get_api_data  
from utils.save_api_data import save_api_data

log = LoggingMixin().log

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
META_URL = "https://api.openbrewerydb.org/v1/breweries/meta"
RAW_PATH = "data_lake_mock/raw/"
PER_PAGE = 200
DATASET_PATH = Dataset("/logs/trigger_silver.csv")

# -------------------------------------------------------------
# DAG - Extração dos dados
# -------------------------------------------------------------
@dag(
    schedule="@monthly",
    start_date=datetime(2025, 9, 27),
    description="Extração dos dados da API https://www.openbrewerydb.org/ ",
    tags=["extracao", "brewery"],
    catchup=False,
)
def extracao_brewery():

    @task(retries=3, retry_delay=timedelta(seconds=60))
    def get_total_pages(per_page: int = PER_PAGE) -> int:
        """Busca o total de itens da API e calcula o total de páginas."""
        log.info("Consultando meta endpoint: %s", META_URL)
        try:
            meta = get_api_data(META_URL)
        except ValueError as e:
            log.error("Falha ao obter meta: %s", e)
            raise

        total_items = meta.get("total")
        if not isinstance(total_items, int) or total_items < 0:
            log.error("Campo 'total' inválido no meta: %s", meta)
            raise ValueError("Meta inválido: campo 'total' ausente ou inválido")

        if per_page <= 0:
            log.error("per_page inválido: %s", per_page)
            raise ValueError("per_page deve ser > 0")

        total_pages = math.ceil(total_items / per_page)
        log.info("Itens=%s | per_page=%s | total_pages=%s", total_items, per_page, total_pages)
        return total_pages

    @task(retries=3, retry_delay=timedelta(seconds=60))
    def get_api_task(total_pages: int, per_page: int = PER_PAGE) -> None:
        """Consulta página a página e salva em RAW_PATH."""
        if total_pages <= 0:
            log.warning("Nenhuma página para processar (total_pages=%s)", total_pages)
            return

        for page in range(1, total_pages + 1):
            url = f"{BASE_URL}?page={page}&per_page={per_page}"
            log.info("Buscando página %s/%s: %s", page, total_pages, url)
            try:
                data = get_api_data(url)          
                save_api_data(data, RAW_PATH, page) 
                log.info("Página %s persistida com sucesso.", page)
            except ValueError as e:
                log.error("Erro ao processar página %s: %s", page, e)
                raise

        log.info("Todas as páginas processadas. total_pages=%s", total_pages)

    @task(outlets=[DATASET_PATH])
    def trigger_silver() -> None:
        log.info("Transformação concluída e dataset atualizado.")

    # Orquestração
    total_pages = get_total_pages()
    get_data = get_api_task(total_pages)
    total_pages >> get_data >> trigger_silver()

extracao_brewery()
