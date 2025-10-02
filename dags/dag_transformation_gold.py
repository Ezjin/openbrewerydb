from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import os

from utils.gold_pipeline import gold_pipeline  # usa LoggingMixin().log internamente
from utils.context_utils import get_run_day

SILVER_PATH = "data_lake_mock/silver/fact"
GOLD_PATH = "data_lake_mock/gold"
DATASET_GOLD_PATH = Dataset("/logs/trigger_gold.csv")

log = LoggingMixin().log

@dag(
    schedule=[DATASET_GOLD_PATH],  # dispara quando o dataset for atualizado
    start_date=datetime(2025, 9, 27),
    description="Agregação para a camada Gold - Nº de cervejarias por País/Estado/Cidade",
    tags=["aggregation", "gold", "brewery"],
    catchup=False,
)
def transformation_gold():

    @task()
    def aggregation_silver_to_gold(
        silver_path: str = SILVER_PATH,
        gold_path: str = GOLD_PATH,
    ) -> str:
        
        day_run = get_run_day()

        # Gold particionada por batch para manter histórico de execuções
        gold_path_batch = os.path.join(gold_path, f"batch={day_run}")
        log.info("Iniciando gold_pipeline: silver=%s gold_batch=%s", silver_path, gold_path_batch)

        out_dir = gold_pipeline(silver_path=silver_path, gold_path=gold_path_batch)
        log.info("Gold concluído em: %s", out_dir)
        return out_dir

    aggregation_silver_to_gold()

transformation_gold()
