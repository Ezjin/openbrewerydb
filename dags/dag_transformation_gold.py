from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
import pyarrow as pa
import os
from datetime import datetime

from utils.gold_pipeline import gold_pipeline


SILVER_PATH = "data_lake_mock/silver/fact"
GOLD_PATH = "data_lake_mock/gold"
DATASET_GOLD_PATH = Dataset("/logs/trigger_gold.csv")

today = datetime.today()

log = LoggingMixin().log

# -------------------------------------------------------------
# DAG - Tratamento Silver Layer 
# -------------------------------------------------------------

@dag(
    schedule = [DATASET_GOLD_PATH],
    start_date = datetime(2025, 9, 27),
    description = "Agregação para a camada Gold - Número de Cervejarias por País/Estado/Cidade",
    tags = ["aggregation", "gold", "brewery"]
)
def transformation_gold():

    @task
    def aggregation_silver_to_gold(silver_path = SILVER_PATH, gold_path = GOLD_PATH, log = log):

        gold_path_batch = os.path.join(gold_path, f"batch={today.date()}")
        
        gold_pipeline(silver_path, gold_path_batch, log)

    aggregation_silver_to_gold()

transformation_gold()
