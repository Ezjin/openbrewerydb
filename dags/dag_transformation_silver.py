from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from utils.save_data import save_data
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


SILVER_PATH = "data_lake_mock/silver"
DATASET_PATH = Dataset("/logs/trigger_silver.csv")

# -------------------------------------------------------------
# DAG - Tratamento Silver Layer 
# -------------------------------------------------------------

@dag(
    schedule= [DATASET_PATH],
    start_date=datetime(2025, 9, 27),
    description="Transformação para a camada Silver - JSON para PARQUET",
    tags=["transformation", "silver", "brewery"]
)
def transformation_silver():
    
    @task()
    def task1():
        print("Funcionou")

    
    task1()

transformation_silver()
