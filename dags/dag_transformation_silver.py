from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from utils.silver_pipeline import silver_pipeline
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
import os


log = LoggingMixin().log

RAW_PATH = "data_lake_mock/raw"
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
    def transformation(raw_path = RAW_PATH, silver_path = SILVER_PATH, log=log):
        today = datetime.today()

        read_path = os.path.join(
            RAW_PATH,
            f"year={today.year}",
            f"month={today.month:02d}",
            f"day={today.day:02d}"
        )

        silver_pipeline(read_path, silver_path, log)
    

    transformation()

transformation_silver()