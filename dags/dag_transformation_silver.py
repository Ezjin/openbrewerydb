from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd
import os
from glob import glob
from utils.silver_pipeline import silver_pipeline
from utils.dimensions import update_dimension
from utils.normalization import normalize_name, normalize_brewery_df


log = LoggingMixin().log
today = datetime.today()

RAW_PATH = "data_lake_mock/raw"
SILVER_PATH = "data_lake_mock/silver"
DATASET_SILVER_PATH = Dataset("/logs/trigger_silver.csv")
DATASET_GOLD_PATH = Dataset("/logs/trigger_gold.csv")

read_path = os.path.join(
            RAW_PATH,
            f"year={today.year}",
            f"month={today.month:02d}",
            f"day={today.day:02d}"
        )

# -------------------------------------------------------------
# DAG - Tratamento Silver Layer 
# -------------------------------------------------------------

@dag(
    schedule= [DATASET_SILVER_PATH],
    start_date=datetime(2025, 9, 27),
    description="Transformação para a camada Silver - JSON para PARQUET",
    tags=["transformation", "silver", "brewery"]
)
def transformation_silver():

    @task()
    def update_dim(raw_path = read_path, silver_path = SILVER_PATH, log = log, batch_size = 10):
        files = glob(os.path.join(read_path, "*.json"))
        log.info(f"Total de arquivos encontrados: {len(files)}")

        dim_path = os.path.join(silver_path, "dim")
        os.makedirs(dim_path, exist_ok=True)

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i+batch_size]
            log.info(f"Processando batch {i//batch_size + 1}: {len(batch_files)} arquivos")

            dfs = [pd.read_json(f) for f in batch_files]
            df = pd.concat(dfs, ignore_index=True)
        
            df["country_norm"] = df["country"].map(normalize_name)
            df["state_norm"]   = df["state"].map(normalize_name)
            df["city_norm"]    = df["city"].map(normalize_name)

            # Atualizar dimensões
            update_dimension(df[["country", "country_norm"]].dropna(), "country_norm", os.path.join(dim_path, "dim_country.parquet"))
            update_dimension(df[["state", "state_norm"]].dropna(), "state_norm", os.path.join(dim_path, "dim_state.parquet"))
            update_dimension(df[["city", "city_norm"]].dropna(), "city_norm", os.path.join(dim_path, "dim_city.parquet"))

    @task()
    def transformation(raw_path = read_path, silver_path = SILVER_PATH, log = log, batch_size = 10):
        files = glob(os.path.join(read_path, "*.json"))
        log.info(f"Total de arquivos encontrados: {len(files)}")
        for i in range(0, len(files), batch_size):
            batch_files = files[i:i+batch_size]
            log.info(f"Processando batch {i//batch_size + 1}: {len(batch_files)} arquivos")

            dfs = [pd.read_json(f) for f in batch_files]
            df = pd.concat(dfs, ignore_index=True)
            
            df_norm = normalize_brewery_df(df)
            silver_pipeline(df_norm, silver_path, today.date(), log, i)

    @task(outlets=[DATASET_GOLD_PATH])
    def trigger_gold(log = log):

        log.info("Finalizada transformação para camada silver.")
    
    update_dim() >> transformation() >> trigger_gold()

transformation_silver()