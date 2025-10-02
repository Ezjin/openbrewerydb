from airflow.sdk import dag
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context
from datetime import datetime
import pandas as pd
import os
from glob import glob
from utils.silver_pipeline import silver_pipeline
from utils.dimensions import update_dimension
from utils.normalization import normalize_name, normalize_brewery_df
from utils.remove_duplicates_batch import remove_duplicates_batch

log = LoggingMixin().log
today = datetime.today()

RAW_PATH = "data_lake_mock/raw"
SILVER_PATH_DIM = "data_lake_mock/silver/dim"
SILVER_PATH_FACT = "data_lake_mock/silver/fact"
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
    def update_dim(raw_path = read_path, silver_path_dim = SILVER_PATH_DIM, log = log, batch_size = 10):
        
        files = glob(os.path.join(raw_path, "*.json"))
        log.info(f"Total de arquivos encontrados: {len(files)}")
        os.makedirs(silver_path_dim, exist_ok=True)

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i+batch_size]
            log.info(f"Processando batch {i//batch_size + 1}: {len(batch_files)} arquivos")

            dfs = [pd.read_json(f) for f in batch_files]
            df = pd.concat(dfs, ignore_index=True)
        
            df["country_norm"] = df["country"].map(normalize_name)
            df["state_norm"]   = df["state"].map(normalize_name)
            df["city_norm"]    = df["city"].map(normalize_name)

            # Atualizar dimensões
            update_dimension(df[["country", "country_norm"]].dropna(), "country_norm", os.path.join(silver_path_dim, "dim_country.parquet"))
            update_dimension(df[["state", "state_norm"]].dropna(), "state_norm", os.path.join(silver_path_dim, "dim_state.parquet"))
            update_dimension(df[["city", "city_norm"]].dropna(), "city_norm", os.path.join(silver_path_dim, "dim_city.parquet"))

    @task()
    def transformation(raw_path = read_path, silver_path_fact = SILVER_PATH_FACT, silver_path_dim = SILVER_PATH_DIM, log = log, batch_size = 10):
        context = get_current_context()
        ds = context["ds"]  # string "YYYY-MM-DD"
        files = glob(os.path.join(raw_path, "*.json"))
        log.info(f"Total de arquivos encontrados: {len(files)}")
        
        for i in range(0, len(files), batch_size):
            batch_files = files[i:i+batch_size]
            log.info(f"Processando batch {i//batch_size + 1}: {len(batch_files)} arquivos")

            dfs = [pd.read_json(f) for f in batch_files]
            df = pd.concat(dfs, ignore_index=True)
            
            df_norm = normalize_brewery_df(df)
            silver_pipeline(df_norm, silver_path_fact, silver_path_dim, ds, log, i)

    @task()
    def remove_duplicates(ds = None, log = log):
        context = get_current_context()
        ds = context["ds"]  # string "YYYY-MM-DD"
        remove_duplicates_batch(ds, SILVER_PATH_FACT, log)
 
    @task(outlets=[DATASET_GOLD_PATH])
    def trigger_gold(log = log):

        log.info("Finalizada transformação para camada silver.")
    
    update_dim() >> transformation() >> remove_duplicates() >> trigger_gold()

transformation_silver()