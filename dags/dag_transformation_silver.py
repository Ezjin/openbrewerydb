from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from datetime import datetime
from pathlib import Path
from glob import glob
import os
import pandas as pd

from utils.silver_pipeline import silver_pipeline           
from utils.update_dim import update_dim              
from utils.normalization import normalize_name, normalize_brewery_df
from utils.remove_duplicates_batch import remove_duplicates_batch  
from utils.context_utils import get_run_day

log = LoggingMixin().log

RAW_PATH = "data_lake_mock/raw"
SILVER_PATH_DIM = "data_lake_mock/silver/dim"
SILVER_PATH_FACT = "data_lake_mock/silver/fact"
DATASET_SILVER_PATH = Dataset("/logs/trigger_silver.csv")
DATASET_GOLD_PATH = Dataset("/logs/trigger_gold.csv")


@dag(
    schedule=[DATASET_SILVER_PATH],
    start_date=datetime(2025, 9, 27),
    description="Transformação para a camada Silver - JSON -> Parquet particionado",
    tags=["transformation", "silver", "brewery"],
    catchup=False,
)
def transformation_silver():

    @task()
    def update_dimensions(raw_path: str = RAW_PATH,
                          silver_path_dim: str = SILVER_PATH_DIM,
                          batch_size: int = 10) -> None:
        
        day_run = get_run_day()
        year, month, day = day_run.split("-")

        read_path = os.path.join(
            raw_path,
            f"year={year}",
            f"month={int(month):02d}",
            f"day={int(day):02d}",
        )
        Path(silver_path_dim).mkdir(parents=True, exist_ok=True)

        files = sorted(glob(os.path.join(read_path, "*.json")))
        log.info("update_dimensions: path=%s files=%s", read_path, len(files))
        if not files:
            log.warning("Nenhum arquivo JSON encontrado em %s (run %s).", read_path, day_run)
            # Não falha a DAG: apenas não há nada para atualizar.
            return

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i + batch_size]
            log.info("Batch %s: %s arquivos", i // batch_size + 1, len(batch_files))
            try:
                dfs = [pd.read_json(f) for f in batch_files]
                df = pd.concat(dfs, ignore_index=True)
                if df.empty:
                    log.warning("Batch vazio após concatenação; pulando.")
                    continue
            except Exception as e:
                log.exception("Falha ao ler/concatenar JSONs do batch: %s", batch_files)
                raise AirflowFailException(f"Erro de leitura de JSON: {e}") from e

            # Normalizações de chave para dimensões
            df["country_norm"] = df.get("country").map(normalize_name) if "country" in df else None
            df["state_norm"]   = df.get("state").map(normalize_name)     if "state"   in df else None
            df["city_norm"]    = df.get("city").map(normalize_name)      if "city"    in df else None

            # Atualiza dims (função já valida/loga)
            update_dim(df[["country", "country_norm"]].dropna(), "country", "country_norm",
                       os.path.join(silver_path_dim, "dim_country.parquet"))
            update_dim(df[["state", "state_norm"]].dropna(), "state", "state_norm",
                       os.path.join(silver_path_dim, "dim_state.parquet"))
            update_dim(df[["city", "city_norm"]].dropna(), "city", "city_norm",
                       os.path.join(silver_path_dim, "dim_city.parquet"))

    @task()
    def transformation(raw_path: str = RAW_PATH,
                       silver_path_fact: str = SILVER_PATH_FACT,
                       silver_path_dim: str = SILVER_PATH_DIM,
                       batch_size: int = 10) -> None:
        
        day_run = get_run_day()
        year, month, day = day_run.split("-")

        read_path = os.path.join(
            raw_path,
            f"year={year}",
            f"month={int(month):02d}",
            f"day={int(day):02d}",
        )

        files = sorted(glob(os.path.join(read_path, "*.json")))
        log.info("transformation: path=%s files=%s", read_path, len(files))
        if not files:
            log.warning("Nenhum arquivo JSON encontrado em %s (run %s).", read_path, day_run)
            return

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i + batch_size]
            log.info("Batch %s: %s arquivos", i // batch_size + 1, len(batch_files))
            try:
                dfs = [pd.read_json(f) for f in batch_files]
                df = pd.concat(dfs, ignore_index=True)
                if df.empty:
                    log.warning("Batch vazio após concatenação; pulando.")
                    continue
            except Exception as e:
                log.exception("Falha ao ler/concatenar JSONs do batch: %s", batch_files)
                raise AirflowFailException(f"Erro de leitura de JSON: {e}") from e

            df_norm = normalize_brewery_df(df)  # já valida e loga
            # silver_pipeline já usa LoggingMixin().log; `part=i` garante particionamento estável
            silver_pipeline(df_norm, silver_path_fact, silver_path_dim, day_run, part=i)

    @task()
    def remove_duplicates() -> None:
        
        day_run = get_run_day()
        # função já usa logger interno
        remove_duplicates_batch(day_run, SILVER_PATH_FACT)

    @task(outlets=[DATASET_GOLD_PATH])
    def trigger_gold() -> None:
        log.info("Finalizada transformação para camada silver; dataset_gold atualizado.")

    update_dimensions() >> transformation() >> remove_duplicates() >> trigger_gold()


transformation_silver()
