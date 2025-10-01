import os
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from airflow.exceptions import AirflowFailException

def silver_pipeline(df_raw, save_path, date, log, part=1):
    try:
        # Carrega dimensões
        dim_country_df = pd.read_parquet(os.path.join(save_path, "dim/dim_country.parquet"))
        dim_state_df   = pd.read_parquet(os.path.join(save_path, "dim/dim_state.parquet"))
        dim_city_df    = pd.read_parquet(os.path.join(save_path, "dim/dim_city.parquet"))

        # Merge com dimensões
        df = (df_raw.merge(dim_country_df, on="country", how="left")
                    .merge(dim_state_df, on="state", how="left")
                    .merge(dim_city_df, on="city", how="left"))

        if df.empty:
            raise AirflowFailException("Problema na criação do dataframe normalizado.")

        # Substitui colunas originais pelos valores normalizados
        df = df.drop(["country", "state", "city"], axis=1)
        df = df.rename(columns={
            "country_norm": "country",
            "state_norm": "state",
            "city_norm": "city"
        })
        
        # Remove linhas com NA, que não sabemos o nível de agregação de nossa análise
        df = df.dropna(subset=["name", "country", "state", "city", "brewery_type"])

        for country in df["country"].unique():
            df_country = df[df["country"] == country]
            # Opcional: iterar por estado se necessário
            df_country["part"] = str(part)
            df_country["batch"] = str(date)
            
            ds.write_dataset(
                pa.Table.from_pandas(df_country),
                base_dir=save_path,
                format="parquet",
                partitioning=["batch", "country", "state", "part"],
                partitioning_flavor="hive",
                existing_data_behavior="overwrite_or_ignore"
            )

        log.info(f"Silver dataset salvo em {os.path.join(save_path, f'batch={date}')}")
        
    except Exception as e:
        log.exception(f"Erro ao processar e salvar dados: {e}")
        raise AirflowFailException(f"silver_pipeline falhou: {e}")
