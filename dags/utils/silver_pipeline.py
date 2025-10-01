import json
import os
import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowFailException

def silver_pipeline(df_raw, save_path, date, log):
    
    dim_country_df = pd.read_parquet("./data_lake_mock/silver/dim/dim_country.parquet")
    dim_state_df = pd.read_parquet("./data_lake_mock/silver/dim/dim_state.parquet")
    dim_city_df = pd.read_parquet("./data_lake_mock/silver/dim/dim_city.parquet")
    
    df = (df_raw.merge(dim_country_df, on="country", how="left")
          .merge(dim_state_df, on="state", how="left")
          .merge(dim_city_df, on="city", how="left")
    )

    if not df.empty:
        try:
            df.drop(["country", "state", "city"], axis=1, inplace=True)
            df.rename(
                columns={
                    "country_norm" : "country",
                    "state_norm" : "state",
                    "city_norm" : "city"
                },
                inplace=True
            )

            for country in df["country"].unique():
                log.debug("Processando país: %s", country)
                df_country = df[df["country"] == country]
                for state in df_country["state"].unique():
                    log.debug("Processando estado: %s (país: %s)", state, country)
                    df_state = df_country[df_country["state"] == state]
                    for city in df_state["city"].unique():
                        log.debug("Processando cidade: %s (estado: %s, país: %s)", city, state, country)
                        
                        df_city = df_state[df_state["city"] == city]

                        save_dir = os.path.join(
                            save_path,
                            f"country={country}",
                            f"state={state}",
                            f"city={city}",
                            f"batch={date}"
                        )
                        os.makedirs(save_dir, exist_ok=True)
                        
                        save_filepath = os.path.join(save_dir, "breweries.parquet")
                        df_city.to_parquet(save_filepath, engine="fastparquet", index=False, append=os.path.exists(save_filepath))
                        log.info("Arquivo salvo em %s", save_filepath)

        except Exception as e:
            log.exception(f"Erro geral ao processar e salvar dados: {e}")
            raise AirflowFailException(f"silver_pipeline falhou: {e}")
    else:
        raise AirflowFailException(f"Problema na criação do dataframe normalizado.")                

                






