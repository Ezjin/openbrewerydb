import json
import os
import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowFailException

def silver_pipeline(read_path, save_path, log):
    """
    Lê um arquivo JSON e salva em um arquivo .parquet em sua particao Country/State/City/batch.

    Args:
        read_path (str): Caminho do arquivo JSON.
        save_path (str): Caminho do arquivo PARQUET.
        log (structlog): Criar com LoggingMixin().log.

    Returns:
        dict | list: Conteúdo do JSON.
    """
     
    for file in os.listdir(read_path):
            read_filepath = os.path.join(read_path, file)

            if read_filepath.endswith(".json"):
                try:
                    with open(read_filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except FileNotFoundError:
                    log.error(f"Arquivo não encontrado: {read_filepath}")
                    return None
                except json.JSONDecodeError as e:
                    log.error(f"Erro ao decodificar JSON: {e}")
                    return None
            else:
                log.error(f"Arquivo com extensão errada: {read_filepath}")
            
            if data:
                try:
                    df = pd.DataFrame(data)
                    today = datetime.today().date()

                    log.info("DataFrame criado com %d linhas e colunas: %s", len(df), df.columns.tolist())

                    for country in df["country"].unique():
                        log.debug("Processando país: %s", country)
                        df_country = df[df["country"] == country]
                        country = country.lower().replace(" ", "_")
                        for state in df_country["state"].unique():
                            log.debug("Processando estado: %s (país: %s)", state, country)
                            df_state = df_country[df_country["state"] == state]
                            state = state.lower().replace(" ", "_")
                            for city in df_state["city"].unique():
                                log.debug("Processando cidade: %s (estado: %s, país: %s)", city, state, country)
                                city = city.lower().replace(" ", "_")
                                
                                save_dir = os.path.join(
                                    save_path,
                                    f"country={country}",
                                    f"state={state}",
                                    f"city={city}",
                                    f"batch={today}"
                                )
                                os.makedirs(save_dir, exist_ok=True)
                                
                                save_filepath = os.path.join(save_dir, "breweries.parquet")
                                
                                #if os.path.exists(save_filepath):
                                    
                                #    df_existing = pd.read_parquet(save_filepath, engine="pyarrow")
                                    
                                #    df_to_save = pd.concat([df_existing, df])
                                #else:
                                #    df_to_save = df

                                #df_to_save.to_parquet(save_filepath, engine="pyarrow", index=False)
                                df.to_parquet(save_filepath, engine="fastparquet", index=False, append=os.path.exists(save_filepath))
                                log.info("Arquivo salvo em %s", save_filepath)
                except Exception as e:
                    log.exception("Erro geral ao processar e salvar dados: {e}")
                    raise AirflowFailException(f"silver_pipeline falhou: {e}")
                

                






