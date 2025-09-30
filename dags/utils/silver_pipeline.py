import json
import os
import pandas as pd
from datetime import datetime

def silver_pipeline(read_path, save_path, log):
    """
    Lê um arquivo JSON e retorna seu conteúdo como um objeto Python (dict ou list).

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
                    for country in df["country"].unique():
                        df_country = df[df["country"] == country]
                        country = country.lower().replace(" ", "_")
                        for state in df_country["state"].unique():
                            df_state = df_country[df_country["state"] == state]
                            state = state.lower().replace(" ", "_")
                            for city in df_state["city"].unique():
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
                                df.to_parquet(save_filepath, engine="fastparquet", index=False, append=os.pah.exist(save_filepath))
                except:
                    log.error("Deu errado")
                

                






