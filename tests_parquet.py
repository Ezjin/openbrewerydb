import pandas as pd

df = pd.read_parquet("./data_lake_mock/silver/dim/dim_state.parquet")

df[df["state"] == "Westmeath"]




import os
from glob import glob
from datetime import datetime
from dags.utils.normalization import normalize_brewery_df
today = datetime.today()

RAW_PATH = "data_lake_mock/raw"
read_path = os.path.join(
            RAW_PATH,
            f"year={today.year}",
            f"month={today.month:02d}",
            f"day={today.day:02d}"
        )
files = glob(os.path.join(read_path, "*.json"))
for i in range(0, 1, 10):
    batch_files = files[i:i+10]
    

    dfs = [pd.read_json(f) for f in batch_files]
    df = pd.concat(dfs, ignore_index=True)
    
    df_norm = normalize_brewery_df(df)
    df = (df_norm.merge(dim_country_df, on="country", how="left")
            .merge(dim_state_df, on="state", how="left")
            .merge(dim_city_df, on="city", how="left"))

    # Substitui colunas originais pelos valores normalizados
    df = df.drop(["country", "state", "city"], axis=1)
    df = df.rename(columns={
        "country_norm": "country",
        "state_norm": "state",
        "city_norm": "city"
    })


    df[df["country"].isna()]


save_path = "./data_lake_mock/silver"
# Carrega dimensões
dim_country_df = pd.read_parquet(os.path.join(save_path, "dim/dim_country.parquet"))
dim_state_df   = pd.read_parquet(os.path.join(save_path, "dim/dim_state.parquet"))
dim_city_df    = pd.read_parquet(os.path.join(save_path, "dim/dim_city.parquet"))

# Merge com dimensões
df = (df_norm.merge(dim_country_df, on="country", how="left")
            .merge(dim_state_df, on="state", how="left")
            .merge(dim_city_df, on="city", how="left"))

# Substitui colunas originais pelos valores normalizados
df = df.drop(["country", "state", "city"], axis=1)
df = df.rename(columns={
    "country_norm": "country",
    "state_norm": "state",
    "city_norm": "city"
})


df[df["city"].isna()]