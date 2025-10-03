import os
from typing import Iterable
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.required_columns import require_columns


def silver_pipeline(
    df_raw: pd.DataFrame,
    save_path_fact: str,
    save_path_dim: str,
    date: str | int,
    part: int = 1,
) -> None:
    """
    Normaliza e particiona o dataset 'raw' (country/state/city) com dimensões
    e grava em Parquet particionado: batch/country/state/part (formato Hive).

    Args:
        df_raw: DataFrame de entrada (camada bronze/raw).
        save_path_fact: Caminho base da fato silver.
        save_path_dim: Caminho onde estão as dimensões parquet (dim_country/state/city).
        date: Identificador do batch (ex.: '2025-09-27' ou run_id).
        part: Número da partição (útil para sharding do mesmo batch).

    Raises:
        AirflowFailException: Para qualquer falha de validação/IO.
    """
    log = LoggingMixin().log
    log.info(
        "Início silver_pipeline rows=%s cols=%s save_path_fact=%s part=%s batch=%s",
        len(df_raw) if df_raw is not None else "None",
        len(df_raw.columns) if df_raw is not None else "None",
        save_path_fact,
        part,
        date,
    )

    try:
        # Validação
        if df_raw is None or df_raw.empty:
            raise AirflowFailException("df_raw vazio ou None.")

        require_columns(
            df_raw, ["country", "state", "city", "name", "brewery_type"], "df_raw"
        )

        # Dimensões
        p_country = os.path.join(save_path_dim, "dim_country.parquet")
        p_state = os.path.join(save_path_dim, "dim_state.parquet")
        p_city = os.path.join(save_path_dim, "dim_city.parquet")
        p_brewery_type = os.path.join(save_path_dim, "dim_brewery_type.parquet")

        try:
            dim_country_df = pd.read_parquet(p_country)
            dim_state_df = pd.read_parquet(p_state)
            dim_city_df = pd.read_parquet(p_city)
            dim_brewery_type_df = pd.read_parquet(p_brewery_type)
        except Exception as e:
            log.exception("Falha ao ler dimensões em %s | %s | %s", p_country, p_state, p_city)
            raise AirflowFailException(f"Erro ao ler dimensões: {e}") from e

        require_columns(dim_country_df, ["country", "country_norm"], "dim_country")
        require_columns(dim_state_df, ["state", "state_norm"], "dim_state")
        require_columns(dim_city_df, ["city", "city_norm"], "dim_city")
        require_columns(dim_brewery_type_df, ["brewery_type", "brewery_type_norm"], "dim_brewery_type")

        # Merge Dimensões
        df = (
            df_raw.merge(dim_country_df, on="country", how="left")
                  .merge(dim_state_df, on="state", how="left")
                  .merge(dim_city_df, on="city", how="left")
                  .merge(dim_brewery_type_df, on="brewery_type", how="left")
                  .copy()
        )

        # Checa o que deu miss e dropa (Não é espero nenhum miss)
        miss_country = df["country_norm"].isna().sum()
        miss_state = df["state_norm"].isna().sum()
        miss_city = df["city_norm"].isna().sum()
        miss_brewery_type = df["brewery_type_norm"].isna().sum()

        if any([miss_country, miss_state, miss_city, miss_brewery_type]):
            log.warning(
                "Valores sem normalização: country=%s state=%s city=%s brewery_type=%s",
                miss_country, miss_state, miss_city, miss_brewery_type
            )

        if df.empty:
            raise AirflowFailException("DataFrame pós-merge ficou vazio.")

        # Substitui colunas
        require_columns(df, ["country_norm", "state_norm", "city_norm"], "pós-merge")
        df = (
            df.drop(["country", "state", "city", "brewery_type"], axis=1)
              .rename(columns={
                  "country_norm": "country",
                  "state_norm": "state",
                  "city_norm": "city",
                  "brewery_type_norm": "brewery_type"
              })
        )

        # Seleciona somente as linhas completas
        before = len(df)
        df = df.dropna(subset=["name", "country", "state", "city", "brewery_type"]).copy()
        after = len(df)
        if after == 0:
            raise AirflowFailException("Todos os registros foram descartados após dropna().")
        if after < before:
            log.info("Registros removidos por NA: %s -> %s (removidos=%s)", before, after, before - after)

        # Escrita
        # Garantindo ser string
        df["country"] = df["country"].astype(str)
        df["state"]   = df["state"].astype(str)
        batch_str = str(date)
        part_str  = str(part)

        # Escreve por país 
        unique_countries = df["country"].dropna().unique().tolist()
        log.info("Países a escrever: %s", unique_countries)

        for country in unique_countries:
            df_country = df[df["country"] == country].copy()
            if df_country.empty:
                log.warning("DataFrame vazio para country=%s, pulando.", country)
                continue

            df_country.loc[:, "batch"] = batch_str
            df_country.loc[:, "part"]  = part_str

            # Salva nas partições
            try:
                ds.write_dataset(
                    data=pa.Table.from_pandas(df_country, preserve_index=False),
                    base_dir=save_path_fact,
                    format="parquet",
                    partitioning=["batch", "country", "state", "part"],
                    partitioning_flavor="hive",
                    existing_data_behavior="overwrite_or_ignore",
                )
                log.info(
                    "Escrito: rows=%s batch=%s country=%s states=%s",
                    len(df_country), batch_str, country, df_country["state"].nunique()
                )
            except Exception as e:
                log.exception("Falha ao escrever parquet para country=%s batch=%s", country, batch_str)
                raise AirflowFailException(f"Erro ao escrever parquet: {e}") from e

        log.info("Silver salvo em %s/batch=%s", save_path_fact, batch_str)

    except AirflowFailException:
        raise
    except Exception as e:
        log.exception("Erro inesperado na silver_pipeline")
        raise AirflowFailException(f"silver_pipeline falhou: {e}") from e 
