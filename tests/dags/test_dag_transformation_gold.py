# tests/dags/test_transformation_gold_airflow_only.py
import sys
import types
import importlib
from datetime import date
from airflow.datasets import Dataset


# Ajuste se o caminho/nome do arquivo da DAG for outro
MODULE_PATH = "dags.dag_transformation_gold"

# ------------------------------
# Stubs para módulos utils.*
# (evitam import error e garantem que nada é chamado)
# ------------------------------
sys.modules.setdefault("utils", types.ModuleType("utils"))

mod_gold = types.ModuleType("utils.gold_pipeline")
def _stub_gold_pipeline(*args, **kwargs):
    raise AssertionError("gold_pipeline não deve ser chamado neste teste.")
mod_gold.gold_pipeline = _stub_gold_pipeline
sys.modules["utils.gold_pipeline"] = mod_gold

mod_ctx = types.ModuleType("utils.context_utils")
mod_ctx.get_run_day = lambda: "2025-09-27"  # não será chamado aqui
sys.modules["utils.context_utils"] = mod_ctx

# ------------------------------
# Importa o módulo da DAG
# ------------------------------
dag_mod = importlib.import_module(MODULE_PATH)


def _get_dag():
    dag = dag_mod.transformation_gold()
    assert dag is not None
    return dag


def test_dag_basics_and_schedule():
    dag = _get_dag()

    # Há schedule (dataset-based), sem assumir valor exato
    assert getattr(dag, "schedule", None) or getattr(dag, "schedule_interval", None)
    
    # start_date (ignora timezone)
    assert dag.start_date is not None
    assert dag.start_date.date() == date(2025, 9, 27)
    assert dag.catchup is False

    # tasks presentes
    tids = {t.task_id for t in dag.tasks}
    assert {"aggregation_silver_to_gold"} <= tids


def test_task_outlets_and_wiring():
    dag = _get_dag()
    t_agg = dag.get_task("aggregation_silver_to_gold")

    # Não há upstreams/downs explicitas além dela mesma (DAG tem 1 task)
    assert not t_agg.upstream_list
    assert not t_agg.downstream_list

    # A DAG agenda por Dataset; apenas valida que a constante existe e é Dataset
    assert isinstance(dag_mod.DATASET_GOLD_PATH, Dataset)
