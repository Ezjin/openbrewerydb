# tests/dags/test_transformation_silver_airflow_only.py
import sys
import types
import importlib
from datetime import date
from airflow.datasets import Dataset

# Ajuste se o arquivo tiver outro nome/caminho:
MODULE_PATH = "dags.dag_transformation_silver"

# ------------------------------------------------------------------
# Stubs p/ "utils.*" (não chamaremos nada — só evitar import errors)
# ------------------------------------------------------------------
sys.modules.setdefault("utils", types.ModuleType("utils"))

def _assert_not_called(*args, **kwargs):
    raise AssertionError("Função de utils não deve ser chamada neste teste.")

# utils.silver_pipeline
mod_sp = types.ModuleType("utils.silver_pipeline")
mod_sp.silver_pipeline = _assert_not_called
sys.modules["utils.silver_pipeline"] = mod_sp

# utils.update_dim
mod_ud = types.ModuleType("utils.update_dim")
mod_ud.update_dim = _assert_not_called
sys.modules["utils.update_dim"] = mod_ud

# utils.normalization
mod_norm = types.ModuleType("utils.normalization")
mod_norm.normalize_name = _assert_not_called
mod_norm.normalize_brewery_df = _assert_not_called
sys.modules["utils.normalization"] = mod_norm

# utils.remove_duplicates_batch
mod_rdb = types.ModuleType("utils.remove_duplicates_batch")
mod_rdb.remove_duplicates_batch = _assert_not_called
sys.modules["utils.remove_duplicates_batch"] = mod_rdb

# utils.context_utils
mod_ctx = types.ModuleType("utils.context_utils")
mod_ctx.get_run_day = lambda: "2025-09-27"  # não será chamado aqui
sys.modules["utils.context_utils"] = mod_ctx

# ------------------------------------------------------------------
# Importa o módulo da DAG
# ------------------------------------------------------------------
dag_mod = importlib.import_module(MODULE_PATH)


def _get_dag():
    dag = dag_mod.transformation_silver()
    assert dag is not None
    return dag


def test_dag_basics_and_schedule():
    dag = _get_dag()

    # Há schedule (dataset-based timetable), sem assumir valor específico
    assert getattr(dag, "schedule", None) or getattr(dag, "schedule_interval", None)

    # start_date (ignore TZ)
    assert dag.start_date is not None
    assert dag.start_date.date() == date(2025, 9, 27)
    assert dag.catchup is False

    # tasks presentes
    tids = {t.task_id for t in dag.tasks}
    assert {"update_dimensions", "transformation", "remove_duplicates", "trigger_gold"} <= tids


def test_task_dependencies_and_outlets():
    dag = _get_dag()
    t_upd = dag.get_task("update_dimensions")
    t_trf = dag.get_task("transformation")
    t_rm  = dag.get_task("remove_duplicates")
    t_trg = dag.get_task("trigger_gold")

    # update_dimensions -> transformation -> remove_duplicates -> trigger_gold
    assert t_trf in t_upd.downstream_list
    assert t_rm  in t_trf.downstream_list
    assert t_trg in t_rm.downstream_list

    # trigger_gold publica o Dataset esperado
    assert hasattr(t_trg, "outlets") and len(t_trg.outlets) == 1
    assert isinstance(t_trg.outlets[0], Dataset)
    assert str(t_trg.outlets[0].uri) == str(dag_mod.DATASET_GOLD_PATH.uri)
