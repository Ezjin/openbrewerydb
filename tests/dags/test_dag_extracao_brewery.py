import types
import sys
import importlib
from datetime import timedelta, date
from airflow.datasets import Dataset


MODULE_PATH = "dags.dag_extracao_brewery"

# ------------------------------------------------------------------
# Stubs para evitar dependência dos módulos utils (não serão chamados)
# ------------------------------------------------------------------
# cria pacote 'utils'
sys.modules.setdefault("utils", types.ModuleType("utils"))

# submódulo utils.get_api_data com função dummy
mod_get = types.ModuleType("utils.get_api_data")
def _get_api_data_stub(*_, **__):
    raise AssertionError("get_api_data não deve ser chamado neste teste")
mod_get.get_api_data = _get_api_data_stub
sys.modules["utils.get_api_data"] = mod_get

# submódulo utils.save_api_data com função dummy
mod_save = types.ModuleType("utils.save_api_data")
def _save_api_data_stub(*_, **__):
    raise AssertionError("save_api_data não deve ser chamado neste teste")
mod_save.save_api_data = _save_api_data_stub
sys.modules["utils.save_api_data"] = mod_save

# ------------------------------------------------------------------
# Importa o módulo da DAG
# ------------------------------------------------------------------
dag_mod = importlib.import_module(MODULE_PATH)


def _get_dag():
    # A função decorada retorna um callable que, ao ser chamado, instancia a DAG
    dag = dag_mod.extracao_brewery()
    assert dag is not None
    return dag

def test_dag_metadata_e_tasks():
    dag = _get_dag()

    # schedule / start_date / tags / catchup
    assert getattr(dag, "schedule", None) or getattr(dag, "schedule_interval", None)
    # start_date definido no decorator
    assert dag.start_date.date() == date(2025, 9, 27)  
    assert dag.catchup is False

    # tasks
    tids = {t.task_id for t in dag.tasks}
    assert {"get_total_pages", "get_api_task", "trigger_silver"} <= tids

    # dependências: total_pages -> get_api_task -> trigger_silver
    t_total = dag.get_task("get_total_pages")
    t_get   = dag.get_task("get_api_task")
    t_trig  = dag.get_task("trigger_silver")
    assert t_get in t_total.downstream_list
    assert t_trig in t_get.downstream_list


def test_task_configs_airflow_only():
    dag = _get_dag()
    t_total = dag.get_task("get_total_pages")
    t_get   = dag.get_task("get_api_task")
    t_trig  = dag.get_task("trigger_silver")

    # retries e retry_delay configurados nas @task
    assert t_total.retries == 3
    assert t_total.retry_delay == timedelta(seconds=60)
    assert t_get.retries == 3
    assert t_get.retry_delay == timedelta(seconds=60)

    # trigger_silver publica Dataset correto
    assert hasattr(t_trig, "outlets")
    assert len(t_trig.outlets) == 1
    ds = t_trig.outlets[0]
    assert isinstance(ds, Dataset)
    # compara com a constante do módulo
    assert str(ds.uri) == str(dag_mod.DATASET_PATH.uri)
