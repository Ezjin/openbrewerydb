import os
import tempfile
import pytest
from pathlib import Path

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # /openbrewerydb
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

@pytest.fixture
def tmpdir_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)

@pytest.fixture
def env_clean(monkeypatch):
    # isola variáveis de ambiente entre testes
    for k in list(os.environ.keys()):
        if k.startswith("OPENBREWERYDB_"):
            monkeypatch.delenv(k, raising=False)
    return monkeypatch