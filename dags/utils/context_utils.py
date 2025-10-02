# utils/context_utils.py
from __future__ import annotations
from airflow.operators.python import get_current_context
import pendulum
from typing import Optional

def _to_pendulum(dt_or_str) -> pendulum.DateTime:
    if hasattr(dt_or_str, "in_timezone"):  # já é pendulum
        return dt_or_str
    if isinstance(dt_or_str, str):
        # tenta parsear TS iso (contexto costuma trazer em UTC)
        try:
            return pendulum.parse(dt_or_str)
        except Exception:
            pass
    # fallback: agora em UTC
    return pendulum.now("UTC")

def get_run_day(tz: Optional[str] = None) -> str:
    """
    Retorna YYYY-MM-DD da execução atual, robusto a diferentes chaves de contexto:
    tenta logical_date, data_interval_start, execution_date, ts, ds.
    """
    ctx = get_current_context()

    candidates = [
        ctx.get("logical_date"),
        ctx.get("data_interval_start"),
        ctx.get("execution_date"),
        ctx.get("ts"),  # string ISO
        ctx.get("ds"),  # string YYYY-MM-DD
    ]

    for c in candidates:
        if c is None:
            continue
        if isinstance(c, str) and len(c) == 10 and c.count("-") == 2:
            # já é YYYY-MM-DD
            return c
        dt = _to_pendulum(c)
        if tz:
            dt = dt.in_timezone(tz)
        return dt.strftime("%Y-%m-%d")

    # último recurso: agora (UTC ou TZ pedida)
    now = pendulum.now(tz or "UTC")
    return now.strftime("%Y-%m-%d")
