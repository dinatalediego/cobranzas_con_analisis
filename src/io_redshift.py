from __future__ import annotations
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
load_dotenv()  # carga el archivo .env al entorno

def read_sql(query: str) -> pd.DataFrame:
    """Read a query from Redshift using env vars REDSHIFT_*"""
    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        #port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        port = _get_int_env("REDSHIFT_PORT", 5439)
        dbname=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

import os

def _get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    raw = str(raw).strip().strip('"').strip("'")
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer. Got: {raw!r}") from e
