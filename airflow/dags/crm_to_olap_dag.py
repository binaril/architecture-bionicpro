"""
DAG: crm_to_olap
Schedule: every hour

Reads customers from a CSV file (CRM export) and incrementally loads them
into the OLAP database:
  /opt/airflow/data/customers.csv  →  stg_customers  →  dim_customers
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

OLAP_CONN_ID  = "olap_db"
CSV_PATH      = "/opt/airflow/data/customers.csv"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_load_customers(**_):
    """Read customers.csv and upsert rows into stg_customers."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CRM CSV not found: {CSV_PATH}")

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (int(r["customer_id"]), r["name"].strip(), r["email"].strip())
            for r in reader
            if r["customer_id"].strip()
        ]

    if not rows:
        logger.info("CSV is empty, nothing to load")
        return

    logger.info("Loaded %d row(s) from %s", len(rows), CSV_PATH)

    olap   = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
    conn   = olap.get_conn()
    cursor = conn.cursor()

    cursor.executemany(
        """
        INSERT INTO stg_customers
            (customer_id, name, email, loaded_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (customer_id) DO UPDATE SET
            name      = EXCLUDED.name,
            email     = EXCLUDED.email,
            loaded_at = NOW()
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()

    logger.info("stg_customers updated: %d row(s)", len(rows))


def upsert_dim_customers(**_):
    """Merge stg_customers into dim_customers."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
    olap.run(
        """
        INSERT INTO dim_customers
            (customer_id, name, email, is_current, loaded_at)
        SELECT customer_id, name, email, TRUE, NOW()
        FROM   stg_customers
        ON CONFLICT (customer_id) DO UPDATE SET
            name      = EXCLUDED.name,
            email     = EXCLUDED.email,
            loaded_at = NOW()
        """
    )
    logger.info("dim_customers upserted")


def load_fct_telemetry(**_):
    """Insert new telemetry rows from stg_telemetry into fct_telemetry."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
    olap.run(
        """
        INSERT INTO fct_telemetry
            (event_id, device_sk, customer_sk, customer_id,
             device_serial, event_type, event_value, recorded_at, loaded_at)
        SELECT
            s.event_id,
            dd.device_sk,
            dc.customer_sk,
            dc.customer_id,
            s.device_serial,
            s.event_type,
            s.event_value,
            s.recorded_at,
            NOW()
        FROM       stg_telemetry  s
        LEFT JOIN  dim_devices    dd ON dd.device_serial = s.device_serial
        LEFT JOIN  dim_customers  dc ON dc.customer_id   = dd.customer_id
        ON CONFLICT (event_id) DO NOTHING
        """
    )
    logger.info("fct_telemetry loaded")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="crm_to_olap",
    default_args=default_args,
    description="ETL: CRM CSV → OLAP (staging + dimensions + facts)",
    schedule_interval="0 * * * *",   # каждый час
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "crm", "olap"],
) as dag:

    task_customers = PythonOperator(
        task_id="extract_load_customers",
        python_callable=extract_load_customers,
    )

    task_dim_customers = PythonOperator(
        task_id="upsert_dim_customers",
        python_callable=upsert_dim_customers,
    )

    task_fct_telemetry = PythonOperator(
        task_id="load_fct_telemetry",
        python_callable=load_fct_telemetry,
    )

    task_customers >> task_dim_customers >> task_fct_telemetry
