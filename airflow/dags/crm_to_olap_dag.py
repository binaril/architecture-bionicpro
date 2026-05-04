"""
DAG: crm_to_olap
Schedule: every hour

Reads customers from a CSV file (CRM export) and incrementally loads them
into the OLAP database (ClickHouse):
  /opt/airflow/data/customers.csv  →  stg_customers  →  dim_customers
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

CSV_PATH = "/opt/airflow/data/customers.csv"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def get_ch_client() -> Client:
    return Client(
        host=os.environ.get("CLICKHOUSE_HOST", "olap_db"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        database=os.environ.get("CLICKHOUSE_DB", "olap_db"),
        user=os.environ.get("CLICKHOUSE_USER", "olap_user"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "olap_password"),
    )


def extract_load_customers(**_):
    """Read customers.csv and insert rows into stg_customers."""
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

    client = get_ch_client()
    client.execute(
        "INSERT INTO stg_customers (customer_id, name, email) VALUES",
        rows,
    )
    logger.info("stg_customers updated: %d row(s)", len(rows))


def upsert_dim_customers(**_):
    """Merge stg_customers into dim_customers."""
    client = get_ch_client()
    client.execute(
        """
        INSERT INTO dim_customers (customer_id, name, email, is_current)
        SELECT customer_id, name, email, 1
        FROM stg_customers FINAL
        """
    )
    logger.info("dim_customers upserted")


def load_fct_telemetry(**_):
    """Insert new telemetry rows from stg_telemetry into fct_telemetry."""
    client = get_ch_client()
    client.execute(
        """
        INSERT INTO fct_telemetry
            (event_id, customer_id, device_id, device_serial, event_type, event_value, recorded_at)
        SELECT
            s.event_id,
            dd.customer_id,
            dd.device_id,
            s.device_serial,
            s.event_type,
            s.event_value,
            s.recorded_at
        FROM (SELECT * FROM stg_telemetry FINAL) AS s
        LEFT JOIN (SELECT * FROM dim_devices FINAL) AS dd ON dd.device_serial = s.device_serial
        WHERE s.event_id NOT IN (SELECT event_id FROM fct_telemetry)
        """
    )
    logger.info("fct_telemetry loaded")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="crm_to_olap",
    default_args=default_args,
    description="ETL: CRM CSV → OLAP ClickHouse (staging + dimensions + facts)",
    schedule_interval="0 * * * *",
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
