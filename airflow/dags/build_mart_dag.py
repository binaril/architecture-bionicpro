"""
DAG: build_mart
Schedule: daily at 01:00

Builds (refreshes) the data mart mart_customer_telemetry from
fct_telemetry + dim_customers + dim_devices stored in ClickHouse.

Витрина денормализована и оптимизирована для быстрого доступа
по customer_id / email / company в сервисе отчётов.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def get_ch_client() -> Client:
    return Client(
        host=os.environ.get("CLICKHOUSE_HOST", "olap_db"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        database=os.environ.get("CLICKHOUSE_DB", "olap_db"),
        user=os.environ.get("CLICKHOUSE_USER", "olap_user"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "olap_password"),
    )


# ── SQL for mart refresh ──────────────────────────────────────────────────────

SQL_REFRESH_MART = """
INSERT INTO mart_customer_telemetry (
    customer_id, customer_name, email, phone, company, crm_created_at,
    device_count, active_devices, device_types,
    total_events, avg_event_value, max_event_value, min_event_value,
    first_event_at, last_event_at,
    events_30d, avg_value_30d,
    events_by_type, mart_updated_at
)
SELECT
    dc.customer_id,
    dc.name                                                                        AS customer_name,
    dc.email,
    dc.phone,
    dc.company,
    dc.crm_created_at,

    -- устройства
    uniqExact(dd.device_id)                                                        AS device_count,
    uniqExactIf(dd.device_id, dd.is_active = 1)                                   AS active_devices,
    nullIf(
        arrayStringConcat(
            arrayDistinct(groupArrayIf(assumeNotNull(dd.device_type), isNotNull(dd.device_type))),
            ', '
        ), ''
    )                                                                              AS device_types,

    -- телеметрия за всё время
    count(ft.event_id)                                                             AS total_events,
    round(avg(ft.event_value), 2)                                                  AS avg_event_value,
    round(max(ft.event_value), 2)                                                  AS max_event_value,
    round(min(ft.event_value), 2)                                                  AS min_event_value,
    min(ft.recorded_at)                                                            AS first_event_at,
    max(ft.recorded_at)                                                            AS last_event_at,

    -- телеметрия за последние 30 дней
    countIf(ft.recorded_at >= now() - INTERVAL 30 DAY)                            AS events_30d,
    round(avgIf(ft.event_value, ft.recorded_at >= now() - INTERVAL 30 DAY), 2)    AS avg_value_30d,

    -- разбивка по типам событий
    ifNull(ec.events_by_type, '{}')                                                AS events_by_type,

    now()                                                                          AS mart_updated_at

FROM (SELECT * FROM dim_customers FINAL) AS dc
LEFT JOIN (SELECT * FROM dim_devices   FINAL) AS dd ON dd.customer_id = dc.customer_id
LEFT JOIN (SELECT * FROM fct_telemetry FINAL) AS ft ON ft.customer_id = dc.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        toJSONString(
            mapFromArrays(groupArray(event_type), groupArray(toUInt64(cnt)))
        ) AS events_by_type
    FROM (
        SELECT customer_id, event_type, count() AS cnt
        FROM fct_telemetry FINAL
        WHERE isNotNull(event_type) AND isNotNull(customer_id)
        GROUP BY customer_id, event_type
    )
    GROUP BY customer_id
) AS ec ON ec.customer_id = dc.customer_id

GROUP BY
    dc.customer_id, dc.name, dc.email, dc.phone, dc.company, dc.crm_created_at,
    ec.events_by_type
"""

SQL_REMOVE_DELETED = """
ALTER TABLE mart_customer_telemetry
DELETE WHERE customer_id NOT IN (SELECT customer_id FROM dim_customers FINAL)
"""


# ── Task callables ────────────────────────────────────────────────────────────

def validate_sources(**_):
    """Check that source tables are not empty before building the mart."""
    client = get_ch_client()

    result = client.execute("SELECT count() FROM dim_customers FINAL")
    customer_count = result[0][0] if result else 0

    if customer_count == 0:
        raise ValueError("dim_customers is empty — crm_to_olap DAG must run first")

    result = client.execute("SELECT count() FROM fct_telemetry FINAL")
    event_count = result[0][0] if result else 0

    logger.info(
        "Source validation passed: %d customers, %d telemetry events",
        customer_count,
        event_count,
    )


def refresh_mart(**_):
    """Truncate and rebuild mart_customer_telemetry."""
    client = get_ch_client()
    client.execute("TRUNCATE TABLE mart_customer_telemetry")
    client.execute(SQL_REFRESH_MART)
    logger.info("mart_customer_telemetry refreshed")


def remove_stale_rows(**_):
    """Remove mart rows for customers that no longer exist in dim_customers."""
    client = get_ch_client()
    client.execute(SQL_REMOVE_DELETED)
    logger.info("Stale mart rows removed")


def log_mart_stats(**_):
    """Log row count and coverage stats after the refresh."""
    client = get_ch_client()
    result = client.execute(
        """
        SELECT
            count()                                                              AS total_customers,
            sum(total_events)                                                    AS total_events,
            countIf(last_event_at >= now() - INTERVAL 30 DAY)                   AS active_30d,
            max(mart_updated_at)                                                 AS last_refresh
        FROM mart_customer_telemetry FINAL
        """
    )
    if result:
        total_customers, total_events, active_30d, last_refresh = result[0]
        logger.info(
            "Mart stats — customers: %s | total events: %s | active 30d: %s | refreshed: %s",
            total_customers, total_events, active_30d, last_refresh,
        )


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="build_mart",
    default_args=default_args,
    description="Daily refresh of mart_customer_telemetry (витрина клиентской телеметрии)",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mart", "olap", "reports"],
) as dag:

    task_validate = PythonOperator(
        task_id="validate_sources",
        python_callable=validate_sources,
    )

    task_refresh = PythonOperator(
        task_id="refresh_mart",
        python_callable=refresh_mart,
    )

    task_cleanup = PythonOperator(
        task_id="remove_stale_rows",
        python_callable=remove_stale_rows,
    )

    task_stats = PythonOperator(
        task_id="log_mart_stats",
        python_callable=log_mart_stats,
    )

    task_validate >> task_refresh >> task_cleanup >> task_stats
