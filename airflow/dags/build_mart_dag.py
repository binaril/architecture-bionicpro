"""
DAG: build_mart
Schedule: daily at 01:00

Builds (refreshes) the data mart mart_customer_telemetry from
fct_telemetry + dim_customers + dim_devices stored in the OLAP database.

Витрина денормализована и оптимизирована для быстрого доступа
по customer_id / email / company в сервисе отчётов.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

OLAP_CONN_ID = "olap_db"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# ── SQL for mart refresh ──────────────────────────────────────────────────────

SQL_REFRESH_MART = """
INSERT INTO mart_customer_telemetry (
    customer_id,
    customer_name,
    email,
    phone,
    company,
    crm_created_at,
    device_count,
    active_devices,
    device_types,
    total_events,
    avg_event_value,
    max_event_value,
    min_event_value,
    first_event_at,
    last_event_at,
    events_30d,
    avg_value_30d,
    events_by_type,
    mart_updated_at
)
SELECT
    dc.customer_id,
    dc.name                                             AS customer_name,
    dc.email,
    dc.phone,
    dc.company,
    dc.crm_created_at,

    -- устройства
    COUNT(DISTINCT dd.device_id)                        AS device_count,
    COUNT(DISTINCT dd.device_id) FILTER (WHERE dd.is_active)
                                                        AS active_devices,
    STRING_AGG(DISTINCT dd.device_type, ', ' ORDER BY dd.device_type)
                                                        AS device_types,

    -- телеметрия за всё время
    COUNT(ft.event_id)                                  AS total_events,
    ROUND(AVG(ft.event_value)::numeric, 2)              AS avg_event_value,
    ROUND(MAX(ft.event_value)::numeric, 2)              AS max_event_value,
    ROUND(MIN(ft.event_value)::numeric, 2)              AS min_event_value,
    MIN(ft.recorded_at)                                 AS first_event_at,
    MAX(ft.recorded_at)                                 AS last_event_at,

    -- телеметрия за последние 30 дней
    COUNT(ft.event_id) FILTER (WHERE ft.recorded_at >= NOW() - INTERVAL '30 days')
                                                        AS events_30d,
    ROUND(
        AVG(ft.event_value) FILTER (WHERE ft.recorded_at >= NOW() - INTERVAL '30 days')::numeric,
        2
    )                                                   AS avg_value_30d,

    -- разбивка по типам событий
    COALESCE(
        jsonb_object_agg(
            event_counts.event_type,
            event_counts.cnt
        ) FILTER (WHERE event_counts.event_type IS NOT NULL),
        '{}'::jsonb
    )                                                   AS events_by_type,

    NOW()                                               AS mart_updated_at

FROM dim_customers dc
LEFT JOIN dim_devices   dd ON dd.customer_id  = dc.customer_id
LEFT JOIN fct_telemetry ft ON ft.customer_id  = dc.customer_id

-- подзапрос для разбивки событий по типам на уровне клиента
LEFT JOIN (
    SELECT
        ft2.customer_id,
        ft2.event_type,
        COUNT(*)::int AS cnt
    FROM fct_telemetry ft2
    GROUP BY ft2.customer_id, ft2.event_type
) event_counts ON event_counts.customer_id = dc.customer_id

GROUP BY
    dc.customer_id,
    dc.name,
    dc.email,
    dc.phone,
    dc.company,
    dc.crm_created_at

ON CONFLICT (customer_id) DO UPDATE SET
    customer_name   = EXCLUDED.customer_name,
    email           = EXCLUDED.email,
    phone           = EXCLUDED.phone,
    company         = EXCLUDED.company,
    crm_created_at  = EXCLUDED.crm_created_at,
    device_count    = EXCLUDED.device_count,
    active_devices  = EXCLUDED.active_devices,
    device_types    = EXCLUDED.device_types,
    total_events    = EXCLUDED.total_events,
    avg_event_value = EXCLUDED.avg_event_value,
    max_event_value = EXCLUDED.max_event_value,
    min_event_value = EXCLUDED.min_event_value,
    first_event_at  = EXCLUDED.first_event_at,
    last_event_at   = EXCLUDED.last_event_at,
    events_30d      = EXCLUDED.events_30d,
    avg_value_30d   = EXCLUDED.avg_value_30d,
    events_by_type  = EXCLUDED.events_by_type,
    mart_updated_at = NOW();
"""

SQL_REMOVE_DELETED = """
DELETE FROM mart_customer_telemetry
WHERE  customer_id NOT IN (SELECT customer_id FROM dim_customers);
"""


# ── Task callables ────────────────────────────────────────────────────────────

def validate_sources(**_):
    """Check that source tables are not empty before building the mart."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

    result = olap.get_first("SELECT COUNT(*) FROM dim_customers")
    customer_count = result[0] if result else 0

    if customer_count == 0:
        raise ValueError(
            "dim_customers is empty — crm_to_olap DAG must run first"
        )

    result = olap.get_first("SELECT COUNT(*) FROM fct_telemetry")
    event_count = result[0] if result else 0

    logger.info(
        "Source validation passed: %d customers, %d telemetry events",
        customer_count,
        event_count,
    )


def refresh_mart(**_):
    """Upsert all customer rows into mart_customer_telemetry."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
    olap.run(SQL_REFRESH_MART)
    logger.info("mart_customer_telemetry refreshed")


def remove_stale_rows(**_):
    """Remove mart rows for customers that no longer exist in dim_customers."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
    olap.run(SQL_REMOVE_DELETED)
    logger.info("Stale mart rows removed")


def log_mart_stats(**_):
    """Log row count and coverage stats after the refresh."""
    olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

    stats = olap.get_first(
        """
        SELECT
            COUNT(*)                                       AS total_customers,
            SUM(total_events)                              AS total_events,
            COUNT(*) FILTER (WHERE last_event_at >= NOW() - INTERVAL '30 days')
                                                           AS active_30d,
            MAX(mart_updated_at)                           AS last_refresh
        FROM mart_customer_telemetry
        """
    )

    if stats:
        logger.info(
            "Mart stats — customers: %s | total events: %s | active 30d: %s | refreshed: %s",
            *stats,
        )


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="build_mart",
    default_args=default_args,
    description="Daily refresh of mart_customer_telemetry (витрина клиентской телеметрии)",
    schedule_interval="0 1 * * *",   # каждый день в 01:00
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
