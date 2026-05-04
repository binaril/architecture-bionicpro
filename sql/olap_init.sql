-- ClickHouse OLAP database schema

-- ============================================================
-- STAGING LAYER  (raw copies from source)
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_customers
(
    customer_id    Int32,
    name           Nullable(String),
    email          Nullable(String),
    phone          Nullable(String),
    company        Nullable(String),
    crm_created_at Nullable(DateTime),
    crm_updated_at Nullable(DateTime),
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS stg_customer_devices
(
    device_id     Int32,
    customer_id   Int32,
    device_type   Nullable(String),
    device_serial Nullable(String),
    assigned_at   Nullable(DateTime),
    is_active     UInt8 DEFAULT 1,
    loaded_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY device_id;

CREATE TABLE IF NOT EXISTS stg_telemetry
(
    event_id      Int64,
    device_serial Nullable(String),
    event_type    Nullable(String),
    event_value   Nullable(Decimal(12, 4)),
    recorded_at   Nullable(DateTime),
    loaded_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY event_id;

-- ============================================================
-- DIMENSION LAYER
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customers
(
    customer_id    Int32,
    name           Nullable(String),
    email          Nullable(String),
    phone          Nullable(String),
    company        Nullable(String),
    crm_created_at Nullable(DateTime),
    is_current     UInt8 DEFAULT 1,
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dim_devices
(
    device_id     Int32,
    customer_id   Int32,
    device_type   Nullable(String),
    device_serial String,
    assigned_at   Nullable(DateTime),
    is_active     UInt8 DEFAULT 1,
    loaded_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY device_id;

-- ============================================================
-- FACT LAYER
-- ============================================================

CREATE TABLE IF NOT EXISTS fct_telemetry
(
    event_id      Int64,
    customer_id   Nullable(Int32),
    device_id     Nullable(Int32),
    device_serial Nullable(String),
    event_type    Nullable(String),
    event_value   Nullable(Decimal(12, 4)),
    recorded_at   Nullable(DateTime),
    loaded_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY event_id;

-- ============================================================
-- DATA MART — витрина клиентской телеметрии
-- ReplacingMergeTree по mart_updated_at: новая INSERT перекрывает
-- старую запись при FINAL-чтении или OPTIMIZE.
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_customer_telemetry
(
    customer_id       Int32,
    customer_name     Nullable(String),
    email             Nullable(String),
    phone             Nullable(String),
    company           Nullable(String),
    crm_created_at    Nullable(DateTime),
    device_count      Int32            DEFAULT 0,
    active_devices    Int32            DEFAULT 0,
    device_types      Nullable(String),
    total_events      Int64            DEFAULT 0,
    avg_event_value   Nullable(Decimal(10, 2)),
    max_event_value   Nullable(Decimal(10, 2)),
    min_event_value   Nullable(Decimal(10, 2)),
    first_event_at    Nullable(DateTime),
    last_event_at     Nullable(DateTime),
    events_30d        Int64            DEFAULT 0,
    avg_value_30d     Nullable(Decimal(10, 2)),
    events_by_type    String           DEFAULT '{}',
    mart_updated_at   DateTime         DEFAULT now()
) ENGINE = ReplacingMergeTree(mart_updated_at)
ORDER BY customer_id;
