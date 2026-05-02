-- OLAP database: staging → dimensions → facts → data mart (витрина)

-- ============================================================
-- STAGING LAYER  (raw copies from source)
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id    INT          NOT NULL,
    name           VARCHAR(255),
    email          VARCHAR(255),
    phone          VARCHAR(50),
    company        VARCHAR(255),
    crm_created_at TIMESTAMP,
    crm_updated_at TIMESTAMP,
    loaded_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS stg_customer_devices (
    device_id     INT          NOT NULL,
    customer_id   INT          NOT NULL,
    device_type   VARCHAR(100),
    device_serial VARCHAR(255),
    assigned_at   TIMESTAMP,
    is_active     BOOLEAN,
    loaded_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id)
);

CREATE TABLE IF NOT EXISTS stg_telemetry (
    event_id      BIGINT       NOT NULL,
    device_serial VARCHAR(255),
    event_type    VARCHAR(100),
    event_value   NUMERIC(12, 4),
    recorded_at   TIMESTAMP,
    loaded_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id)
);

-- ============================================================
-- DIMENSION LAYER
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk    SERIAL       PRIMARY KEY,
    customer_id    INT          NOT NULL UNIQUE,   -- natural key from CRM
    name           VARCHAR(255),
    email          VARCHAR(255),
    phone          VARCHAR(50),
    company        VARCHAR(255),
    crm_created_at TIMESTAMP,
    is_current     BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_devices (
    device_sk     SERIAL       PRIMARY KEY,
    device_id     INT          NOT NULL UNIQUE,
    customer_id   INT          NOT NULL,
    device_type   VARCHAR(100),
    device_serial VARCHAR(255) NOT NULL UNIQUE,
    assigned_at   TIMESTAMP,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_dev_customer ON dim_devices(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_dev_serial   ON dim_devices(device_serial);

-- ============================================================
-- FACT LAYER
-- ============================================================

CREATE TABLE IF NOT EXISTS fct_telemetry (
    event_id      BIGINT       NOT NULL,
    device_sk     INT          REFERENCES dim_devices(device_sk),
    customer_sk   INT          REFERENCES dim_customers(customer_sk),
    customer_id   INT,
    device_serial VARCHAR(255),
    event_type    VARCHAR(100),
    event_value   NUMERIC(12, 4),
    recorded_at   TIMESTAMP,
    loaded_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_fct_tel_customer_id  ON fct_telemetry(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_tel_device_serial ON fct_telemetry(device_serial);
CREATE INDEX IF NOT EXISTS idx_fct_tel_recorded_at  ON fct_telemetry(recorded_at);
CREATE INDEX IF NOT EXISTS idx_fct_tel_event_type   ON fct_telemetry(event_type);

-- ============================================================
-- DATA MART — витрина клиентской телеметрии
-- Денормализованная таблица для сервиса отчётов.
-- Первичный ключ customer_id + индексы обеспечивают быстрый
-- доступ к данным по любому клиенту.
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_customer_telemetry (
    -- Идентификация клиента
    customer_id       INT          NOT NULL,
    customer_name     VARCHAR(255),
    email             VARCHAR(255),
    phone             VARCHAR(50),
    company           VARCHAR(255),
    crm_created_at    TIMESTAMP,

    -- Сводка по устройствам
    device_count      INT          DEFAULT 0,
    active_devices    INT          DEFAULT 0,
    device_types      TEXT,        -- уникальные типы устройств через запятую

    -- Агрегаты телеметрии (за всё время)
    total_events      BIGINT       DEFAULT 0,
    avg_event_value   NUMERIC(10, 2),
    max_event_value   NUMERIC(10, 2),
    min_event_value   NUMERIC(10, 2),
    first_event_at    TIMESTAMP,
    last_event_at     TIMESTAMP,

    -- Агрегаты за последние 30 дней
    events_30d        BIGINT       DEFAULT 0,
    avg_value_30d     NUMERIC(10, 2),

    -- Разбивка по типам событий (JSON для гибкости запросов)
    events_by_type    JSONB,

    -- Метаданные витрины
    mart_updated_at   TIMESTAMP    NOT NULL DEFAULT NOW(),

    PRIMARY KEY (customer_id)
);

-- Быстрый доступ по ключевым измерениям
CREATE INDEX IF NOT EXISTS idx_mart_ct_email        ON mart_customer_telemetry(email);
CREATE INDEX IF NOT EXISTS idx_mart_ct_company      ON mart_customer_telemetry(company);
CREATE INDEX IF NOT EXISTS idx_mart_ct_last_event   ON mart_customer_telemetry(last_event_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mart_ct_total_events ON mart_customer_telemetry(total_events DESC);
