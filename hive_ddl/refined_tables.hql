-- ============================================
-- REFINED LAYER: cleaned and normalized data
-- ============================================

CREATE DATABASE IF NOT EXISTS refined;

-- Refined Orders
CREATE TABLE IF NOT EXISTS refined.orders (
    order_id    STRING,
    seller_id   STRING,
    customer_id STRING,
    amount      DOUBLE,
    event_ts    TIMESTAMP,
    status      STRING,
    category    STRING
)
STORED AS PARQUET
LOCATION '/data/refined/orders/';

-- Refined Returns
CREATE TABLE IF NOT EXISTS refined.returns (
    return_id   STRING,
    order_id    STRING,
    seller_id   STRING,
    event_ts    TIMESTAMP,
    reason      STRING,
    status      STRING
)
STORED AS PARQUET
LOCATION '/data/refined/returns/';

-- Refined Complaints
CREATE TABLE IF NOT EXISTS refined.complaints (
    complaint_id STRING,
    seller_id    STRING,
    event_ts     TIMESTAMP,
    severity     STRING,
    category     STRING
)
STORED AS PARQUET
LOCATION '/data/refined/complaints/';

-- Refined Sellers
CREATE TABLE IF NOT EXISTS refined.sellers (
    seller_id           STRING,
    seller_name         STRING,
    registration_date   DATE,
    region              STRING,
    category            STRING,
    verification_status STRING,
    support_tier        STRING
)
STORED AS PARQUET
LOCATION '/data/refined/sellers/';